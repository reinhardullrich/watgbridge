package history

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PaulSonOfLars/gotgbot/v2"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
	"watgbridge/database"
	"watgbridge/state"
)

func TestMediaRefreshPreservesMessageAndStripsQuotes(t *testing.T) {
	setup(t)
	v := message("old", "original", 100)
	if err := Save(v); err != nil {
		t.Fatal(err)
	}
	v.Message = &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		DirectPath: proto.String("/media"), MediaKey: []byte("private-key"), FileLength: proto.Uint64(5),
		ContextInfo: &waE2E.ContextInfo{QuotedMessage: &waE2E.Message{Conversation: proto.String("private quote")}},
	}}
	if err := Save(v); err != nil {
		t.Fatal(err)
	}
	var row Message
	state.State.Database.First(&row)
	if row.Text != "original" || len(row.Media) == 0 {
		t.Fatal("refresh lost text or reference")
	}
	m, kind, _, size, err := decodeMedia(row.Media)
	if err != nil || kind != "photo" || size != 5 {
		t.Fatal("invalid media roundtrip")
	}
	if m.(*waE2E.ImageMessage).ContextInfo != nil {
		t.Fatal("quoted context persisted")
	}
	v.Info.ID = "ephemeral"
	v.IsEphemeral = true
	if err := Save(v); err != nil {
		t.Fatal(err)
	}
	var count int64
	state.State.Database.Model(&Message{}).Count(&count)
	if count != 1 {
		t.Fatal("ephemeral media cached")
	}
}

func TestAttachmentUpgradeAndNoDuplicate(t *testing.T) {
	for _, kind := range []string{"photo", "video", "audio", "document"} {
		t.Run(kind, func(t *testing.T) {
			fake := setup(t)
			row := Message{Chat: "123@s.whatsapp.net", ID: "old", Text: "[Image; attachment not imported]", Timestamp: 100, Media: []byte{1}}
			state.State.Database.Create(&row)
			d := Delivery{Chat: row.Chat, ID: row.ID, Target: -100, Status: "sent", TelegramID: 42}
			state.State.Database.Create(&d)
			if err := uploadAttachment(context.Background(), row, &d, kind, "file", 4, strings.NewReader("data")); err != nil {
				t.Fatal(err)
			}
			if fake.methods[0] != "editMessageMedia" || fake.params["message_id"] != int64(42) || d.AttachmentStatus != "sent" {
				t.Fatal("did not upgrade same message")
			}
			media := fake.params["media"].(gotgbot.InputMedia)
			if media.GetType() != kind {
				t.Fatal("wrong media type")
			}
			jid, _ := ParseChat(row.Chat)
			if n, err := Attachments(context.Background(), jid, 5); n != 0 || err != nil || fake.calls != 1 {
				t.Fatal("attachment duplicated")
			}
		})
	}
}

func TestAttachmentAmbiguousAndLongCaption(t *testing.T) {
	fake := setup(t)
	if err := database.ChatThreadAddNewPair("123@s.whatsapp.net", -100, 7); err != nil {
		t.Fatal(err)
	}
	row := Message{Chat: "123@s.whatsapp.net", ID: "old", Text: strings.Repeat("x", 2000), Media: []byte{1}}
	state.State.Database.Create(&row)
	d := Delivery{Chat: row.Chat, ID: row.ID, Target: -100, Status: "sent", TelegramID: 42}
	state.State.Database.Create(&d)
	fake.fail = true
	if err := uploadAttachment(context.Background(), row, &d, "document", "file", 4, strings.NewReader("data")); err == nil {
		t.Fatal("failure hidden")
	}
	if fake.methods[0] != "sendDocument" || d.AttachmentStatus != "pending" {
		t.Fatal("missing uncertain-delivery reservation")
	}
	rp := fake.params["reply_parameters"].(*gotgbot.ReplyParameters)
	if rp.MessageId != 42 {
		t.Fatal("not linked to original text")
	}
	jid, _ := ParseChat(row.Chat)
	if n, err := Attachments(context.Background(), jid, 5); n != 0 || err != nil || fake.calls != 1 {
		t.Fatal("uncertain attachment retried")
	}
}

func TestMediaSizeBoundAndFilename(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "media"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := boundedMediaFile{f}
	if err := b.Truncate(maxHistoryMedia + 1025); err == nil {
		t.Fatal("oversized allocation")
	}
	f.Seek(maxHistoryMedia+1024, io.SeekStart)
	if _, err := io.Copy(b, io.TeeReader(strings.NewReader("x"), io.Discard)); err == nil {
		t.Fatal("io.Copy bypassed limit")
	}
	if _, err := b.WriteAt([]byte("x"), maxHistoryMedia+1024); err == nil {
		t.Fatal("WriteAt bypassed limit")
	}
	data, _ := encodeMedia(&waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{FileName: proto.String("../../secret.txt")}})
	_, _, name, _, err := decodeMedia(data)
	if err != nil || name != "secret.txt" {
		t.Fatal("unsafe filename")
	}
}

func TestExactMessageImport(t *testing.T) {
	fake := setup(t)
	v := message("selected", "text", 100)
	if err := Save(v); err != nil {
		t.Fatal(err)
	}
	wrong, _ := ParseChat("456@s.whatsapp.net")
	if _, err := ImportMessage(context.Background(), wrong, v.Info.ID); err == nil {
		t.Fatal("cross-chat selection allowed")
	}
	if ok, err := ImportMessage(context.Background(), v.Info.Chat, v.Info.ID); !ok || err != nil {
		t.Fatal(err)
	}
	if ok, err := ImportMessage(context.Background(), v.Info.Chat, v.Info.ID); ok || err != nil || fake.calls != 1 {
		t.Fatal("exact import duplicated")
	}
}
