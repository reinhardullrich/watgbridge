package history

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"watgbridge/database"
	"watgbridge/state"

	"github.com/PaulSonOfLars/gotgbot/v2"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type fakeTelegram struct {
	gotgbot.BaseBotClient
	calls   int
	fail    bool
	methods []string
	params  map[string]any
}

func (f *fakeTelegram) RequestWithContext(_ context.Context, _ string, method string, params map[string]any, _ *gotgbot.RequestOpts) (json.RawMessage, error) {
	f.params = params
	f.methods = append(f.methods, method)
	if method == "createForumTopic" {
		return json.RawMessage(`{"message_thread_id":7,"name":"History"}`), nil
	}
	f.calls++
	if f.fail {
		return nil, errors.New("simulated ambiguous network failure")
	}
	return json.RawMessage(`{"message_id":42,"date":1,"chat":{"id":-100,"type":"supergroup"}}`), nil
}

func setup(t *testing.T) *fakeTelegram {
	t.Helper()
	old := state.State
	t.Cleanup(func() { state.State = old })
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "history.db")), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	conn, _ := db.DB()
	t.Cleanup(func() { conn.Close() })
	state.State.Database = db
	state.State.Config = &state.Config{}
	state.State.Config.HistorySync.Enabled = true
	state.State.Config.HistorySync.DaysLimit = 90
	state.State.Config.Telegram.TargetChatID = -100
	state.State.WhatsAppClient = nil
	if err := database.AutoMigrate(); err != nil {
		t.Fatal(err)
	}
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	fake := &fakeTelegram{}
	state.State.TelegramBot = &gotgbot.Bot{BotClient: fake}
	return fake
}

func message(id, text string, stamp int64) *events.Message {
	j, _ := types.ParseJID("123@s.whatsapp.net")
	return &events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: j, Sender: j}, ID: id, Timestamp: time.Unix(stamp, 0)}, Message: &waE2E.Message{Conversation: proto.String(text)}}
}

func TestCacheAndOrdering(t *testing.T) {
	setup(t)
	for _, v := range []*events.Message{message("b", "newer", 200), message("a", "older", 100), message("a", "duplicate", 100)} {
		if err := Save(v); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := Pending("123@s.whatsapp.net", 50)
	if err != nil || len(rows) != 2 || rows[0].ID != "a" || rows[0].Text != "older" {
		t.Fatalf("ordering/dedup: %v, %v", rows, err)
	}
	if _, err := Pending("123@s.whatsapp.net", 201); err == nil {
		t.Fatal("unbounded import accepted")
	}
	for _, raw := range []string{"", "status@broadcast", "invalid", "123:4@s.whatsapp.net"} {
		if _, err := ParseChat(raw); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}

func TestPrivacyAndMedia(t *testing.T) {
	setup(t)
	v := message("private", "secret", 100)
	v.IsViewOnce = true
	if err := Save(v); err != nil {
		t.Fatal(err)
	}
	v.IsViewOnce = false
	v.IsEphemeral = true
	if err := Save(v); err != nil {
		t.Fatal(err)
	}
	v = message("image", "", 101)
	v.Message = &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Caption: proto.String("caption")}}
	if err := Save(v); err != nil {
		t.Fatal(err)
	}
	rows, err := Pending(v.Info.Chat.String(), 50)
	if err != nil || len(rows) != 1 || !strings.Contains(rows[0].Text, "caption") || !strings.Contains(rows[0].Text, "not imported") {
		t.Fatalf("privacy/media: %v %v", rows, err)
	}
}

func TestIgnoredChatAndBusyCancellation(t *testing.T) {
	fake := setup(t)
	state.State.Config.WhatsApp.IgnoreChats = []string{"123"}
	if _, err := ParseChat("123@s.whatsapp.net"); err == nil {
		t.Fatal("ignored chat accepted")
	}
	if err := Save(message("ignored", "text", 100)); err != nil {
		t.Fatal(err)
	}
	rows, err := Pending("123@s.whatsapp.net", 1)
	if err != nil || len(rows) != 0 {
		t.Fatal("ignored message cached")
	}
	DeliveryMu.Lock()
	defer DeliveryMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := deliver(ctx, Message{}); !errors.Is(err, context.DeadlineExceeded) || fake.calls != 0 {
		t.Fatalf("busy import did not cancel: %v", err)
	}
}

func TestDeliveryNoCommandReplayAndNoDuplicates(t *testing.T) {
	fake := setup(t)
	v := message("one", ".id @all <b>not markup</b>", 100)
	if err := Save(v); err != nil {
		t.Fatal(err)
	}
	rows, _ := Pending(v.Info.Chat.String(), 1)
	if ok, err := deliver(context.Background(), rows[0]); !ok || err != nil {
		t.Fatalf("delivery: %v %v", ok, err)
	}
	if ok, err := deliver(context.Background(), rows[0]); ok || err != nil || fake.calls != 1 {
		t.Fatalf("duplicate: %v %v calls=%d", ok, err, fake.calls)
	}
	if !strings.Contains(Render(rows[0]), "1970-01-01 00:01:40 UTC") {
		t.Fatal("original date lost")
	}
	// WhatsApp client is deliberately nil: history must never send to WhatsApp.
	for _, method := range fake.methods {
		if method != "createForumTopic" && method != "sendMessage" {
			t.Fatal(method)
		}
	}
}

func TestAmbiguousSendIsNotRetried(t *testing.T) {
	fake := setup(t)
	fake.fail = true
	v := message("one", "text", 100)
	if err := Save(v); err != nil {
		t.Fatal(err)
	}
	rows, _ := Pending(v.Info.Chat.String(), 1)
	if _, err := deliver(context.Background(), rows[0]); err == nil {
		t.Fatal("delivery failure hidden")
	}
	if ok, err := deliver(context.Background(), rows[0]); ok || err != nil || fake.calls != 1 {
		t.Fatal("ambiguous delivery retried")
	}
	var d Delivery
	state.State.Database.First(&d)
	if d.Status != "pending" {
		t.Fatal("uncertain delivery not retained")
	}
}

func TestLongTextAndLiveDuplicate(t *testing.T) {
	fake := setup(t)
	v := message("long", strings.Repeat("x", 5000), 100)
	if err := Save(v); err != nil {
		t.Fatal(err)
	}
	rows, _ := Pending(v.Info.Chat.String(), 1)
	if ok, err := deliver(context.Background(), rows[0]); !ok || err != nil {
		t.Fatal(err)
	}
	if fake.methods[len(fake.methods)-1] != "sendDocument" {
		t.Fatal("long text not preserved as document")
	}
	v = message("live", "already forwarded", 101)
	if err := Save(v); err != nil {
		t.Fatal(err)
	}
	if err := database.MsgIdAddNewPair("live", v.Info.Sender.String(), v.Info.Chat.String(), -100, 43, 7); err != nil {
		t.Fatal(err)
	}
	rows, err := Pending(v.Info.Chat.String(), 10)
	if err != nil || len(rows) != 0 {
		t.Fatalf("live duplicate not excluded: %v", err)
	}
}
