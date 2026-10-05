package history

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
	"watgbridge/database"
	"watgbridge/state"
)

const maxHistoryMedia = 49 * 1024 * 1024

// Keep only the attachment reference, not quoted messages or other context.
func encodeMedia(m *waE2E.Message) ([]byte, error) {
	n := &waE2E.Message{}
	switch {
	case m.ImageMessage != nil:
		n.ImageMessage = proto.Clone(m.ImageMessage).(*waE2E.ImageMessage)
		n.ImageMessage.ContextInfo = nil
	case m.VideoMessage != nil:
		n.VideoMessage = proto.Clone(m.VideoMessage).(*waE2E.VideoMessage)
		n.VideoMessage.ContextInfo = nil
	case m.AudioMessage != nil:
		n.AudioMessage = proto.Clone(m.AudioMessage).(*waE2E.AudioMessage)
		n.AudioMessage.ContextInfo = nil
	case m.DocumentMessage != nil:
		n.DocumentMessage = proto.Clone(m.DocumentMessage).(*waE2E.DocumentMessage)
		n.DocumentMessage.ContextInfo = nil
	case m.StickerMessage != nil:
		n.StickerMessage = proto.Clone(m.StickerMessage).(*waE2E.StickerMessage)
		n.StickerMessage.ContextInfo = nil
	default:
		return nil, nil
	}
	return proto.Marshal(n)
}

func decodeMedia(data []byte) (whatsmeow.DownloadableMessage, string, string, uint64, error) {
	m := &waE2E.Message{}
	if err := proto.Unmarshal(data, m); err != nil {
		return nil, "", "", 0, fmt.Errorf("invalid cached attachment reference")
	}
	switch {
	case m.ImageMessage != nil:
		return m.ImageMessage, "photo", "photo.jpg", m.ImageMessage.GetFileLength(), nil
	case m.VideoMessage != nil:
		return m.VideoMessage, "video", "video.mp4", m.VideoMessage.GetFileLength(), nil
	case m.AudioMessage != nil:
		return m.AudioMessage, "audio", "audio.ogg", m.AudioMessage.GetFileLength(), nil
	case m.DocumentMessage != nil:
		name := filepath.Base(strings.ReplaceAll(m.DocumentMessage.GetFileName(), "\\", "/"))
		if name == "" || name == "." || name == "/" {
			name = "attachment"
		}
		return m.DocumentMessage, "document", name, m.DocumentMessage.GetFileLength(), nil
	case m.StickerMessage != nil:
		return m.StickerMessage, "document", "sticker.webp", m.StickerMessage.GetFileLength(), nil
	}
	return nil, "", "", 0, fmt.Errorf("unsupported cached attachment")
}

// Bound actual writes as well as untrusted declared sizes (encrypted data adds padding).
type boundedMediaFile struct{ *os.File }

func (f boundedMediaFile) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(struct{ io.Writer }{f}, r)
}

func (f boundedMediaFile) Write(p []byte) (int, error) {
	off, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if off+int64(len(p)) > maxHistoryMedia+1024 {
		return 0, fmt.Errorf("attachment exceeds size limit")
	}
	return f.File.Write(p)
}
func (f boundedMediaFile) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 || off+int64(len(p)) > maxHistoryMedia+1024 {
		return 0, fmt.Errorf("attachment exceeds size limit")
	}
	return f.File.WriteAt(p, off)
}
func (f boundedMediaFile) Truncate(n int64) error {
	if n < 0 || n > maxHistoryMedia+1024 {
		return fmt.Errorf("attachment exceeds size limit")
	}
	return f.File.Truncate(n)
}

// Attachment retries operate only on existing history imports, never live messages.
func Attachments(ctx context.Context, jid types.JID, count int) (int, error) {
	if count < 1 || count > 200 {
		return 0, fmt.Errorf("count must be 1..200")
	}
	var rows []Message
	err := state.State.Database.Where("chat = ? AND length(media) > 0", jid.String()).
		Where("EXISTS (SELECT 1 FROM deliveries d WHERE d.chat=messages.chat AND d.id=messages.id AND d.target=? AND d.status='sent' AND COALESCE(d.attachment_status, '')='')", state.State.Config.Telegram.TargetChatID).
		Order("timestamp, id").Limit(count).Find(&rows).Error
	if err != nil {
		return 0, err
	}
	n := 0
	for _, row := range rows {
		for !DeliveryMu.TryLock() {
			select {
			case <-ctx.Done():
				return n, ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
		var d Delivery
		err = state.State.Database.Where("chat=? AND id=? AND target=?", row.Chat, row.ID, state.State.Config.Telegram.TargetChatID).First(&d).Error
		changed := false
		if err == nil && d.Status == "sent" && d.AttachmentStatus == "" {
			err = attach(ctx, row, &d)
			changed = err == nil
		}
		DeliveryMu.Unlock()
		if err != nil {
			return n, err
		}
		if changed {
			n++
		}
		select {
		case <-ctx.Done():
			return n, ctx.Err()
		case <-time.After(1100 * time.Millisecond):
		}
	}
	return n, nil
}

func attach(ctx context.Context, row Message, d *Delivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m, kind, name, size, err := decodeMedia(row.Media)
	if err != nil {
		return err
	}
	if size > maxHistoryMedia {
		return fmt.Errorf("attachment exceeds 49 MiB; text retained")
	}
	f, err := os.CreateTemp("", "history-media-*")
	if err != nil {
		return fmt.Errorf("cannot create private media temporary file")
	}
	defer os.Remove(f.Name())
	defer f.Close()
	err = state.State.WhatsAppClient.DownloadToFile(ctx, m, boundedMediaFile{f})
	if err != nil {
		if errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) || errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) || errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410) {
			return fmt.Errorf("WhatsApp no longer serves this attachment (403/404/410); placeholder retained")
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return fmt.Errorf("attachment download canceled or timed out; placeholder retained")
		}
		return fmt.Errorf("attachment download failed (unavailable media or connection error); text retained, retry with /history attachments")
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 || info.Size() > maxHistoryMedia {
		return fmt.Errorf("attachment is empty or exceeds 49 MiB")
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return uploadAttachment(ctx, row, d, kind, name, info.Size(), f)
}

func uploadAttachment(ctx context.Context, row Message, d *Delivery, kind, name string, size int64, reader io.Reader) error {
	caption := strings.ReplaceAll(Render(row), "; attachment not imported", "")
	file := gotgbot.InputFileByReader(name, reader)
	// Never silently truncate original text; long captions retain the original message.
	reply := len(caption) > 1000
	var thread int64
	if reply {
		var found bool
		var err error
		thread, found, err = database.ChatThreadGetTgFromWa(row.Chat, d.Target)
		if err != nil || !found {
			return fmt.Errorf("missing topic mapping; attachment not sent")
		}
	}
	d.AttachmentStatus = "pending"
	if err := state.State.Database.Save(d).Error; err != nil {
		return err
	}
	bot := state.State.TelegramBot
	var msg *gotgbot.Message
	var err error
	if reply {
		msg, err = bot.SendDocumentWithContext(ctx, d.Target, file, &gotgbot.SendDocumentOpts{
			MessageThreadId: thread,
			Caption:         "Attachment for the imported WhatsApp message above.", DisableNotification: true,
			ReplyParameters: &gotgbot.ReplyParameters{MessageId: d.TelegramID}, RequestOpts: &gotgbot.RequestOpts{Timeout: 2 * time.Minute}})
	} else {
		var media gotgbot.InputMedia = gotgbot.InputMediaDocument{Media: file, Caption: caption}
		switch kind {
		case "photo":
			if size <= 10*1024*1024 && !state.State.Config.Telegram.SendImagesAsFile {
				media = gotgbot.InputMediaPhoto{Media: file, Caption: caption}
			}
		case "video":
			media = gotgbot.InputMediaVideo{Media: file, Caption: caption}
		case "audio":
			media = gotgbot.InputMediaAudio{Media: file, Caption: caption}
		}
		msg, _, err = bot.EditMessageMediaWithContext(ctx, media, &gotgbot.EditMessageMediaOpts{ChatId: d.Target, MessageId: d.TelegramID, RequestOpts: &gotgbot.RequestOpts{Timeout: 2 * time.Minute}})
	}
	if err != nil || msg == nil {
		return fmt.Errorf("attachment delivery unconfirmed; reservation retained for manual review, not automatically retried")
	}
	d.AttachmentStatus = "sent"
	d.AttachmentID = msg.MessageId
	return state.State.Database.Save(d).Error
}
