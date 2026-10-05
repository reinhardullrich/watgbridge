// Package history caches messages and imports them without invoking live handlers.
package history

import (
	"context"
	"fmt"
	"html"
	"strings"
	"sync"
	"time"

	"watgbridge/database"
	"watgbridge/state"
	"watgbridge/utils"

	"github.com/PaulSonOfLars/gotgbot/v2"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"gorm.io/gorm/clause"
)

type Message struct {
	Chat      string `gorm:"primaryKey;size:255"`
	ID        string `gorm:"primaryKey;size:255"`
	Sender    string
	Name      string
	FromMe    bool
	Timestamp int64 `gorm:"index"`
	Text      string
	Media     []byte
}

type Delivery struct {
	Chat             string `gorm:"primaryKey;size:255"`
	ID               string `gorm:"primaryKey;size:255"`
	Target           int64  `gorm:"primaryKey"`
	Status           string
	TelegramID       int64
	AttachmentStatus string
	AttachmentID     int64
}

// ponytail: one delivery lock; use per-chat locks if import throughput matters.
var DeliveryMu sync.Mutex
var fetchMu sync.Mutex
var lastFetch time.Time

func Init() error {
	if !state.State.Config.HistorySync.Enabled {
		return nil
	}
	if n := state.State.Config.HistorySync.DaysLimit; n == 0 || n > 1095 {
		return fmt.Errorf("history_sync.days_limit must be between 1 and 1095")
	}
	return state.State.Database.AutoMigrate(&Message{}, &Delivery{})
}

func ParseChat(raw string) (types.JID, error) {
	j, err := types.ParseJID(raw)
	if err != nil || j.User == "" || j.Device != 0 ||
		(j.Server != types.DefaultUserServer && j.Server != types.HiddenUserServer && j.Server != types.GroupServer) {
		return types.EmptyJID, fmt.Errorf("use an exact WhatsApp person/group JID")
	}
	for _, ignored := range state.State.Config.WhatsApp.IgnoreChats {
		if ignored == j.User {
			return types.EmptyJID, fmt.Errorf("this chat is excluded by ignore_chats")
		}
	}
	return j, nil
}

func Save(v *events.Message) error {
	if !state.State.Config.HistorySync.Enabled || v == nil || v.Message == nil ||
		v.IsViewOnce || v.IsViewOnceV2 || v.IsViewOnceV2Extension || v.IsEphemeral ||
		v.Info.ID == "" || v.Info.Timestamp.Unix() <= 0 {
		return nil
	}
	if _, err := ParseChat(v.Info.Chat.String()); err != nil {
		return nil
	}
	m := v.Message
	// Never cache wrapped ephemeral/view-once content or execute protocol events.
	if m.GetProtocolMessage() != nil || m.GetEphemeralMessage() != nil ||
		m.GetViewOnceMessage() != nil || m.GetViewOnceMessageV2() != nil || m.GetViewOnceMessageV2Extension() != nil {
		return nil
	}
	text := m.GetConversation()
	if e := m.GetExtendedTextMessage(); e != nil {
		text = e.GetText()
	}
	switch {
	case m.GetImageMessage() != nil:
		text = "[Image; attachment not imported]\n" + m.GetImageMessage().GetCaption()
	case m.GetVideoMessage() != nil:
		text = "[Video; attachment not imported]\n" + m.GetVideoMessage().GetCaption()
	case m.GetDocumentMessage() != nil:
		text = "[Document; attachment not imported] " + m.GetDocumentMessage().GetFileName() + "\n" + m.GetDocumentMessage().GetCaption()
	case m.GetAudioMessage() != nil:
		text = "[Audio; attachment not imported]"
	case m.GetStickerMessage() != nil:
		text = "[Sticker; attachment not imported]"
	}
	if text == "" {
		return nil
	}
	row := Message{Chat: v.Info.Chat.String(), ID: v.Info.ID, Sender: v.Info.Sender.String(),
		Name: v.Info.PushName, FromMe: v.Info.IsFromMe, Timestamp: v.Info.Timestamp.Unix(), Text: text}
	var err error
	row.Media, err = encodeMedia(m)
	if err != nil {
		return err
	}
	conflict := clause.OnConflict{DoNothing: true}
	if len(row.Media) > 0 {
		// Re-fetching history adds download references to older text-only caches.
		conflict = clause.OnConflict{Columns: []clause.Column{{Name: "chat"}, {Name: "id"}}, DoUpdates: clause.AssignmentColumns([]string{"media"})}
	}
	return state.State.Database.Clauses(conflict).Create(&row).Error
}

func Receive(event *events.HistorySync) error {
	if !state.State.Config.HistorySync.Enabled || event == nil || event.Data == nil {
		return nil
	}
	for _, conversation := range event.Data.GetConversations() {
		jid, err := ParseChat(conversation.GetID())
		if err != nil {
			continue
		}
		for _, item := range conversation.GetMessages() {
			if item.GetMessage() == nil || item.GetMessage().GetKey() == nil {
				continue
			}
			v, err := state.State.WhatsAppClient.ParseWebMessage(jid, item.GetMessage())
			if err != nil {
				return fmt.Errorf("could not parse history message")
			}
			if err := Save(v); err != nil {
				return err
			}
		}
	}
	return nil
}

func HasDelivery(chat, id string) (bool, error) {
	var count int64
	err := state.State.Database.Model(&Delivery{}).Where("chat = ? AND id = ? AND target = ?", chat, id, state.State.Config.Telegram.TargetChatID).Count(&count).Error
	return count > 0, err
}

func Status() (string, error) {
	var rows []struct {
		Chat   string
		Count  int64
		Oldest int64
	}
	err := state.State.Database.Model(&Message{}).Select("chat, count(*) AS count, min(timestamp) AS oldest").Group("chat").Order("chat").Limit(20).Scan(&rows).Error
	if err != nil {
		return "", err
	}
	text := "Cached history (up to 20 chats):\n"
	for _, row := range rows {
		text += fmt.Sprintf("%s: %d messages, oldest %s\n", row.Chat, row.Count, time.Unix(row.Oldest, 0).UTC().Format("2006-01-02"))
	}
	var pending int64
	if err := state.State.Database.Model(&Delivery{}).Where("status = ? AND target = ?", "pending", state.State.Config.Telegram.TargetChatID).Count(&pending).Error; err != nil {
		return "", err
	}
	var mediaPending int64
	if err := state.State.Database.Model(&Delivery{}).Where("attachment_status = ? AND target = ?", "pending", state.State.Config.Telegram.TargetChatID).Count(&mediaPending).Error; err != nil {
		return "", err
	}
	return text + fmt.Sprintf("Unconfirmed deliveries needing manual review: %d\nUnconfirmed attachments: %d\n", pending, mediaPending), nil
}

func Fetch(ctx context.Context, jid types.JID, refresh bool) error {
	fetchMu.Lock()
	defer fetchMu.Unlock()
	if time.Since(lastFetch) < time.Minute {
		return fmt.Errorf("wait one minute between history requests")
	}
	var first Message
	order := "timestamp, id"
	if refresh {
		order = "timestamp DESC, id DESC"
	}
	if err := state.State.Database.Where("chat = ?", jid.String()).Order(order).First(&first).Error; err != nil {
		return fmt.Errorf("no cached anchor for this chat; receive a new message or initial history first")
	}
	if state.State.WhatsAppClient == nil || !state.State.WhatsAppClient.IsLoggedIn() {
		return fmt.Errorf("WhatsApp is not connected")
	}
	info := types.MessageInfo{MessageSource: types.MessageSource{Chat: jid, IsFromMe: first.FromMe}, ID: first.ID, Timestamp: time.Unix(first.Timestamp, 0)}
	lastFetch = time.Now()
	_, err := state.State.WhatsAppClient.SendPeerMessage(ctx, state.State.WhatsAppClient.BuildHistorySyncRequest(&info, 50))
	if err != nil {
		return fmt.Errorf("phone history request failed; try later")
	}
	return nil
}

func Pending(chat string, count int) ([]Message, error) {
	if count < 1 || count > 200 {
		return nil, fmt.Errorf("import count must be 1..200")
	}
	var rows []Message
	err := state.State.Database.Where("chat = ?", chat).
		Where("NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.chat = messages.chat AND d.id = messages.id AND d.target = ?)", state.State.Config.Telegram.TargetChatID).
		Where("NOT EXISTS (SELECT 1 FROM msg_id_pairs p WHERE p.wa_chat_id = messages.chat AND p.id = messages.id AND p.tg_chat_id = ?)", state.State.Config.Telegram.TargetChatID).
		Order("timestamp, id").Limit(count).Find(&rows).Error
	return rows, err
}

func Render(row Message) string {
	sender := row.Sender
	if row.Name != "" {
		sender = row.Name + " (" + row.Sender + ")"
	}
	if row.FromMe {
		sender = "Me"
	}
	return "[Imported WhatsApp history]\n" + time.Unix(row.Timestamp, 0).UTC().Format("2006-01-02 15:04:05 UTC") + "\n" + sender + "\n\n" + row.Text
}

// Import copies cached history to Telegram. Never call live WhatsApp handlers here.
func Import(ctx context.Context, jid types.JID, count int) (int, error) {
	rows, err := Pending(jid.String(), count)
	if err != nil {
		return 0, err
	}
	sent := 0
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return sent, err
		}
		ok, err := deliver(ctx, row)
		if ok {
			sent++
		}
		if err != nil {
			return sent, err
		}
		select {
		case <-ctx.Done():
			return sent, ctx.Err()
		case <-time.After(1100 * time.Millisecond):
		}
	}
	return sent, nil
}

func ImportMessage(ctx context.Context, jid types.JID, id string) (bool, error) {
	if id == "" || len(id) > 255 {
		return false, fmt.Errorf("invalid message ID")
	}
	var row Message
	if err := state.State.Database.Where("chat=? AND id=?", jid.String(), id).First(&row).Error; err != nil {
		return false, fmt.Errorf("message not cached in this chat")
	}
	return deliver(ctx, row)
}

func deliver(ctx context.Context, row Message) (bool, error) {
	for !DeliveryMu.TryLock() {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	defer DeliveryMu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	target := state.State.Config.Telegram.TargetChatID
	if exists, err := HasDelivery(row.Chat, row.ID); err != nil || exists {
		return false, err
	}
	var existing database.MsgIdPair
	result := state.State.Database.Where("id = ?", row.ID).Find(&existing)
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected > 0 {
		if existing.WaChatId == row.Chat && existing.TgChatId == target {
			return false, nil
		}
		return false, fmt.Errorf("message ID mapping conflict; import stopped")
	}
	thread, err := utils.TgGetOrMakeThreadFromWa_StringContext(ctx, row.Chat, target, "History "+row.Chat)
	if err != nil {
		return false, fmt.Errorf("could not create or resolve history topic")
	}
	delivery := Delivery{Chat: row.Chat, ID: row.ID, Target: target, Status: "pending"}
	if err := state.State.Database.Create(&delivery).Error; err != nil {
		return false, err
	}
	text := Render(row)
	bot := state.State.TelegramBot
	var msg *gotgbot.Message
	if escaped := html.EscapeString(text); len(escaped) <= 3500 {
		msg, err = bot.SendMessageWithContext(ctx, target, escaped, &gotgbot.SendMessageOpts{MessageThreadId: thread, DisableNotification: true, ParseMode: "HTML", RequestOpts: &gotgbot.RequestOpts{Timeout: 30 * time.Second}})
	} else {
		msg, err = bot.SendDocumentWithContext(ctx, target, gotgbot.InputFileByReader("history-message.txt", strings.NewReader(text)), &gotgbot.SendDocumentOpts{MessageThreadId: thread, DisableNotification: true, Caption: "Imported history: long message, complete text attached.", RequestOpts: &gotgbot.RequestOpts{Timeout: 30 * time.Second}})
	}
	if err != nil || msg == nil {
		return false, fmt.Errorf("Telegram delivery unconfirmed; pending record retained, no automatic retry")
	}
	delivery.TelegramID = msg.MessageId
	delivery.Status = "sent"
	if err := state.State.Database.Save(&delivery).Error; err != nil {
		return false, err
	}
	if err := database.MsgIdAddNewPair(row.ID, row.Sender, row.Chat, target, msg.MessageId, thread); err != nil {
		return false, err
	}
	if len(row.Media) > 0 {
		if err := attach(ctx, row, &delivery); err != nil {
			return true, err
		}
	}
	return true, nil
}
