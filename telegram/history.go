package telegram

import (
	"context"
	"fmt"
	"html"
	"strconv"
	"strings"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"
	"github.com/PaulSonOfLars/gotgbot/v2/ext"
	"watgbridge/history"
	"watgbridge/state"
	"watgbridge/utils"
)

func HistoryCommandHandler(b *gotgbot.Bot, c *ext.Context) error {
	// History access is owner-only, including when ordinary sudo users exist.
	if c.EffectiveSender == nil || c.EffectiveSender.User == nil || c.EffectiveSender.User.Id != state.State.Config.Telegram.OwnerID {
		return nil
	}
	if c.EffectiveChat == nil || c.EffectiveMessage == nil || (c.EffectiveChat.Id != state.State.Config.Telegram.OwnerID && c.EffectiveChat.Id != state.State.Config.Telegram.TargetChatID) {
		return nil
	}
	reply := func(text string) error {
		_, err := utils.TgReplyTextByContext(b, c, html.EscapeString(text), nil, false)
		return err
	}
	if !state.State.Config.HistorySync.Enabled {
		return reply("History sync is disabled in configuration.")
	}
	args := strings.Fields(c.EffectiveMessage.Text)
	if len(args) == 1 {
		text, err := history.Status()
		if err != nil {
			return reply("History storage lookup failed.")
		}
		return reply(text + "\n/history fetch JID\n/history refresh JID (re-fetch recent attachment references)\n/history import JID COUNT\n/history attachments JID COUNT (upgrade existing imports only)")
	}
	if len(args) < 3 || len(args) > 4 {
		return reply("Use /history, /history fetch JID, or /history import JID COUNT.")
	}
	jid, err := history.ParseChat(args[2])
	if err != nil {
		return reply(err.Error())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if args[1] == "import-message" && len(args) == 4 {
		ok, err := history.ImportMessage(ctx, jid, args[3])
		if err != nil {
			return reply(fmt.Sprintf("Message imported: %t. Stopped: %s", ok, err))
		}
		return reply(fmt.Sprintf("Message imported: %t (false means already delivered).", ok))
	}
	if (args[1] == "fetch" || args[1] == "refresh") && len(args) == 3 {
		if err := history.Fetch(ctx, jid, args[1] == "refresh"); err != nil {
			return reply(err.Error())
		}
		return reply("Requested up to 50 older messages from your phone. They will only be cached locally; use /history to check arrival.")
	}
	if (args[1] == "import" || args[1] == "attachments") && len(args) == 4 {
		count, err := strconv.Atoi(args[3])
		if err != nil || count < 1 || count > 200 {
			return reply("Count must be 1..200.")
		}
		var n int
		if args[1] == "attachments" {
			n, err = history.Attachments(ctx, jid, count)
		} else {
			n, err = history.Import(ctx, jid, count)
		}
		if err != nil {
			return reply(fmt.Sprintf("Imported %d. Stopped: %s", n, err))
		}
		return reply(fmt.Sprintf("Completed %d %s. Original dates are preserved; use /history to check unconfirmed deliveries.", n, args[1]))
	}
	return reply("Use /history, /history fetch JID, or /history import JID COUNT.")
}
