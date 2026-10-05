package telegram

import (
	"github.com/PaulSonOfLars/gotgbot/v2"
	"github.com/PaulSonOfLars/gotgbot/v2/ext"
	"testing"
	"watgbridge/state"
)

func TestHistoryRefusesNonOwnerAndOtherChats(t *testing.T) {
	old := state.State.Config
	t.Cleanup(func() { state.State.Config = old })
	state.State.Config = &state.Config{}
	state.State.Config.Telegram.OwnerID = 1
	state.State.Config.Telegram.TargetChatID = -100
	state.State.Config.Telegram.SudoUsersID = []int64{2}
	for _, c := range []*ext.Context{
		{},
		{EffectiveSender: &gotgbot.Sender{User: &gotgbot.User{Id: 2}}, EffectiveChat: &gotgbot.Chat{Id: -100}},
		{EffectiveSender: &gotgbot.Sender{User: &gotgbot.User{Id: 1}}, EffectiveChat: &gotgbot.Chat{Id: -200}},
	} {
		if err := HistoryCommandHandler(nil, c); err != nil {
			t.Fatal(err)
		}
	}
}
