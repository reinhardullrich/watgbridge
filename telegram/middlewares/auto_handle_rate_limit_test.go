package middlewares

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/PaulSonOfLars/gotgbot/v2"
	"testing"
	"time"
)

type limitedClient struct{ gotgbot.BaseBotClient }

func (limitedClient) RequestWithContext(context.Context, string, string, map[string]any, *gotgbot.RequestOpts) (json.RawMessage, error) {
	return nil, &gotgbot.TelegramError{Code: 429, ResponseParams: &gotgbot.ResponseParameters{RetryAfter: 60}}
}
func TestRateLimitHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := AutoHandleRateLimit(&limitedClient{}).RequestWithContext(ctx, "", "sendMessage", map[string]any{}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline, got %v", err)
	}
}
