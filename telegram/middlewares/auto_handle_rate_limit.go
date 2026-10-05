package middlewares

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"
)

type autoHandleRateLimitBotClient struct {
	gotgbot.BotClient
}

func (b *autoHandleRateLimitBotClient) RequestWithContext(ctx context.Context,
	token string, method string, params map[string]any,
	opts *gotgbot.RequestOpts) (json.RawMessage, error) {

	if strings.HasPrefix(method, "send") || strings.HasPrefix(method, "edit") {
		params["parse_mode"] = "html"
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		response, err := b.BotClient.RequestWithContext(ctx, token, method, params, opts)
		if err == nil {
			return response, err
		}

		tgError, ok := err.(*gotgbot.TelegramError)
		if !ok {
			return response, err
		}

		if tgError.Code == 429 {
			if tgError.ResponseParams == nil || tgError.ResponseParams.RetryAfter <= 0 {
				return response, err
			}
			timeToSleep := tgError.ResponseParams.RetryAfter
			log.Printf("[auto_handle_rate_limit] sleeping for %v seconds", timeToSleep)
			timer := time.NewTimer(time.Second * time.Duration(timeToSleep))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}

		return response, err
	}
}

func AutoHandleRateLimit(b gotgbot.BotClient) gotgbot.BotClient {
	return &autoHandleRateLimitBotClient{b}
}
