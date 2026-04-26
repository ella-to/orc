// Example 08 — Queue with a rate limiter.
//
// Demonstrates:
//   - NewWorkflowQueue + WithRateLimiter (sliding window)
//   - Enqueueing a burst of work
//   - The queue runner staggers dispatch so the rate limit is respected
//
// We enqueue 12 webhook deliveries with a limit of 4 per second.
// You'll see 4 fire essentially immediately, then 4 more after ~1s, etc.
//
// Run:  go run .
package main

import (
	"context"
	"fmt"
	"time"

	"ella.to/orc"
)

type webhook struct {
	URL  string
	Body string
}

var start = time.Now()

func deliverWebhook(c *orc.Context, w webhook) (string, error) {
	elapsed := time.Since(start).Round(10 * time.Millisecond)
	fmt.Printf("[+%6s] delivering webhook -> %s\n", elapsed, w.URL)
	// Pretend the call is fast; the rate limiter is what's pacing us.
	return "200 OK", nil
}

func main() {
	ctx, err := orc.NewContext(context.Background(), orc.Config{
		AppName:      "rate-limited",
		DatabasePath: "rate-limited.db",
	})
	if err != nil {
		panic(err)
	}
	defer orc.Shutdown(ctx, 5*time.Second)

	orc.RegisterWorkflow[webhook, string](ctx, deliverWebhook,
		orc.WithWorkflowName("deliver_webhook"))

	q := orc.NewWorkflowQueue(ctx, "webhooks",
		orc.WithRateLimiter(&orc.RateLimiter{
			Limit:  4,
			Period: time.Second,
		}),
	)

	if err := orc.Launch(ctx); err != nil {
		panic(err)
	}

	const N = 12
	handles := make([]orc.WorkflowHandle[string], N)
	for i := 0; i < N; i++ {
		h, err := orc.RunWorkflow[webhook, string](ctx, deliverWebhook,
			webhook{URL: fmt.Sprintf("https://example.test/hook/%02d", i),
				Body: "ping"},
			orc.WithQueue(q.Name),
		)
		if err != nil {
			panic(err)
		}
		handles[i] = h
	}

	for _, h := range handles {
		if _, err := h.GetResult(orc.WithHandleTimeout(15 * time.Second)); err != nil {
			panic(err)
		}
	}
	fmt.Printf("\nall %d webhooks delivered in %v (rate limit = 4/s)\n",
		N, time.Since(start).Round(10*time.Millisecond))
}
