// Example 02 — Step retries with exponential backoff.
//
// Demonstrates:
//   - RunAsStep with WithStepMaxRetries + WithStepRetryBackoff
//   - A step that fails the first 2 attempts and succeeds on the 3rd
//   - The workflow is unaware of the transient errors; it sees only the
//     final outcome
//
// Run:  go run .
package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"ella.to/orc"
)

var attempts atomic.Int32

// flakyExternalAPI fails twice, then returns a stable answer. In real life
// this would be calling Stripe, Twilio, your auth provider, etc.
func flakyExternalAPI(_ context.Context) (string, error) {
	n := attempts.Add(1)
	fmt.Printf("  [api] attempt #%d\n", n)
	if n < 3 {
		return "", errors.New("temporary network glitch")
	}
	return "API-OK", nil
}

func chargeCustomer(c *orc.Context, customerID string) (string, error) {
	fmt.Printf("workflow charging customer %s ...\n", customerID)

	// The step is retried up to 5 times with exponential backoff. From the
	// workflow's perspective, the call is a simple synchronous function.
	result, err := orc.RunAsStep(c, flakyExternalAPI,
		orc.WithStepName("call_payment_api"),
		orc.WithStepMaxRetries(5),
		orc.WithStepRetryBackoff(50*time.Millisecond, 2*time.Second, 2.0),
	)
	if err != nil {
		return "", fmt.Errorf("payment failed permanently: %w", err)
	}
	fmt.Printf("workflow got %q\n", result)
	return result, nil
}

func main() {
	ctx, err := orc.NewContext(context.Background(), orc.Config{
		AppName:      "step-retries",
		DatabasePath: "step-retries.db",
	})
	if err != nil {
		panic(err)
	}
	defer orc.Shutdown(ctx, 5*time.Second)

	orc.RegisterWorkflow[string, string](ctx, chargeCustomer,
		orc.WithWorkflowName("charge_customer"))
	if err := orc.Launch(ctx); err != nil {
		panic(err)
	}

	h, err := orc.RunWorkflow[string, string](ctx, chargeCustomer, "cust-42")
	if err != nil {
		panic(err)
	}
	out, err := h.GetResult(orc.WithHandleTimeout(5 * time.Second))
	if err != nil {
		panic(err)
	}

	fmt.Printf("\nfinal result : %s\n", out)
	fmt.Printf("api attempts : %d (the workflow saw none of the failures)\n", attempts.Load())
}
