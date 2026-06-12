// Example 06 — Saga / compensation pattern.
//
// Demonstrates:
//   - A multi-step booking flow (hotel + flight + car)
//   - When a later step fails, prior successful steps run their
//     compensating step to undo their work
//   - Compensations are themselves durable steps, so they happen
//     at most once even on restart
//
// Run:  go run .
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"ella.to/orc"
)

// Pretend "external services" with side effects.
type bookings struct {
	hotelBooked  bool
	flightBooked bool
	carBooked    bool
}

var state bookings

func bookHotel() error   { state.hotelBooked = true; fmt.Println("  HOTEL  booked"); return nil }
func bookFlight() error  { state.flightBooked = true; return errors.New("flight provider down") }
func bookCar() error     { state.carBooked = true; fmt.Println("  CAR    booked"); return nil }
func refundHotel() error { state.hotelBooked = false; fmt.Println("  HOTEL  refunded"); return nil }
func refundCar() error   { state.carBooked = false; fmt.Println("  CAR    refunded"); return nil }

// bookTrip is a saga: at each step, if it fails, run compensations for
// everything we've already done in reverse.
func bookTrip(c *orc.Context, _ string) (string, error) {
	// Step 1: hotel
	_, err := orc.RunAsStep(c, func(_ context.Context) (string, error) {
		return "ok", bookHotel()
	}, orc.WithStepName("book_hotel"))
	if err != nil {
		return "", err
	}

	// Step 2: car
	_, err = orc.RunAsStep(c, func(_ context.Context) (string, error) {
		return "ok", bookCar()
	}, orc.WithStepName("book_car"))
	if err != nil {
		// Compensate hotel
		_, _ = orc.RunAsStep(c, func(_ context.Context) (string, error) {
			return "ok", refundHotel()
		}, orc.WithStepName("refund_hotel"))
		return "", err
	}

	// Step 3: flight (this one fails)
	_, err = orc.RunAsStep(c, func(_ context.Context) (string, error) {
		return "ok", bookFlight()
	}, orc.WithStepName("book_flight"))
	if err != nil {
		fmt.Println("  ! flight booking failed; running compensations")
		_, _ = orc.RunAsStep(c, func(_ context.Context) (string, error) {
			return "ok", refundCar()
		}, orc.WithStepName("refund_car"))
		_, _ = orc.RunAsStep(c, func(_ context.Context) (string, error) {
			return "ok", refundHotel()
		}, orc.WithStepName("refund_hotel"))
		return "", err
	}

	return "trip-booked", nil
}

func main() {
	ctx, err := orc.NewContext(context.Background(), orc.Config{
		AppName:      "saga",
		DatabasePath: "saga.db",
	})
	if err != nil {
		panic(err)
	}
	defer orc.Shutdown(ctx, 5*time.Second)

	orc.RegisterWorkflow[string, string](ctx, bookTrip,
		orc.WithWorkflowName("book_trip"))
	if err := orc.Launch(ctx); err != nil {
		panic(err)
	}

	h, err := orc.RunWorkflow[string, string](ctx, bookTrip, "")
	if err != nil {
		panic(err)
	}
	out, runErr := h.GetResult(orc.WithHandleTimeout(5 * time.Second))

	fmt.Println()
	if runErr != nil {
		fmt.Printf("workflow failed (as expected): %v\n", runErr)
	} else {
		fmt.Printf("workflow succeeded: %q\n", out)
	}
	fmt.Printf("\nfinal external state: hotel=%v flight=%v car=%v\n",
		state.hotelBooked, state.flightBooked, state.carBooked)
}
