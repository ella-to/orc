package main

import (
	"context"
	"fmt"
	"time"

	"ella.to/orc"
)

// A workflow is just a Go function with the signature
//
//	func(*orc.Context, In) (Out, error)
func greet(c *orc.Context, name string) (string, error) {
	return "hello, " + name, nil
}

func main() {
	ctx, err := orc.NewContext(context.Background(), orc.Config{
		AppName:      "demo",
		DatabasePath: "demo2.db",
	})
	if err != nil {
		panic(err)
	}

	orc.RegisterWorkflow[string, string](ctx, greet)
	if err := orc.Launch(ctx); err != nil {
		panic(err)
	}
	defer orc.Shutdown(ctx, 5*time.Second)

	h, _ := orc.RunWorkflow[string, string](ctx, greet, "world")
	out, _ := h.GetResult(orc.WithHandleTimeout(2 * time.Second))
	fmt.Println(out) // hello, world
}
