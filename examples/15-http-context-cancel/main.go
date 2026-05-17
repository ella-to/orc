// Example 15 — Propagate HTTP request cancellation into a workflow.
//
// Demonstrates:
//   - Running a workflow inside an http.Handler with `WithCallerContext`
//   - The workflow's per-run context is cancelled when the client
//     disconnects (or the request times out)
//   - h.GetResult returns context.Canceled so the handler can decide
//     how to respond
//   - The workflow row is recorded as CANCELLED, not ERROR
//
// This is the request-scoped pattern. Use it when the workflow should
// stop work the moment the caller goes away. For background work that
// must outlive the request, drop WithCallerContext and rely on the
// usual durable-recovery flow instead.
//
// Run:  go run .
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	"ella.to/orc"
)

// slowJob is a long-running workflow. It uses orc.Sleep, which honours
// ctx.Done(), so cancelling its context wakes it up early.
func slowJob(c *orc.Context, label string) (string, error) {
	log.Printf("workflow: starting %q (will sleep up to 10s)", label)
	if _, err := orc.Sleep(c, 10*time.Second); err != nil {
		log.Printf("workflow: cancelled early via ctx: %v", err)
		return "", err
	}
	return "finished:" + label, nil
}

func main() {
	ctx, err := orc.NewContext(context.Background(), orc.Config{
		AppName:      "http-cancel",
		DatabasePath: "http-cancel.db",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer orc.Shutdown(ctx, 5*time.Second)

	orc.RegisterWorkflow[string, string](ctx, slowJob,
		orc.WithWorkflowName("slow_job"))
	if err := orc.Launch(ctx); err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()

	// /run starts a workflow whose lifetime is tied to the HTTP request.
	// If the client disconnects or aborts, the workflow's context is
	// cancelled and orc.Sleep returns promptly with context.Canceled.
	mux.HandleFunc("/run", func(w http.ResponseWriter, r *http.Request) {
		label := r.URL.Query().Get("label")
		if label == "" {
			label = "anon"
		}

		h, err := orc.RunWorkflow[string, string](ctx, slowJob, label,
			orc.WithCallerContext(r.Context()))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		result, err := h.GetResult(orc.WithHandleTimeout(30 * time.Second))
		switch {
		case errors.Is(err, context.Canceled):
			log.Printf("handler: request cancelled, workflow id=%s", h.GetWorkflowID())
			http.Error(w, "request cancelled", 499)
			return
		case err != nil:
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "ok: %s\n", result)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()
	log.Printf("server listening at %s", srv.URL)

	// Drive the example: start a request, then cancel it after ~300ms by
	// abandoning the request's context.
	reqCtx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(reqCtx, http.MethodGet, srv.URL+"/run?label=demo", nil)

	go func() {
		time.Sleep(300 * time.Millisecond)
		log.Println("client: cancelling request")
		cancel()
	}()

	if _, err := http.DefaultClient.Do(req); err != nil {
		log.Printf("client: request errored as expected: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Inspect the workflow rows so the operator can see the workflow was
	// marked CANCELLED, not ERROR.
	rows, _ := orc.ListWorkflows(ctx, orc.WithListWorkflowName("slow_job"))
	for _, r := range rows {
		fmt.Printf("workflow %s: status=%s err=%v\n", r.ID, r.Status, r.Error)
	}
}
