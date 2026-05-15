// Example 14 — Admin HTTP server.
//
// Demonstrates how to mount orc's admin handler in a standard library
// http.ServeMux at any prefix you like, and exercise it with curl-style
// requests for live monitoring, cancellation, resume, fork and tree
// inspection.
//
// In addition to the JSON API, this example also mounts the bundled
// HTML/JS dashboard from package web at /ui/. Open
// http://localhost:8080/ in a browser and you'll be redirected to the UI.
//
// The admin handler purposely has NO authentication or authorization built
// in — protect it at a higher layer (reverse proxy, middleware, internal-
// only listener) before exposing it externally.
//
// Run:  go run .
//
// While it's running:
//
//	open http://localhost:8080/ui/                              # dashboard
//	curl http://localhost:8080/admin/                           # discovery
//	curl http://localhost:8080/admin/health
//	curl http://localhost:8080/admin/info
//	curl http://localhost:8080/admin/registered
//	curl http://localhost:8080/admin/queues
//	curl http://localhost:8080/admin/workflows
//	curl http://localhost:8080/admin/workflows?status=PENDING
//	curl http://localhost:8080/admin/workflows/job-1
//	curl http://localhost:8080/admin/workflows/job-1/steps
//	curl http://localhost:8080/admin/workflows/parent-1/tree
//	curl -X POST http://localhost:8080/admin/workflows/job-1/cancel
//	curl -X POST http://localhost:8080/admin/workflows/job-1/resume
//	curl -X POST -d '{"start_from_step":1}' \
//	     http://localhost:8080/admin/workflows/job-1/fork
//	curl -X DELETE http://localhost:8080/admin/workflows/job-1
//	curl -X POST -d '{"ids":["job-1","job-2"]}' \
//	     http://localhost:8080/admin/workflows/delete
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"ella.to/orc"
	"ella.to/orc/web"
)

// longJob is a workflow that takes a few seconds to complete and exposes
// useful surface area for the admin endpoints (multiple steps, durable
// sleep, cancellation-friendly).
func longJob(c *orc.Context, label string) (string, error) {
	for i := 1; i <= 5; i++ {
		_, err := orc.RunAsStep(c, func(_ context.Context) (string, error) {
			return fmt.Sprintf("%s-step-%d", label, i), nil
		}, orc.WithStepName(fmt.Sprintf("step_%d", i)))
		if err != nil {
			return "", err
		}
		if _, err := orc.Sleep(c, 800*time.Millisecond); err != nil {
			return "", err
		}
	}
	return label + "-done", nil
}

// flakyJob fails on its first attempt and succeeds on the second so we can
// demonstrate the resume endpoint.
func flakyJob(c *orc.Context, _ string) (string, error) {
	st, _ := orc.RunAsStep(c, func(_ context.Context) (int32, error) {
		return failures.Add(1), nil
	}, orc.WithStepName("count"))
	if st == 1 {
		return "", errors.New("first attempt fails")
	}
	return "ok", nil
}

var failures atomic.Int32

// parentJob spawns a few child workflows so the /tree and /children
// endpoints have something to show.
func parentJob(c *orc.Context, _ string) (string, error) {
	for i := 0; i < 3; i++ {
		h, err := orc.RunWorkflow[string, string](c, longJob,
			fmt.Sprintf("child-%d", i),
			orc.WithWorkflowID(fmt.Sprintf("child-%d", i)))
		if err != nil {
			return "", err
		}
		if _, err := h.GetResult(orc.WithHandleTimeout(30 * time.Second)); err != nil {
			return "", err
		}
	}
	return "parent-done", nil
}

func main() {
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ctx, err := orc.NewContext(rootCtx, orc.Config{
		AppName:      "admin-demo",
		DatabasePath: "admin-demo.db",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer orc.Shutdown(ctx, 5*time.Second)

	orc.RegisterWorkflow[string, string](ctx, longJob, orc.WithWorkflowName("long_job"))
	orc.RegisterWorkflow[string, string](ctx, flakyJob, orc.WithWorkflowName("flaky_job"))
	orc.RegisterWorkflow[string, string](ctx, parentJob, orc.WithWorkflowName("parent_job"))
	orc.NewWorkflowQueue(ctx, "demo-queue", orc.WithWorkerConcurrency(2))

	if err := orc.Launch(ctx); err != nil {
		log.Fatal(err)
	}

	// Kick off some interesting workloads so the admin endpoints are
	// exercising real data when the user pokes them.
	go func() {
		_, _ = orc.RunWorkflow[string, string](ctx, longJob, "main",
			orc.WithWorkflowID("job-1"))
		_, _ = orc.RunWorkflow[string, string](ctx, longJob, "queued",
			orc.WithWorkflowID("job-2"),
			orc.WithQueue("demo-queue"))
		_, _ = orc.RunWorkflow[string, string](ctx, flakyJob, "",
			orc.WithWorkflowID("flaky-1"))
		_, _ = orc.RunWorkflow[string, string](ctx, parentJob, "",
			orc.WithWorkflowID("parent-1"))
	}()

	mux := http.NewServeMux()

	// Mount the admin handler under /admin. http.StripPrefix removes the
	// "/admin" portion before the inner ServeMux sees the request, so the
	// admin handler's routes ("/workflows", "/info", etc.) resolve correctly.
	mux.Handle("/admin/", http.StripPrefix("/admin", orc.AdminHandler(ctx,
		orc.WithAdminPrettyJSON(),
	)))

	// Mount the bundled HTML/JS dashboard under /ui. The UI talks back
	// to the admin handler at "/admin" (passed in below).
	mux.Handle("/ui/", http.StripPrefix("/ui", web.Handler("/admin")))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/ui/", http.StatusFound)
			return
		}
		http.NotFound(w, r)
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Wire graceful shutdown.
	go func() {
		<-rootCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Println("admin demo listening on http://localhost:8080  (Ctrl-C to stop)")
	log.Println("dashboard:  http://localhost:8080/ui/")
	log.Println("admin api:  http://localhost:8080/admin/")

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
