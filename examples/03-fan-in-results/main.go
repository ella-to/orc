// Example 03 — Fan-in: many worker workflows ship their results to a single
// aggregator workflow.
//
// Demonstrates:
//   - Spawning N worker workflows on a queue
//   - Each worker publishes its result to the aggregator via Send
//   - The aggregator workflow loops on Recv and totals everything up
//   - Cross-workflow communication via topics
//
// This is a typical map-reduce pattern: workers do the heavy lifting,
// aggregator combines.
//
// Run:  go run .
package main

import (
	"context"
	"fmt"
	"time"

	"ella.to/orc"
)

const aggregatorID = "aggregator-1"
const topic = "shard-result"

// shardWorker computes a "result" for a shard of work and ships it to the
// aggregator. In a real app this might be reading a partition, scoring a
// model, etc.
func shardWorker(c *orc.Context, shardID int) (int, error) {
	// Simulate some work.
	score := shardID*shardID + 1
	fmt.Printf("worker shard=%d  score=%d\n", shardID, score)

	// Send is durable when invoked from inside a workflow — it's recorded
	// as a step so the message is delivered at most once even if the worker
	// restarts.
	if err := orc.Send(c, aggregatorID, score, topic); err != nil {
		return 0, err
	}
	return score, nil
}

// aggregator sits in a Recv loop until it has gathered `expected` results,
// then returns the sum. It's a single, durable rendezvous point.
func aggregator(c *orc.Context, expected int) (int, error) {
	total := 0
	for i := 0; i < expected; i++ {
		v, err := orc.Recv[int](c, topic, 30*time.Second)
		if err != nil {
			return 0, err
		}
		fmt.Printf("aggregator got #%d => %d\n", i+1, v)
		total += v
	}
	fmt.Printf("aggregator total = %d\n", total)
	return total, nil
}

func main() {
	ctx, err := orc.NewContext(context.Background(), orc.Config{
		AppName:      "fan-in",
		DatabasePath: "fan-in.db",
	})
	if err != nil {
		panic(err)
	}
	defer orc.Shutdown(ctx, 5*time.Second)

	orc.RegisterWorkflow[int, int](ctx, shardWorker,
		orc.WithWorkflowName("shard_worker"))
	orc.RegisterWorkflow[int, int](ctx, aggregator,
		orc.WithWorkflowName("aggregator"))

	// Workers run on a background queue with bounded concurrency.
	q := orc.NewWorkflowQueue(ctx, "shards", orc.WithWorkerConcurrency(4))

	if err := orc.Launch(ctx); err != nil {
		panic(err)
	}

	const N = 8

	// Start the aggregator first so it's ready to Recv.
	aggH, err := orc.RunWorkflow[int, int](ctx, aggregator, N,
		orc.WithWorkflowID(aggregatorID))
	if err != nil {
		panic(err)
	}

	// Fan out the workers.
	for i := 0; i < N; i++ {
		_, err := orc.RunWorkflow[int, int](ctx, shardWorker, i,
			orc.WithQueue(q.Name))
		if err != nil {
			panic(err)
		}
	}

	// Wait for the aggregator to finish.
	total, err := aggH.GetResult(orc.WithHandleTimeout(60 * time.Second))
	if err != nil {
		panic(err)
	}
	fmt.Printf("\nfinal aggregated total = %d\n", total)
}
