package orc

import "sync"

// notifyHub wakes in-process waiters the moment the thing they are polling
// for changes, so same-process Send / SetEvent / workflow completion are
// observed immediately instead of after up to NotificationPollInterval.
//
// It is strictly a latency optimization: every waiter still polls the
// database as a fallback, which is what makes cross-process delivery work.
// Missing a signal is therefore never a correctness problem.
type notifyHub struct {
	mu   sync.Mutex
	subs map[string][]chan struct{}
}

func newNotifyHub() *notifyHub {
	return &notifyHub{subs: make(map[string][]chan struct{})}
}

// Keyspace helpers. Keys are namespaced so notifications, events and
// workflow-completion signals can't collide.
func notificationKey(destinationID, topic string) string { return "n|" + destinationID + "|" + topic }
func eventKey(workflowID, key string) string             { return "e|" + workflowID + "|" + key }
func workflowDoneKey(workflowID string) string           { return "w|" + workflowID }

// subscribe registers interest in key and returns a channel with capacity 1.
// Callers must subscribe BEFORE checking the database (subscribe → poll →
// wait) so a signal arriving in between is never lost, and must call
// unsubscribe when done.
func (h *notifyHub) subscribe(key string) chan struct{} {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	h.subs[key] = append(h.subs[key], ch)
	h.mu.Unlock()
	return ch
}

func (h *notifyHub) unsubscribe(key string, ch chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	subs := h.subs[key]
	for i, c := range subs {
		if c == ch {
			subs[i] = subs[len(subs)-1]
			subs = subs[:len(subs)-1]
			break
		}
	}
	if len(subs) == 0 {
		delete(h.subs, key)
	} else {
		h.subs[key] = subs
	}
}

// signal wakes every current subscriber of key. Non-blocking: a subscriber
// that already has a pending signal is not queued a second one (its next DB
// poll observes everything that happened in the meantime). Delivery happens
// under the lock — each send is non-blocking, and holding the lock avoids
// racing unsubscribe's in-place slice mutation.
func (h *notifyHub) signal(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.subs[key] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
