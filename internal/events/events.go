// Package events broadcasts "the data changed" notifications to open pages.
package events

import "sync"

// Hub fans out change notifications. Notifications carry no payload and are
// coalesced: a slow subscriber sees at most one pending notification.
type Hub struct {
	mu     sync.Mutex
	subs   map[chan struct{}]struct{}
	closed bool
}

func NewHub() *Hub { return &Hub{subs: map[chan struct{}]struct{}{}} }

// Subscribe returns a channel that receives a value after changes, and is
// closed when the hub closes. Call cancel when done.
func (h *Hub) Subscribe() (ch <-chan struct{}, cancel func()) {
	c := make(chan struct{}, 1)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		close(c)
		return c, func() {}
	}
	h.subs[c] = struct{}{}
	return c, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, ok := h.subs[c]; ok {
			delete(h.subs, c)
			close(c)
		}
	}
}

// Publish notifies every subscriber without blocking.
func (h *Hub) Publish() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.subs {
		select {
		case c <- struct{}{}:
		default: // already has a pending notification
		}
	}
}

// Close disconnects every subscriber (used at shutdown).
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for c := range h.subs {
		delete(h.subs, c)
		close(c)
	}
}
