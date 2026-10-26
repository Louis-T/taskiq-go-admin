package web

import (
	"fmt"
	"net/http"
	"time"
)

const (
	// sseMinInterval caps how often a page is told to refresh, so a busy
	// queue costs at most one refresh per second per open tab.
	sseMinInterval = time.Second
	sseHeartbeat   = 25 * time.Second
)

// events streams "changed" notifications (Server-Sent Events). Pages refresh
// their live parts when they receive one, instead of polling.
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{}) // long-lived response

	changes, cancel := s.hub.Subscribe()
	defer cancel()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no") // nginx: do not buffer
	send := func(msg string) bool {
		if _, err := fmt.Fprint(w, msg); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	if !send("retry: 3000\n\n") {
		return
	}

	heartbeat := time.NewTicker(sseHeartbeat)
	defer heartbeat.Stop()
	var last time.Time
	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			if !send(": ping\n\n") {
				return
			}
		case _, ok := <-changes:
			if !ok {
				return // server shutting down
			}
			if wait := sseMinInterval - time.Since(last); wait > 0 {
				select {
				case <-time.After(wait):
				case <-r.Context().Done():
					return
				}
				// drop notifications that arrived while waiting
				select {
				case <-changes:
				default:
				}
			}
			last = time.Now()
			if !send("event: changed\ndata: {}\n\n") {
				return
			}
		}
	}
}
