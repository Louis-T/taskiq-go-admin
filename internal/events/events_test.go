package events

import "testing"

func TestPublishCoalescesAndClose(t *testing.T) {
	h := NewHub()
	ch, cancel := h.Subscribe()
	defer cancel()
	h.Publish()
	h.Publish() // must not block nor queue a second notification
	<-ch
	select {
	case <-ch:
		t.Fatal("notifications were not coalesced")
	default:
	}
	h.Close()
	if _, ok := <-ch; ok {
		t.Fatal("channel not closed by Close")
	}
	cancel() // safe after Close
	late, _ := h.Subscribe()
	if _, ok := <-late; ok {
		t.Fatal("subscribe after close must return a closed channel")
	}
}
