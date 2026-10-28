// Package redisadmin inspects the taskiq Redis queues and cleans their
// streams for the admin panel.
package redisadmin

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ID is a Redis stream entry ID: "<milliseconds>-<sequence>".
type ID struct{ Ms, Seq uint64 }

// ParseID parses "1700000000000-3"; a bare "1700000000000" means sequence 0.
func ParseID(s string) (ID, error) {
	msPart, seqPart, hasSeq := strings.Cut(s, "-")
	ms, err := strconv.ParseUint(msPart, 10, 64)
	if err != nil {
		return ID{}, fmt.Errorf("invalid stream id %q: %w", s, err)
	}
	var seq uint64
	if hasSeq {
		if seq, err = strconv.ParseUint(seqPart, 10, 64); err != nil {
			return ID{}, fmt.Errorf("invalid stream id %q: %w", s, err)
		}
	}
	return ID{Ms: ms, Seq: seq}, nil
}

func (a ID) String() string { return fmt.Sprintf("%d-%d", a.Ms, a.Seq) }

func (a ID) Less(b ID) bool {
	return a.Ms < b.Ms || (a.Ms == b.Ms && a.Seq < b.Seq)
}

// Next returns the smallest ID greater than a.
func (a ID) Next() ID {
	if a.Seq == ^uint64(0) {
		return ID{Ms: a.Ms + 1}
	}
	return ID{Ms: a.Ms, Seq: a.Seq + 1}
}

func (a ID) Time() time.Time { return time.UnixMilli(int64(a.Ms)) }

// IDAt returns the lowest ID created at t.
func IDAt(t time.Time) ID { return ID{Ms: uint64(t.UnixMilli())} }
