package redisadmin

import (
	"math"
	"testing"
)

func TestParseID(t *testing.T) {
	cases := map[string]ID{"1700000000000-3": {1700000000000, 3}, "5": {5, 0}}
	for in, want := range cases {
		got, err := ParseID(in)
		if err != nil || got != want {
			t.Errorf("ParseID(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"a-1", "1-", ""} {
		if _, err := ParseID(in); err == nil {
			t.Errorf("ParseID(%q) expected error", in)
		}
	}
	if s := (ID{7, 2}).String(); s != "7-2" {
		t.Errorf("String() = %q", s)
	}
	if id, _ := ParseID("7-2"); id.String() != "7-2" {
		t.Errorf("round trip failed: %v", id)
	}
}

func TestIDNext(t *testing.T) {
	if got := (ID{5, 3}).Next(); got != (ID{5, 4}) {
		t.Errorf("got %v", got)
	}
	if got := (ID{5, math.MaxUint64}).Next(); got != (ID{6, 0}) {
		t.Errorf("got %v", got)
	}
}

func TestParseVersion(t *testing.T) {
	v, err := ParseVersion("8.2.0")
	if err != nil || !v.AtLeast(8, 2, 0) {
		t.Errorf("8.2.0: %v %v", v, err)
	}
	v, _ = ParseVersion("8.1.9")
	if v.AtLeast(8, 2, 0) {
		t.Error("8.1.9 should not be >= 8.2.0")
	}
	if v, _ := ParseVersion("7.2.4-valkey"); v != (Version{7, 2, 4}) {
		t.Errorf("got %v", v)
	}
	if v, _ := ParseVersion("7"); v != (Version{7, 0, 0}) {
		t.Errorf("got %v", v)
	}
	if _, err := ParseVersion("x"); err == nil {
		t.Error("expected error")
	}
}

func TestSafeCutoff(t *testing.T) {
	if _, ok := SafeCutoff(ID{100, 0}, nil); ok {
		t.Error("no groups should give ok=false")
	}
	pend := &ID{150, 2}
	cases := []struct {
		name   string
		age    ID
		groups []GroupState
		want   ID
	}{
		{"age wins", ID{100, 0}, []GroupState{{"g", ID{200, 0}, nil}}, ID{100, 0}},
		{"delivered wins", ID{300, 0}, []GroupState{{"g", ID{200, 5}, nil}}, ID{200, 6}},
		{"pending wins", ID{300, 0}, []GroupState{{"g", ID{250, 0}, pend}}, ID{150, 2}},
		{"idle group", ID{300, 0}, []GroupState{{"a", ID{200, 0}, nil}, {"b", ID{0, 0}, nil}}, ID{0, 1}},
	}
	for _, c := range cases {
		got, ok := SafeCutoff(c.age, c.groups)
		if !ok || got != c.want {
			t.Errorf("%s: got %v, %v; want %v", c.name, got, ok, c.want)
		}
	}
}

func TestParseTaskMessage(t *testing.T) {
	id, name, ok := ParseTaskMessage([]byte(`{"task_id":"abc","task_name":"mod:fn","labels":{},"args":[],"kwargs":{}}`))
	if !ok || id != "abc" || name != "mod:fn" {
		t.Errorf("got %q %q %v", id, name, ok)
	}
	for _, in := range []string{"\x80\x04pickle", "{}", "[1]"} {
		if _, _, ok := ParseTaskMessage([]byte(in)); ok {
			t.Errorf("%q should not parse", in)
		}
	}
}

func TestDeliveredCutoff(t *testing.T) {
	pend := &ID{150, 2}
	cases := []struct {
		name   string
		age    ID
		groups []GroupState
		want   ID
	}{
		{"age wins", ID{100, 0}, []GroupState{{"g", ID{200, 0}, nil}}, ID{100, 0}},
		{"delivered wins", ID{300, 0}, []GroupState{{"g", ID{200, 5}, nil}}, ID{200, 6}},
		{"pending ignored", ID{300, 0}, []GroupState{{"g", ID{250, 0}, pend}}, ID{250, 1}},
		{"slowest group", ID{300, 0}, []GroupState{{"a", ID{200, 0}, nil}, {"b", ID{120, 0}, nil}}, ID{120, 1}},
	}
	for _, c := range cases {
		if got := deliveredCutoff(c.age, c.groups); got != c.want {
			t.Errorf("%s: got %v; want %v", c.name, got, c.want)
		}
	}
}
