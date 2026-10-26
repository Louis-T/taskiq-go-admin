package web

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/louis-t/taskiq-go-admin/internal/task"
)

//go:embed templates
var templateFS embed.FS

// pages holds one template set per page (layout + page + shared partials),
// so each page can define its own "content" block.
type pages struct {
	sets map[string]*template.Template
}

func parsePages() (*pages, error) {
	p := &pages{sets: map[string]*template.Template{}}
	for name, files := range map[string][]string{
		"tasks":    {"list.html", "tasks.html"},
		"type":     {"list.html", "type.html"},
		"types":    {"types.html"},
		"task":     {"task.html"},
		"settings": {"settings.html"},
	} {
		patterns := []string{"templates/layout.html", "templates/partials.html"}
		for _, f := range files {
			patterns = append(patterns, "templates/"+f)
		}
		t, err := template.New(name).Funcs(funcs).ParseFS(templateFS, patterns...)
		if err != nil {
			return nil, fmt.Errorf("parse %s templates: %w", name, err)
		}
		p.sets[name] = t
	}
	return p, nil
}

func (p *pages) execute(w io.Writer, page, block string, data any) error {
	t, ok := p.sets[page]
	if !ok {
		return fmt.Errorf("unknown page %q", page)
	}
	return t.ExecuteTemplate(w, block, data)
}

var funcs = template.FuncMap{
	"fmtTime":     fmtTime,
	"shortTime":   func(loc *time.Location, v any) string { return formatTime(loc, v, "Jan 2 15:04:05") },
	"isoTime":     isoTime,
	"shortName":   shortName,
	"trunc":       trunc,
	"jsonInline":  jsonInline,
	"jsonPretty":  jsonPretty,
	"returnValue": returnValue,
	"deref":       deref,
	"seconds":     seconds,
	"duration":    taskDuration,
	"percent":     func(f float64) string { return strconv.FormatFloat(f, 'f', 2, 64) + "%" },
	"stateLabel":  func(s task.State) string { return string(s) },
	"pathEscape":  url.PathEscape,
	"typeURL":     typeURL,
	"count":       func(m map[task.State]int64, s string) int64 { return m[task.State(s)] },
	"ratio":       ratio,
	"secs":        humanSeconds,
	"wait":        humanWait,
	"timeline":    buildTimeline,
	"asset":       func(name string) string { return "/static/" + name + "?v=" + assetVersion },
	"positive":    func(v *float64) bool { return v != nil && *v > 0 },
}

// assetVersion fingerprints the embedded static files, so browsers fetch new
// CSS/JS after an upgrade instead of using a stale cached copy.
var assetVersion = func() string {
	h := sha256.New()
	_ = fs.WalkDir(staticFS, "static", func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			b, _ := staticFS.ReadFile(path)
			h.Write([]byte(path))
			h.Write(b)
		}
		return nil
	})
	return hex.EncodeToString(h.Sum(nil))[:12]
}()

func ratio(r *float64) string {
	if r == nil {
		return "—"
	}
	return strconv.FormatFloat(*r*100, 'f', 1, 64) + "%"
}

// humanSeconds renders a duration in seconds: "850 ms", "2.81 s", "3m 05s".
func humanSeconds(v *float64) string {
	if v == nil {
		return "—"
	}
	return humanDur(time.Duration(*v * float64(time.Second)))
}

func humanWait(d *time.Duration) string {
	if d == nil {
		return "—"
	}
	return humanDur(*d)
}

func humanDur(d time.Duration) string {
	switch {
	case d < time.Second:
		return strconv.FormatInt(d.Milliseconds(), 10) + " ms"
	case d < time.Minute:
		return strconv.FormatFloat(d.Seconds(), 'f', 2, 64) + " s"
	case d < time.Hour:
		return fmt.Sprintf("%dm %02ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh %02dm", int(d.Hours()), int(d.Minutes())%60)
}

// fmtTime formats a time.Time or *time.Time in loc ("" for nil). With
// millis, milliseconds are shown.
func fmtTime(loc *time.Location, v any, millis bool) string {
	layout := "Jan 2, 2006 15:04:05"
	if millis {
		layout += ".000"
	}
	return formatTime(loc, v, layout)
}

func formatTime(loc *time.Location, v any, layout string) string {
	switch x := v.(type) {
	case time.Time:
		return x.In(loc).Format(layout)
	case *time.Time:
		if x != nil {
			return x.In(loc).Format(layout)
		}
	}
	return ""
}

func isoTime(v any) string {
	switch x := v.(type) {
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case *time.Time:
		if x != nil {
			return x.UTC().Format(time.RFC3339Nano)
		}
	}
	return ""
}

// shortName drops the module path ("app.tasks:send_email" -> "send_email").
func shortName(name string) string {
	if i := strings.LastIndex(name, ":"); i >= 0 && i < len(name)-1 {
		return name[i+1:]
	}
	return name
}

// trunc shortens s to at most n runes (taskiq-admin#7: never assume the
// value is a string of a given shape).
func trunc(n int, s string) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return strings.TrimSpace(string(r[:n])) + "…"
}

func jsonInline(raw json.RawMessage) string {
	if raw == nil {
		return ""
	}
	return string(raw)
}

func jsonPretty(raw json.RawMessage) string {
	if raw == nil {
		return "null"
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		return string(raw)
	}
	return buf.String()
}

// returnValue extracts "return_value" from the stored {"return_value": x}.
func returnValue(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	var wrapper map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wrapper); err == nil {
		if v, ok := wrapper["return_value"]; ok {
			return v
		}
	}
	return raw
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func seconds(f *float64) string {
	if f == nil {
		return ""
	}
	return strconv.FormatFloat(*f, 'f', -1, 64)
}

// taskDuration is the wall time from start to finish, "" when unknown.
func taskDuration(t *task.Task) string {
	if t.StartedAt == nil || t.FinishedAt == nil {
		return ""
	}
	return t.FinishedAt.Sub(*t.StartedAt).Round(time.Millisecond).String()
}

// timeline is the queued → started → finished bar of the details page.
type timeline struct {
	WaitMs, RunMs    int64 // segment widths (flex-grow)
	Wait, Run, Total string
	WaitOpen         bool // still queued
	RunOpen          bool // still running
	RunKnown         bool
	State            task.State
}

func buildTimeline(t *task.Task) *timeline {
	now := time.Now()
	tl := &timeline{State: t.State}
	switch {
	case t.StartedAt == nil:
		if t.State != task.Queued {
			return nil
		}
		tl.WaitMs, tl.WaitOpen = now.Sub(t.QueuedAt).Milliseconds(), true
	default:
		tl.WaitMs = max(t.StartedAt.Sub(t.QueuedAt).Milliseconds(), 0)
		switch {
		case t.FinishedAt != nil:
			tl.RunMs, tl.RunKnown = max(t.FinishedAt.Sub(*t.StartedAt).Milliseconds(), 0), true
		case t.State == task.Running:
			tl.RunMs, tl.RunKnown, tl.RunOpen = max(now.Sub(*t.StartedAt).Milliseconds(), 0), true, true
		}
	}
	ms := func(v int64) string { return humanDur(time.Duration(v) * time.Millisecond) }
	tl.Wait, tl.Run, tl.Total = ms(tl.WaitMs), ms(tl.RunMs), ms(tl.WaitMs+tl.RunMs)
	return tl
}
