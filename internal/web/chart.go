package web

import (
	"fmt"
	"math"
	"net/http"
	"net/url"
	"time"

	"github.com/louis-t/taskiq-go-admin/internal/store"
	"github.com/louis-t/taskiq-go-admin/internal/task"
)

const (
	maxBuckets    = 400
	targetBuckets = 60
)

type rangePreset struct {
	Key, Label string
	Span       time.Duration
}

var rangePresets = []rangePreset{
	{"1h", "1 hour", time.Hour},
	{"6h", "6 hours", 6 * time.Hour},
	{"24h", "24 hours", 24 * time.Hour},
	{"7d", "7 days", 7 * 24 * time.Hour},
	{"30d", "30 days", 30 * 24 * time.Hour},
}

const defaultRange = "24h"

var bucketSteps = []time.Duration{
	time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 15 * time.Minute, 30 * time.Minute,
	time.Hour, 2 * time.Hour, 3 * time.Hour, 6 * time.Hour, 12 * time.Hour, 24 * time.Hour,
	2 * 24 * time.Hour, 7 * 24 * time.Hour,
}

// stackOrder is the bottom-to-top order of the stacked segments. The order
// keeps each adjacent pair distinguishable under color-vision deficiency
// (success green never touches failure red).
var stackOrder = []task.State{task.Success, task.Queued, task.Running, task.Failure, task.Abandoned}

type window struct {
	From, To time.Time
	Bucket   time.Duration
	Preset   string // empty for a custom date range
	Offset   time.Duration
}

func presetByKey(key string) rangePreset {
	for _, p := range rangePresets {
		if p.Key == key {
			return p
		}
	}
	return presetByKey(defaultRange)
}

// chartWindow picks the period and bucket size: the list's date filter when
// set, otherwise the range preset ending now. Bucket boundaries are aligned
// on the viewer's local time.
func chartWindow(q taskQuery, preset string, now time.Time, loc *time.Location) window {
	w := window{}
	if !q.From.IsZero() || !q.To.IsZero() {
		w.To = q.To
		if w.To.IsZero() {
			w.To = now
		}
		w.From = q.From
		if w.From.IsZero() {
			w.From = w.To.Add(-24 * time.Hour)
		}
	} else {
		p := presetByKey(preset)
		w.Preset = p.Key
		w.To, w.From = now, now.Add(-p.Span)
	}
	if !w.From.Before(w.To) {
		w.From = w.To.Add(-time.Minute)
	}
	span := w.To.Sub(w.From)
	w.Bucket = bucketSteps[len(bucketSteps)-1]
	for _, step := range bucketSteps {
		if span/step <= targetBuckets {
			w.Bucket = step
			break
		}
	}
	_, off := w.To.In(loc).Zone()
	w.Offset = time.Duration(off) * time.Second
	w.From = alignDown(w.From, w.Bucket, w.Offset)
	return w
}

func alignDown(t time.Time, bucket, offset time.Duration) time.Time {
	ms, b, off := t.UnixMilli(), bucket.Milliseconds(), offset.Milliseconds()
	v := ms + off
	floor := v / b * b
	if v < 0 && v%b != 0 {
		floor -= b
	}
	return time.UnixMilli(floor - off).UTC()
}

func (s *Server) histogram(r *http.Request, f store.Filter, w window) ([]store.HistogramRow, error) {
	f.From, f.To = w.From, w.To
	return s.store.Histogram(r.Context(), store.HistogramQuery{Filter: f, Bucket: w.Bucket, Offset: w.Offset})
}

type chartSeg struct {
	State task.State
	Count int64
}

type chartBar struct {
	Start, End time.Time
	Label      string
	Segs       []chartSeg // stack order, zero counts omitted
	Breakdown  []chartSeg // display order, for the tooltip
	Total      int64
	HeightPct  float64
	Href       string
	TipEdge    string // "start", "end" or "" (centered)
}

type chartTick struct {
	Label  string
	Bottom float64 // % from the plot bottom
}

type chartXLabel struct {
	Label string
	Left  float64 // % from the plot left
}

type chartView struct {
	Bars    []chartBar
	Ticks   []chartTick
	XLabels []chartXLabel
	Totals  []chartSeg // display order, every state
	Total   int64
	Bucket  string
	Period  string
	Preset  string
	Presets []rangePreset
}

// buildChart turns histogram rows into the view model of the stacked bar
// chart, rendered as plain HTML/CSS so it stays responsive.
func buildChart(rows []store.HistogramRow, w window, loc *time.Location, hrefFor func(from, to time.Time) string) *chartView {
	counts := map[int64]map[task.State]int64{}
	totals := map[task.State]int64{}
	for _, r := range rows {
		k := r.Start.UnixMilli()
		if counts[k] == nil {
			counts[k] = map[task.State]int64{}
		}
		counts[k][r.State] += r.Count
		totals[r.State] += r.Count
	}

	v := &chartView{Bucket: humanDuration(w.Bucket), Preset: w.Preset, Presets: rangePresets}
	v.Period = fmt.Sprintf("%s – %s", w.From.In(loc).Format("Jan 2 15:04"), w.To.In(loc).Format("Jan 2 15:04"))

	var maxTotal int64
	for start := w.From; start.Before(w.To) && len(v.Bars) < maxBuckets; start = start.Add(w.Bucket) {
		end := start.Add(w.Bucket)
		bar := chartBar{Start: start, End: end, Label: bucketLabel(start, end, w.Bucket, loc)}
		c := counts[start.UnixMilli()]
		for _, st := range stackOrder {
			if n := c[st]; n > 0 {
				bar.Segs = append(bar.Segs, chartSeg{st, n})
				bar.Total += n
			}
		}
		for _, st := range task.States {
			if n := c[st]; n > 0 {
				bar.Breakdown = append(bar.Breakdown, chartSeg{st, n})
			}
		}
		if hrefFor != nil {
			bar.Href = hrefFor(start, end)
		}
		maxTotal = max(maxTotal, bar.Total)
		v.Bars = append(v.Bars, bar)
	}

	top, step := niceScale(maxTotal)
	for i := range v.Bars {
		v.Bars[i].HeightPct = float64(v.Bars[i].Total) / float64(top) * 100
		switch pos := float64(i) / float64(len(v.Bars)); {
		case pos < 0.2:
			v.Bars[i].TipEdge = "start"
		case pos > 0.8:
			v.Bars[i].TipEdge = "end"
		}
	}
	for t := int64(0); t <= top; t += step {
		v.Ticks = append(v.Ticks, chartTick{Label: compactCount(t), Bottom: float64(t) / float64(top) * 100})
	}
	n := len(v.Bars)
	labels := min(6, n)
	for i := range labels {
		idx := i * n / labels
		v.XLabels = append(v.XLabels, chartXLabel{
			Label: axisLabel(v.Bars[idx].Start, w, loc),
			Left:  (float64(idx) + 0.5) / float64(n) * 100,
		})
	}
	for _, st := range task.States {
		v.Totals = append(v.Totals, chartSeg{st, totals[st]})
		v.Total += totals[st]
	}
	return v
}

// niceScale returns an axis maximum >= m and a tick step from the 1-2-5
// series, giving 2 to 5 ticks.
func niceScale(m int64) (top, step int64) {
	if m <= 0 {
		return 4, 1
	}
	raw := float64(m) / 4
	mag := math.Pow(10, math.Floor(math.Log10(raw)))
	for _, f := range []float64{1, 2, 5, 10} {
		if f*mag >= raw {
			step = int64(math.Max(1, f*mag))
			break
		}
	}
	top = (m + step - 1) / step * step
	return top, step
}

func compactCount(n int64) string {
	switch {
	case n >= 1_000_000:
		return trimZero(fmt.Sprintf("%.1f", float64(n)/1e6)) + "M"
	case n >= 10_000:
		return trimZero(fmt.Sprintf("%.1f", float64(n)/1e3)) + "k"
	}
	return fmt.Sprint(n)
}

func trimZero(s string) string {
	if len(s) > 2 && s[len(s)-2:] == ".0" {
		return s[:len(s)-2]
	}
	return s
}

func axisLabel(t time.Time, w window, loc *time.Location) string {
	t = t.In(loc)
	switch {
	case w.Bucket >= 24*time.Hour:
		return t.Format("Jan 2")
	case w.To.Sub(w.From) > 24*time.Hour:
		return t.Format("Jan 2 15:04")
	}
	return t.Format("15:04")
}

func bucketLabel(start, end time.Time, bucket time.Duration, loc *time.Location) string {
	s, e := start.In(loc), end.In(loc)
	if bucket >= 24*time.Hour {
		return s.Format("Mon Jan 2") + " – " + e.Format("Mon Jan 2")
	}
	if s.YearDay() == e.YearDay() {
		return s.Format("Mon Jan 2, 15:04") + " – " + e.Format("15:04")
	}
	return s.Format("Mon Jan 2, 15:04") + " – " + e.Format("Jan 2, 15:04")
}

func humanDuration(d time.Duration) string {
	switch {
	case d%(24*time.Hour) == 0:
		if d == 24*time.Hour {
			return "1 day"
		}
		return fmt.Sprintf("%d days", d/(24*time.Hour))
	case d%time.Hour == 0:
		if d == time.Hour {
			return "1 hour"
		}
		return fmt.Sprintf("%d hours", d/time.Hour)
	case d%time.Minute == 0:
		if d == time.Minute {
			return "1 minute"
		}
		return fmt.Sprintf("%d minutes", d/time.Minute)
	}
	return d.String()
}

// drillDownValues returns the list filters narrowed to [from, to).
func drillDownValues(base url.Values, from, to time.Time, loc *time.Location) url.Values {
	v := url.Values{}
	for k, vals := range base {
		v[k] = vals
	}
	v.Set("startDate", from.In(loc).Format("2006-01-02T15:04"))
	// endDate is inclusive at minute precision.
	v.Set("endDate", to.Add(-time.Minute).In(loc).Format("2006-01-02T15:04"))
	return v
}
