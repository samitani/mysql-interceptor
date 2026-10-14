package logging

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/takaidohigasi/mysql-interceptor/internal/segment"
)

// rotateHarness drives an hourly rotating Logger with a settable clock
// and collects the segment events it reports.
type rotateHarness struct {
	t   *testing.T
	dir string
	l   *Logger

	mu     sync.Mutex
	clock  time.Time
	events []segment.Event
}

func newRotateHarness(t *testing.T, minSize int64, start time.Time) *rotateHarness {
	t.Helper()
	h := &rotateHarness{t: t, dir: t.TempDir(), clock: start}
	h.open(minSize)
	return h
}

func (h *rotateHarness) open(minSize int64) {
	h.t.Helper()
	l, err := NewLogger(LoggerConfig{
		Enabled:       true,
		Level:         "all",
		OutputDir:     h.dir,
		FilePrefix:    "queries",
		RotateHourly:  true,
		RotateMinSize: minSize,
		OnSegment: func(ev segment.Event) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.events = append(h.events, ev)
		},
		Now: func() time.Time {
			h.mu.Lock()
			defer h.mu.Unlock()
			return h.clock
		},
	})
	if err != nil {
		h.t.Fatal(err)
	}
	h.l = l
}

func (h *rotateHarness) path() string { return filepath.Join(h.dir, "queries.jsonl") }

func (h *rotateHarness) setClock(t time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clock = t
}

func (h *rotateHarness) got() []segment.Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]segment.Event(nil), h.events...)
}

func (h *rotateHarness) log(query string) {
	h.l.Log(LogEntry{Query: query})
}

func readLines(t *testing.T, p string) []string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimRight(string(b), "\n"), "\n")
}

func TestLoggerRotate_ClosesTheHourAsASegment(t *testing.T) {
	t9 := time.Date(2026, 10, 9, 9, 0, 0, 0, time.UTC)
	h := newRotateHarness(t, 0, t9)

	h.log("SELECT 1")
	h.log("SELECT 2")
	h.setClock(t9.Add(time.Hour))
	h.l.rotateNow()
	h.log("SELECT 3")
	h.l.Close()

	seg9 := segment.Name(h.path(), t9)
	seg10 := segment.Name(h.path(), t9.Add(time.Hour))
	evs := h.got()
	if len(evs) != 2 {
		t.Fatalf("events = %+v, want 2", evs)
	}
	if evs[0].Path != seg9 || !evs[0].Closed || !evs[0].Start.Equal(t9) {
		t.Errorf("first event = %+v, want closed %s", evs[0], seg9)
	}
	if evs[1].Path != seg10 || !evs[1].Closed {
		t.Errorf("close event = %+v, want closed %s", evs[1], seg10)
	}
	if got := readLines(t, seg9); len(got) != 2 || !strings.Contains(got[1], "SELECT 2") {
		t.Errorf("hour 9 segment = %q", got)
	}
	if st, err := os.Stat(seg9); err != nil || st.Size() != evs[0].Size {
		t.Errorf("segment size = %v (err %v), event size %d", st, err, evs[0].Size)
	}
	if got := readLines(t, seg10); len(got) != 1 || !strings.Contains(got[0], "SELECT 3") {
		t.Errorf("hour 10 segment = %q", got)
	}
	if _, err := os.Stat(h.path()); !os.IsNotExist(err) {
		t.Errorf("live file should be gone after Close, stat err = %v", err)
	}
}

func TestLoggerRotate_SmallFileIsKeptAndReportedAsSnapshot(t *testing.T) {
	t9 := time.Date(2026, 10, 9, 9, 0, 0, 0, time.UTC)
	h := newRotateHarness(t, 1<<20, t9)

	h.log("SELECT 1")
	h.setClock(t9.Add(time.Hour))
	h.l.rotateNow()
	h.log("SELECT 2")
	h.l.Close()

	evs := h.got()
	if len(evs) != 2 {
		t.Fatalf("events = %+v, want 2", evs)
	}
	if evs[0].Path != h.path() || evs[0].Closed || !evs[0].Start.Equal(t9) {
		t.Errorf("snapshot event = %+v, want open %s started %v", evs[0], h.path(), t9)
	}
	// The small hour is written on, so the closing segment keeps its start.
	seg := segment.Name(h.path(), t9)
	if evs[1].Path != seg || !evs[1].Closed {
		t.Errorf("close event = %+v, want closed %s", evs[1], seg)
	}
	if got := readLines(t, seg); len(got) != 2 {
		t.Errorf("segment = %q, want both entries", got)
	}
}

func TestLoggerRotate_EmptyFileIsNotRotated(t *testing.T) {
	t9 := time.Date(2026, 10, 9, 9, 0, 0, 0, time.UTC)
	h := newRotateHarness(t, 0, t9)

	h.setClock(t9.Add(time.Hour))
	h.l.rotateNow()
	h.l.Close()

	if evs := h.got(); len(evs) != 0 {
		t.Errorf("events = %+v, want none", evs)
	}
	if segs, _ := segment.List(h.path()); len(segs) != 0 {
		t.Errorf("segments = %v, want none", segs)
	}
}

func TestLoggerRotate_AdoptsALeftoverFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "queries.jsonl")
	if err := os.WriteFile(p, []byte("{\"query\":\"old\"}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	mtime := time.Date(2026, 10, 9, 8, 30, 0, 0, time.UTC)
	if err := os.Chtimes(p, mtime, mtime); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var evs []segment.Event
	l, err := NewLogger(LoggerConfig{
		Enabled: true, OutputDir: dir, FilePrefix: "queries", RotateHourly: true,
		OnSegment: func(ev segment.Event) {
			mu.Lock()
			defer mu.Unlock()
			evs = append(evs, ev)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	seg := segment.Name(p, mtime)
	mu.Lock()
	defer mu.Unlock()
	if len(evs) != 1 || evs[0].Path != seg || !evs[0].Closed || !evs[0].Start.Equal(mtime) {
		t.Fatalf("events = %+v, want the leftover as closed %s", evs, seg)
	}
	if st, err := os.Stat(p); err != nil || st.Size() != 0 {
		t.Errorf("live file should start empty: %v %v", st, err)
	}
}

func TestLoggerRotate_SizeModeIgnoresRotateNow(t *testing.T) {
	dir := t.TempDir()
	l, err := NewLogger(LoggerConfig{Enabled: true, Level: "all", OutputDir: dir, FilePrefix: "queries", MaxSizeMB: 1})
	if err != nil {
		t.Fatal(err)
	}
	l.Log(LogEntry{Query: "SELECT 1"})
	l.rotateNow()
	l.Close()

	p := filepath.Join(dir, "queries.jsonl")
	if got := readLines(t, p); len(got) != 1 {
		t.Errorf("log = %q, want one entry", got)
	}
	if segs, _ := segment.List(p); len(segs) != 0 {
		t.Errorf("segments = %v, want none", segs)
	}
}
