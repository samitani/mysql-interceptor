package logging

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/takaidohigasi/mysql-interceptor/internal/metrics"
	"github.com/takaidohigasi/mysql-interceptor/internal/segment"
	"gopkg.in/lumberjack.v2"
)

type Logger struct {
	entryCh    chan LogEntry
	stop       chan struct{}
	done       chan struct{}
	writer     io.WriteCloser
	enabled    atomic.Bool
	errorsOnly atomic.Bool
	closed     atomic.Bool
	dropped    atomic.Int64
	once       sync.Once

	// Hourly rotation (RotateHourly); the fields below are owned by the
	// write loop, and by Close once the loop has exited.
	path      string
	rotate    bool
	rotateMin int64
	onSegment func(segment.Event)
	now       func() time.Time
	segStart  time.Time // when the current file started
	broken    bool      // reopening path failed; writing to io.Discard
	rotateReq chan chan struct{}
}

type LoggerConfig struct {
	Enabled bool
	// Level is "all" (default) or "error". See Logger.SetLevel.
	Level      string
	OutputDir  string
	FilePrefix string
	QueueSize  int // channel buffer size; 0 → default 10000
	// Size-based rotation (lumberjack), used unless RotateHourly is set.
	MaxSizeMB  int
	MaxAgeDays int
	MaxBackups int
	Compress   bool

	// RotateHourly splits the log at every UTC hour instead: the current
	// file is renamed to segment.Name(<file>, <its start>) and a fresh one
	// is opened. A file smaller than RotateMinSize bytes is kept and
	// written on through the next hour; an empty one is never rotated.
	// Close always closes the last non-empty file as a segment, and a
	// non-empty file left by a previous process becomes a segment at
	// start-up, named after its last modification. Closed segments are
	// not removed here: OnSegment's receiver (upload.Shipper) prunes them.
	RotateHourly  bool
	RotateMinSize int64
	// OnSegment is called every hour with the file's state (and at
	// start-up / Close for the segments they close), from the write loop
	// or Close. It must not block.
	OnSegment func(segment.Event)
	// Now returns the current time; for tests.
	Now func() time.Time
}

// Path returns the file the logger configured by cfg writes to.
func (cfg LoggerConfig) Path() string {
	return filepath.Join(cfg.OutputDir, cfg.FilePrefix+".jsonl")
}

func NewLogger(cfg LoggerConfig) (*Logger, error) {
	if cfg.OutputDir != "" {
		if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
			return nil, fmt.Errorf("creating log output dir: %w", err)
		}
	}

	filename := cfg.Path()
	now := cfg.Now
	if now == nil {
		now = time.Now
	}

	var leftover string
	var w io.WriteCloser
	if cfg.RotateHourly {
		var err error
		if leftover, err = segment.Adopt(filename); err != nil {
			return nil, fmt.Errorf("rotating the previous query log: %w", err)
		}
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("opening query log: %w", err)
		}
		w = f
	} else {
		w = &lumberjack.Logger{
			Filename:   filename,
			MaxSize:    cfg.MaxSizeMB,
			MaxAge:     cfg.MaxAgeDays,
			MaxBackups: cfg.MaxBackups,
			Compress:   cfg.Compress,
			LocalTime:  true,
		}
	}

	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = 10000
	}
	l := &Logger{
		entryCh: make(chan LogEntry, queueSize),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		writer:  w,

		path:      filename,
		rotate:    cfg.RotateHourly,
		rotateMin: cfg.RotateMinSize,
		onSegment: cfg.OnSegment,
		now:       now,
		segStart:  now(),
		rotateReq: make(chan chan struct{}),
	}
	l.enabled.Store(cfg.Enabled)
	l.errorsOnly.Store(cfg.Level == "error")
	if leftover != "" {
		if st, err := os.Stat(leftover); err == nil {
			start, _ := segment.Start(filename, leftover)
			l.notify(segment.Event{Path: leftover, Size: st.Size(), Start: start, Closed: true})
		}
	}

	go l.writeLoop()

	return l, nil
}

func (l *Logger) Log(entry LogEntry) {
	if l.closed.Load() || !l.enabled.Load() {
		return
	}

	// Level=error: skip entries from queries that didn't error, before
	// they ever reach the channel. Cuts volume during a noisy incident
	// while keeping every failure.
	if l.errorsOnly.Load() && entry.Error == "" {
		return
	}

	// Non-blocking send: if the writer goroutine has already exited or the
	// buffer is full, drop the entry rather than blocking the caller or
	// risking a deadlock during shutdown.
	select {
	case l.entryCh <- entry:
	case <-l.stop:
		l.dropped.Add(1)
		metrics.Global.LoggerDropped.Add(1)
	default:
		l.dropped.Add(1)
		metrics.Global.LoggerDropped.Add(1)
	}
}

func (l *Logger) SetEnabled(enabled bool) {
	l.enabled.Store(enabled)
	slog.Info("sql logging toggled", "enabled", enabled)
}

// SetLevel changes which entries Log records: "error" keeps only entries
// whose query returned a backend error, "all" (or any other value) keeps
// every entry. Hot-reloadable, same as SetEnabled.
func (l *Logger) SetLevel(level string) {
	errorsOnly := level == "error"
	l.errorsOnly.Store(errorsOnly)
	slog.Info("sql logging level changed", "level", level, "errors_only", errorsOnly)
}

func (l *Logger) Dropped() int64 {
	return l.dropped.Load()
}

func (l *Logger) Close() {
	l.once.Do(func() {
		l.closed.Store(true)
		close(l.stop)
		<-l.done
		if err := l.writer.Close(); err != nil {
			slog.Error("failed to close query log", "file", l.path, "err", err)
		}
		if l.rotate && !l.broken {
			// The last file always becomes a segment, however small, so
			// it is shipped with the others before the process exits.
			if st, err := os.Stat(l.path); err == nil && st.Size() > 0 {
				start := l.segStart
				seg, err := l.closeSegment()
				if err != nil {
					slog.Error("query log rotation at close failed", "file", l.path, "err", err)
				} else {
					l.notify(segment.Event{Path: seg, Size: st.Size(), Start: start, Closed: true})
				}
			}
		}
	})
}

func (l *Logger) notify(ev segment.Event) {
	if ev.Closed {
		slog.Info("query log rotated", "segment", ev.Path, "bytes", ev.Size)
	}
	if l.onSegment != nil {
		l.onSegment(ev)
	}
}

// untilNextHour is the wait from t to the next full UTC hour.
func untilNextHour(t time.Time) time.Duration {
	return t.UTC().Truncate(time.Hour).Add(time.Hour).Sub(t)
}

// rotateAtHour runs on the write loop at each UTC hour. It closes the
// current file as a segment unless it is empty or smaller than
// rotateMin, in which case the file is written on through the next hour
// and reported as a not-yet-closed snapshot. Entries are encoded straight
// to the file, so everything written so far is complete records.
func (l *Logger) rotateAtHour() {
	if l.broken {
		l.reopen()
		return
	}
	st, err := os.Stat(l.path)
	if err != nil {
		slog.Error("query log rotation skipped", "file", l.path, "err", err)
		return
	}
	if st.Size() == 0 {
		return
	}
	if st.Size() < l.rotateMin {
		l.notify(segment.Event{Path: l.path, Size: st.Size(), Start: l.segStart})
		return
	}
	if err := l.writer.Close(); err != nil {
		slog.Error("failed to close query log", "file", l.path, "err", err)
	}
	start := l.segStart
	seg, err := l.closeSegment()
	l.reopen()
	if err != nil {
		slog.Error("query log rotation failed", "file", l.path, "err", err)
		return
	}
	l.notify(segment.Event{Path: seg, Size: st.Size(), Start: start, Closed: true})
}

// closeSegment renames the (closed) current file to its segment name.
func (l *Logger) closeSegment() (string, error) {
	seg := segment.Name(l.path, l.segStart)
	if err := os.Rename(l.path, seg); err != nil {
		return "", err
	}
	return seg, nil
}

// reopen opens a fresh file at path. On failure entries go to io.Discard
// until the next hour retries; the error is logged.
func (l *Logger) reopen() {
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		slog.Error("failed to reopen query log; dropping entries until the next rotation", "file", l.path, "err", err)
		l.writer = nopWriteCloser{io.Discard}
		l.broken = true
		return
	}
	l.writer = f
	l.segStart = l.now()
	l.broken = false
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

// rotateNow runs an hourly rotation now and waits for it; for tests.
func (l *Logger) rotateNow() {
	done := make(chan struct{})
	l.rotateReq <- done
	<-done
}

func (l *Logger) writeLoop() {
	defer close(l.done)

	var enc *json.Encoder
	newEncoder := func() {
		enc = json.NewEncoder(l.writer)
		enc.SetEscapeHTML(false)
	}
	newEncoder()
	write := func(entry LogEntry) {
		if !l.enabled.Load() {
			return
		}
		if err := enc.Encode(entry); err != nil {
			slog.Error("failed to write sql log entry", "err", err)
		}
	}

	var rotateC <-chan time.Time
	var rotateTimer *time.Timer
	if l.rotate {
		rotateTimer = time.NewTimer(untilNextHour(l.now()))
		defer rotateTimer.Stop()
		rotateC = rotateTimer.C
	}

	for {
		select {
		case entry := <-l.entryCh:
			write(entry)
		case <-rotateC:
			l.rotateAtHour()
			newEncoder()
			rotateTimer.Reset(untilNextHour(l.now()))
		case done := <-l.rotateReq:
			// Tests trigger a rotation without waiting for the hour.
			// Write what is already queued first, so a test sees the
			// entries it logged before asking.
			for drained := false; !drained; {
				select {
				case entry := <-l.entryCh:
					write(entry)
				default:
					drained = true
				}
			}
			if l.rotate {
				l.rotateAtHour()
				newEncoder()
			}
			close(done)
		case <-l.stop:
			// Drain any remaining buffered entries, then exit.
			for {
				select {
				case entry := <-l.entryCh:
					write(entry)
				default:
					return
				}
			}
		}
	}
}
