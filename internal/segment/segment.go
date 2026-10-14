// Package segment names and finds the closed segments of an hourly
// rotated file (the comparison report or the query log):
// <path>-<UTC start timestamp>, e.g. diff-report.jsonl-20261009T090000Z.
// The timestamp is when the segment started, so names sort
// chronologically.
package segment

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// timeLayout is the UTC timestamp suffix of a segment name.
const timeLayout = "20060102T150405Z"

// Name returns the segment name for the file at path started at start.
func Name(path string, start time.Time) string {
	return path + "-" + start.UTC().Format(timeLayout)
}

// Start parses the start time out of segment, a segment of the file at
// path. ok is false when segment is not one.
func Start(path, segment string) (time.Time, bool) {
	rest, found := strings.CutPrefix(segment, path+"-")
	if !found {
		return time.Time{}, false
	}
	t, err := time.Parse(timeLayout, rest)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// List returns the segments of the file at path, oldest first. A
// missing directory is not an error.
func List(path string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Dir(path))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		p := filepath.Join(filepath.Dir(path), e.Name())
		if _, ok := Start(path, p); ok {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out, nil
}

// Event describes the data an hourly rotation tick (or start-up / close)
// has settled: the first Size bytes of Path, which hold complete records
// only (the writer flushed before measuring).
type Event struct {
	// Path is the file to read: a closed segment when Closed, otherwise
	// the live file, which keeps growing past Size.
	Path string
	Size int64
	// Start is when the data began; it names the segment.
	Start time.Time
	// Closed is true once Path is a closed segment that will not change.
	Closed bool
}

// Adopt renames a non-empty file left at path by a previous process to a
// segment named after its last modification, so this process starts a
// fresh file and the old content is handled like any segment. It returns
// the segment, or "" when there was nothing to adopt.
func Adopt(path string) (string, error) {
	st, err := os.Stat(path)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if st.Size() == 0 {
		return "", nil
	}
	seg := Name(path, st.ModTime())
	if err := os.Rename(path, seg); err != nil {
		return "", err
	}
	return seg, nil
}
