package scheduler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
)

// Definition is one schedule as declared in the cron file. It is the
// configuration shape; store.CronScheduleSpec is the storage shape, and
// reconcile converts between them.
type Definition struct {
	Name        string          `json:"name"`
	Queue       string          `json:"queue"`
	Cron        string          `json:"cron"`
	Timezone    string          `json:"timezone"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority"`
	MaxAttempts int             `json:"max_attempts"`
	Enabled     *bool           `json:"enabled"` // absent means true
}

// cronFile is the document LoadFile reads.
type cronFile struct {
	Schedules []Definition `json:"schedules"`
}

// LoadFile reads and fully validates a cron file. Every rejection happens
// here, at process start, so a malformed schedule set cannot reach a leader.
// Defaults are applied to the returned definitions: an absent timezone is
// UTC, an absent payload is {}, and an absent enabled is true.
func LoadFile(path string) ([]Definition, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("scheduler: cron file: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	// A typo in a field name is an error, not a silently defaulted field.
	dec.DisallowUnknownFields()
	var doc cronFile
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("scheduler: cron file %s: parse: %w", path, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("scheduler: cron file %s: parse: trailing data after the document", path)
	}
	// An empty set is rejected rather than read as "prune everything": a
	// file that lists nothing is far more likely a mistake than an intent.
	if len(doc.Schedules) == 0 {
		return nil, fmt.Errorf("scheduler: cron file %s: no schedules declared", path)
	}

	seen := make(map[string]bool, len(doc.Schedules))
	for i := range doc.Schedules {
		d := &doc.Schedules[i]
		if err := d.validate(); err != nil {
			return nil, fmt.Errorf("scheduler: cron file %s: schedule %d: %w", path, i, err)
		}
		if seen[d.Name] {
			return nil, fmt.Errorf("scheduler: cron file %s: schedule %q declared twice", path, d.Name)
		}
		seen[d.Name] = true
	}
	return doc.Schedules, nil
}

// validate checks one definition and applies its defaults in place. Every
// message names the schedule, so an operator reading a crash-looping pod's
// log can find the line to fix.
func (d *Definition) validate() error {
	if d.Name == "" {
		return errors.New("name is required")
	}
	if d.Queue == "" {
		return fmt.Errorf("schedule %q: queue is required", d.Name)
	}
	if d.Timezone == "" {
		d.Timezone = "UTC"
	}
	// ParseSpec is the tick path's parser too, so the file and the scheduler
	// can never disagree about what parses.
	if _, err := ParseSpec(d.Cron, d.Timezone); err != nil {
		return fmt.Errorf("schedule %q: %w", d.Name, err)
	}
	// Range-checked the way the broker checks priority off the gRPC wire.
	if d.Priority > math.MaxInt16 || d.Priority < math.MinInt16 {
		return fmt.Errorf("schedule %q: priority %d is out of range (%d..%d)",
			d.Name, d.Priority, math.MinInt16, math.MaxInt16)
	}
	// Zero defers to the store's default dead-letter threshold.
	if d.MaxAttempts < 0 || d.MaxAttempts > math.MaxInt32 {
		return fmt.Errorf("schedule %q: max_attempts %d is out of range (0..%d)",
			d.Name, d.MaxAttempts, math.MaxInt32)
	}
	// The decoder has already proved the payload is syntactically JSON, so
	// the remaining mistake is a quoted object: "{\"ms\":50}" decodes as a
	// string, and every job would carry that string instead of the object.
	// Require an object, which is what every hopper handler decodes.
	switch p := bytes.TrimSpace(d.Payload); {
	case len(p) == 0 || bytes.Equal(p, []byte("null")):
		d.Payload = json.RawMessage(`{}`)
	case p[0] != '{':
		return fmt.Errorf("schedule %q: payload must be a JSON object", d.Name)
	}
	if d.Enabled == nil {
		on := true
		d.Enabled = &on
	}
	return nil
}

// enabled reports the definition's enabled flag, treating absent as true for
// definitions constructed in code rather than loaded by LoadFile.
func (d Definition) enabled() bool {
	return d.Enabled == nil || *d.Enabled
}
