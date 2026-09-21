package worker

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func noop(context.Context, Job) error { return nil }

// cancelled returns a context that is already done, so a test that expects Run
// to reject its configuration fails loudly instead of blocking in consume.
func cancelled(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	t.Cleanup(func() { <-ctx.Done() })
	return ctx
}

func TestDefaultsApplyToZeroValuesOnly(t *testing.T) {
	w := New("127.0.0.1:9090", Options{})
	require.Equal(t, defaultConcurrency, w.opts.Concurrency)
	require.Equal(t, defaultShutdownTimeout, w.opts.ShutdownTimeout)
	require.NotEmpty(t, w.opts.ID)
	require.NotNil(t, w.opts.Logger)
	require.Zero(t, w.opts.LeaseDuration, "zero lease duration asks the broker to choose")

	set := New("127.0.0.1:9090", Options{
		ID: "w-1", Concurrency: 3, LeaseDuration: time.Minute, ShutdownTimeout: time.Second,
	})
	require.Equal(t, "w-1", set.opts.ID)
	require.Equal(t, 3, set.opts.Concurrency)
	require.Equal(t, time.Minute, set.opts.LeaseDuration)
	require.Equal(t, time.Second, set.opts.ShutdownTimeout)

	// A negative is not a zero, so it is not defaulted away; Run rejects it.
	neg := New("127.0.0.1:9090", Options{Concurrency: -1})
	require.Equal(t, -1, neg.opts.Concurrency)
}

func TestRunRejectsConfigurationBeforeDialing(t *testing.T) {
	tests := []struct {
		name     string
		target   string
		opts     Options
		register bool
		want     string
	}{
		{"empty target", "", Options{}, true, "target is required"},
		{"negative concurrency", "a:1", Options{Concurrency: -1}, true, "must not be negative"},
		{"oversized concurrency", "a:1", Options{Concurrency: maxConcurrency + 1}, true, "exceeds the maximum"},
		{"negative lease duration", "a:1", Options{LeaseDuration: -time.Second}, true, "lease duration"},
		{"negative shutdown timeout", "a:1", Options{ShutdownTimeout: -time.Second}, true, "shutdown timeout"},
		{"no handlers", "a:1", Options{}, false, ErrNoHandlers.Error()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := New(tc.target, tc.opts)
			if tc.register {
				w.Handle("q", noop)
			}
			err := w.Run(cancelled(t))
			require.ErrorContains(t, err, tc.want)
		})
	}

	// The maximum itself is allowed; only above it is rejected.
	w := New("a:1", Options{Concurrency: maxConcurrency})
	w.Handle("q", noop)
	require.NoError(t, w.validate())
}

func TestRunIsSingleUse(t *testing.T) {
	w := New("a:1", Options{})
	w.Handle("q", noop)

	require.NoError(t, w.Run(cancelled(t)))
	require.ErrorContains(t, w.Run(cancelled(t)), "already been called")
}

func TestHandleRejectsMisuse(t *testing.T) {
	t.Run("empty queue", func(t *testing.T) {
		require.PanicsWithValue(t, "worker: Handle called with an empty queue", func() {
			New("a:1", Options{}).Handle("", noop)
		})
	})
	t.Run("nil handler", func(t *testing.T) {
		require.PanicsWithValue(t, `worker: Handle called with a nil handler for queue "q"`, func() {
			New("a:1", Options{}).Handle("q", nil)
		})
	})
	t.Run("duplicate queue", func(t *testing.T) {
		require.PanicsWithValue(t, `worker: queue "q" already has a handler`, func() {
			New("a:1", Options{}).Handle("q", noop).Handle("q", noop)
		})
	})
	t.Run("after Run", func(t *testing.T) {
		w := New("a:1", Options{})
		w.Handle("q", noop)
		require.NoError(t, w.Run(cancelled(t)))
		require.PanicsWithValue(t, "worker: Handle called after Run; registration is setup-only", func() {
			w.Handle("late", noop)
		})
	})
}

func TestQueuesAreSorted(t *testing.T) {
	w := New("a:1", Options{})
	w.Handle("zeta", noop).Handle("alpha", noop).Handle("mu", noop)
	require.Equal(t, []string{"alpha", "mu", "zeta"}, w.queues())
}

func TestJobUnmarshal(t *testing.T) {
	id := uuid.New()

	var got struct {
		MS int `json:"ms"`
	}
	require.NoError(t, Job{ID: id, Payload: []byte(`{"ms":250}`)}.Unmarshal(&got))
	require.Equal(t, 250, got.MS)

	require.ErrorContains(t, Job{ID: id}.Unmarshal(&got), "empty payload")
	err := Job{ID: id, Payload: []byte(`{`)}.Unmarshal(&got)
	require.ErrorContains(t, err, id.String(), "the message names the job so a log line is actionable")
}

func TestBackoffGrowsJittersAndCaps(t *testing.T) {
	low := &backoff{jitter: func() float64 { return 0 }}
	want := []time.Duration{75, 150, 300, 600, 1200, 2400, 3750, 3750}
	for i, ms := range want {
		require.Equal(t, ms*time.Millisecond, low.next(), "attempt %d at the jitter floor", i)
	}

	// The highest value the sequence can produce is 1.25 × the nominal cap.
	high := &backoff{jitter: func() float64 { return 1 - 1e-12 }}
	var peak time.Duration
	for range 20 {
		peak = max(peak, high.next())
	}
	require.LessOrEqual(t, peak, 6250*time.Millisecond, "5s is the cap on the nominal delay, not on the jittered one")
	require.Greater(t, peak, 6249*time.Millisecond)

	// And with the real source, every draw stays inside the band.
	b := newBackoff()
	for range 1000 {
		d := b.next()
		require.GreaterOrEqual(t, d, 75*time.Millisecond)
		require.LessOrEqual(t, d, 6250*time.Millisecond)
	}

	b.reset()
	b.jitter = func() float64 { return 0 }
	require.Equal(t, 75*time.Millisecond, b.next(), "reset restarts the sequence")
}

func TestSanitizeError(t *testing.T) {
	t.Run("removes NUL and fixes invalid UTF-8", func(t *testing.T) {
		require.Equal(t, "ab", sanitizeError("a\x00b"))
		require.Equal(t, "a�b", sanitizeError("a\xffb"))
		require.Equal(t, "ok é", sanitizeError("ok \x00é"))
	})

	t.Run("leaves a message at the limit alone", func(t *testing.T) {
		at := strings.Repeat("x", maxErrorBytes)
		require.Equal(t, at, sanitizeError(at))
		require.Len(t, sanitizeError(at), maxErrorBytes)
	})

	t.Run("truncates one byte over the limit", func(t *testing.T) {
		got := sanitizeError(strings.Repeat("x", maxErrorBytes+1))
		require.LessOrEqual(t, len(got), maxErrorBytes)
		require.True(t, strings.HasSuffix(got, truncationSuffix))
		require.Len(t, got, maxErrorBytes, "the suffix is counted inside the budget, not added to it")
	})

	t.Run("cuts at a rune boundary", func(t *testing.T) {
		// Two-byte runes make the cut point land mid-rune: the budget before
		// the suffix is 4081 bytes, which is an odd number of bytes into a
		// string built from 2-byte runes.
		got := sanitizeError(strings.Repeat("é", maxErrorBytes))
		require.True(t, utf8.ValidString(got), "a cut inside a rune would be rejected by Postgres")
		require.LessOrEqual(t, len(got), maxErrorBytes)
		require.True(t, strings.HasSuffix(got, truncationSuffix))

		// Same for a 4-byte rune, where three of the four alignments are bad.
		for pad := range 4 {
			s := strings.Repeat("a", pad) + strings.Repeat("\U0001F600", maxErrorBytes)
			out := sanitizeError(s)
			require.True(t, utf8.ValidString(out), "pad %d", pad)
			require.LessOrEqual(t, len(out), maxErrorBytes, "pad %d", pad)
		}
	})

	t.Run("sanitizes before measuring", func(t *testing.T) {
		// Invalid bytes become 3-byte replacement characters, so a string that
		// was under the limit can cross it during sanitizing and must still
		// come out valid and bounded.
		got := sanitizeError(strings.Repeat("\xff", maxErrorBytes-1))
		require.True(t, utf8.ValidString(got))
		require.LessOrEqual(t, len(got), maxErrorBytes)
		require.NotContains(t, got, "\x00")
	})
}
