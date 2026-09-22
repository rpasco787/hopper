package scheduler

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeCronFile writes body to a fresh file under t.TempDir and returns its
// path.
func writeCronFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "schedules.json")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func TestLoadFileAcceptsAWellFormedDocument(t *testing.T) {
	path := writeCronFile(t, `{
	  "schedules": [
	    {
	      "name": "nightly-report",
	      "queue": "report",
	      "cron": "0 3 * * *",
	      "timezone": "America/New_York",
	      "payload": {"scope": "daily"},
	      "priority": 5,
	      "max_attempts": 3,
	      "enabled": true
	    },
	    {"name": "heartbeat", "queue": "sleep", "cron": "@every 10s"}
	  ]
	}`)

	defs, err := LoadFile(path)
	require.NoError(t, err)
	require.Len(t, defs, 2)

	d := defs[0]
	require.Equal(t, "nightly-report", d.Name)
	require.Equal(t, "report", d.Queue)
	require.Equal(t, "0 3 * * *", d.Cron)
	require.Equal(t, "America/New_York", d.Timezone)
	require.JSONEq(t, `{"scope":"daily"}`, string(d.Payload))
	require.Equal(t, 5, d.Priority)
	require.Equal(t, 3, d.MaxAttempts)
	require.NotNil(t, d.Enabled)
	require.True(t, *d.Enabled)

	// Defaults for everything the second schedule leaves out.
	h := defs[1]
	require.Equal(t, "UTC", h.Timezone)
	require.JSONEq(t, `{}`, string(h.Payload))
	require.NotNil(t, h.Enabled)
	require.True(t, *h.Enabled, "an absent enabled means true")
	require.Zero(t, h.Priority)
	require.Zero(t, h.MaxAttempts, "zero defers to the store's default")
}

func TestLoadFileRejectsBadDocuments(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
	}{
		{"invalid json", `{"schedules": [`, "parse"},
		{"unknown top-level key", `{"schedules": [{"name":"a","queue":"q","cron":"@hourly"}], "extra": 1}`, "extra"},
		{"unknown schedule key", `{"schedules": [{"name":"a","queue":"q","cron":"@hourly","prio":1}]}`, "prio"},
		{"empty schedules", `{"schedules": []}`, "no schedules"},
		{"missing schedules", `{}`, "no schedules"},
		{"duplicate name", `{"schedules": [
			{"name":"a","queue":"q","cron":"@hourly"},
			{"name":"a","queue":"q","cron":"@daily"}]}`, `"a"`},
		{"empty name", `{"schedules": [{"name":"","queue":"q","cron":"@hourly"}]}`, "name is required"},
		{"empty queue", `{"schedules": [{"name":"noqueue","queue":"","cron":"@hourly"}]}`, `"noqueue"`},
		{"bad cron", `{"schedules": [{"name":"badcron","queue":"q","cron":"not a cron"}]}`, `"badcron"`},
		{"bad timezone", `{"schedules": [{"name":"badtz","queue":"q","cron":"@hourly","timezone":"Mars/Olympus"}]}`, `"badtz"`},
		{"priority too high", `{"schedules": [{"name":"hot","queue":"q","cron":"@hourly","priority":32768}]}`, `"hot"`},
		{"priority too low", `{"schedules": [{"name":"cold","queue":"q","cron":"@hourly","priority":-32769}]}`, `"cold"`},
		{"negative max_attempts", `{"schedules": [{"name":"neg","queue":"q","cron":"@hourly","max_attempts":-1}]}`, `"neg"`},
		{"payload not json", `{"schedules": [{"name":"pay","queue":"q","cron":"@hourly","payload":"{not json"}]}`, `"pay"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadFile(writeCronFile(t, tc.body))
			require.Error(t, err)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestLoadFileRejectsAMissingFile(t *testing.T) {
	_, err := LoadFile(filepath.Join(t.TempDir(), "absent.json"))
	require.Error(t, err)
	require.ErrorIs(t, err, os.ErrNotExist)
}

// threeDefinitions is a small valid schedule set for reconciliation tests.
func threeDefinitions() []Definition {
	off := false
	return []Definition{
		{Name: "a", Queue: "qa", Cron: "*/5 * * * *", Timezone: "UTC", Payload: json.RawMessage(`{}`)},
		{Name: "b", Queue: "qb", Cron: "0 3 * * *", Timezone: "America/New_York",
			Payload: json.RawMessage(`{"k":1}`), Priority: 4, MaxAttempts: 2},
		{Name: "c", Queue: "qc", Cron: "@every 1s", Timezone: "UTC", Payload: json.RawMessage(`{}`), Enabled: &off},
	}
}

// reconcileOnce runs one term that reconciles and then loses leadership at
// its first Check, and waits for the term to release.
func reconcileOnce(t *testing.T, f *fakeStore, defs []Definition) {
	t.Helper()
	f.termLimit = 1
	f.checkErr = errors.New("stop after reconcile")
	s := newTestScheduler(f, nil)
	s.Definitions = defs
	r := runScheduler(t, s)
	requireEventually(t, func() bool { return f.releaseCount() >= 1 }, "expected the term to end")
	r.stopAndWait(t)
}

func TestReconcileUpsertsEveryDefinition(t *testing.T) {
	f := newFakeStore()
	dbNow := time.Date(2026, 9, 22, 12, 0, 0, 437_000_000, time.UTC)
	f.now = dbNow

	defs := threeDefinitions()
	reconcileOnce(t, f, defs)

	ups := f.upsertsSnapshot()
	require.Len(t, ups, 3)
	for i, d := range defs {
		spec, err := ParseSpec(d.Cron, d.Timezone)
		require.NoError(t, err)
		want, err := spec.Next(dbNow)
		require.NoError(t, err)
		require.Equal(t, d.Name, ups[i].Name)
		require.Equal(t, d.Queue, ups[i].Queue)
		require.Equal(t, d.Cron, ups[i].CronExpr)
		require.Equal(t, d.Timezone, ups[i].Timezone)
		require.True(t, ups[i].NextRunAt.Equal(want), "%s: NextRunAt %s, want %s", d.Name, ups[i].NextRunAt, want)
	}
	require.Equal(t, int16(4), ups[1].Priority)
	require.Equal(t, int32(2), ups[1].MaxAttempts)
	require.JSONEq(t, `{"k":1}`, string(ups[1].Payload))
	require.True(t, ups[0].Enabled, "absent enabled reconciles as enabled")
	require.False(t, ups[2].Enabled)

	require.Equal(t, [][]string{{"a", "b", "c"}}, f.deletesSnapshot())
	require.Equal(t, 1, f.nowCallCount(), "one clock read judges the whole reconciliation")
}

func TestReconcileIgnoresTheProcessClock(t *testing.T) {
	for _, skew := range []time.Duration{time.Hour, -time.Hour} {
		t.Run(skew.String(), func(t *testing.T) {
			f := newFakeStore()
			dbNow := time.Now().Add(skew).UTC().Truncate(time.Second)
			f.now = dbNow

			defs := threeDefinitions()
			reconcileOnce(t, f, defs)

			ups := f.upsertsSnapshot()
			require.Len(t, ups, 3)
			for i, d := range defs {
				spec, err := ParseSpec(d.Cron, d.Timezone)
				require.NoError(t, err)
				want, err := spec.Next(dbNow)
				require.NoError(t, err)
				require.True(t, ups[i].NextRunAt.Equal(want),
					"%s: NextRunAt %s must follow the database clock %s, not the process clock", d.Name, ups[i].NextRunAt, dbNow)
			}
		})
	}
}

func TestReconcileFailsWhenTheClockIsUnavailable(t *testing.T) {
	f := newFakeStore()
	f.nowErr = errors.New("clock down")
	f.termLimit = 1

	buf := &syncBuffer{}
	s := newTestScheduler(f, buf)
	s.Definitions = threeDefinitions()
	r := runScheduler(t, s)

	requireEventually(t, func() bool { return f.releaseCount() >= 1 }, "expected the term to end")
	r.stopAndWait(t)

	require.Empty(t, f.upsertsSnapshot(), "no upsert may be computed without the database clock")
	require.Empty(t, f.deletesSnapshot())
	require.Empty(t, f.dueCallsSnapshot(), "the term must end before any pass")
	require.Contains(t, buf.String(), "clock down")
}

func TestReconcilePrunesRemovedSchedules(t *testing.T) {
	f := newFakeStore()
	f.existing["a"] = true
	f.existing["retired"] = true

	reconcileOnce(t, f, threeDefinitions())

	require.Equal(t, [][]string{{"a", "b", "c"}}, f.deletesSnapshot())
	require.Equal(t, map[string]bool{"a": true, "b": true, "c": true}, f.existingSnapshot(),
		"the row absent from the file must be pruned")
}

func TestReconcileIsSkippedWithoutDefinitions(t *testing.T) {
	f := newFakeStore()
	f.existing["hand-managed"] = true

	reconcileOnce(t, f, nil)

	require.Empty(t, f.upsertsSnapshot())
	require.Empty(t, f.deletesSnapshot(), "no file means no prune")
	require.Zero(t, f.nowCallCount())
	require.True(t, f.existingSnapshot()["hand-managed"], "a hand-managed table survives a broker with no file")
}

func TestReconcileFailureEndsTheTermBeforeFiring(t *testing.T) {
	f := newFakeStore()
	f.upsertErr = errors.New("upsert boom")
	f.termLimit = 1

	s := newTestScheduler(f, nil)
	s.Definitions = threeDefinitions()
	r := runScheduler(t, s)

	requireEventually(t, func() bool { return f.releaseCount() >= 1 }, "expected the term to end")
	r.stopAndWait(t)

	require.Len(t, f.upsertsSnapshot(), 1, "reconciliation stops at the first failed upsert")
	require.Empty(t, f.deletesSnapshot(), "a failed upsert must not be followed by a prune")
	require.Empty(t, f.dueCallsSnapshot(), "a half-applied configuration never fires")
}
