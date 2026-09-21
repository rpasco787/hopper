package broker

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
)

// keysetStore is a fake ListJobs with real keyset semantics over a mutable set
// of jobs, so traversal tests exercise the cursor rather than a canned script.
type keysetStore struct {
	mu   sync.Mutex
	jobs []store.Job // kept sorted by ID
}

func newKeysetStore(n int) *keysetStore {
	k := &keysetStore{}
	for range n {
		k.jobs = append(k.jobs, sampleJob("q"))
	}
	slices.SortFunc(k.jobs, func(a, b store.Job) int { return bytes.Compare(a.ID[:], b.ID[:]) })
	return k
}

func (k *keysetStore) remove(id uuid.UUID) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.jobs = slices.DeleteFunc(k.jobs, func(j store.Job) bool { return j.ID == id })
}

func (k *keysetStore) list(_ context.Context, _ string, _ []store.State, limit int, after *uuid.UUID) ([]store.Job, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	var out []store.Job
	for _, j := range k.jobs {
		if after != nil && bytes.Compare(j.ID[:], after[:]) <= 0 {
			continue
		}
		if len(out) == limit {
			break
		}
		out = append(out, j)
	}
	return out, nil
}

func TestListJobsPassesTheRequestToTheStore(t *testing.T) {
	t.Parallel()

	var (
		gotQueue  string
		gotStates []store.State
		gotAfter  *uuid.UUID
	)
	client, _ := newTestService(t, &fakeStore{
		listJobsFn: func(_ context.Context, q string, st []store.State, _ int, after *uuid.UUID) ([]store.Job, error) {
			gotQueue, gotStates, gotAfter = q, st, after
			return nil, nil
		},
	})

	cursor := uuid.New()
	_, err := client.ListJobs(ctxT(t), &hopperv1.ListJobsRequest{
		Queue:     "orders",
		States:    []hopperv1.JobState{hopperv1.JobState_JOB_STATE_LEASED, hopperv1.JobState_JOB_STATE_DEAD},
		PageToken: cursor.String(),
	})
	require.NoError(t, err)
	require.Equal(t, "orders", gotQueue)
	require.Equal(t, []store.State{store.StateLeased, store.StateDead}, gotStates)
	require.NotNil(t, gotAfter)
	require.Equal(t, cursor, *gotAfter)
}

func TestListJobsWithoutTokenOrStatesAsksForEverythingFromTheStart(t *testing.T) {
	t.Parallel()

	var gotAfter *uuid.UUID
	var gotStates []store.State
	client, _ := newTestService(t, &fakeStore{
		listJobsFn: func(_ context.Context, _ string, st []store.State, _ int, after *uuid.UUID) ([]store.Job, error) {
			gotAfter, gotStates = after, st
			return nil, nil
		},
	})
	_, err := client.ListJobs(ctxT(t), &hopperv1.ListJobsRequest{Queue: "q"})
	require.NoError(t, err)
	require.Nil(t, gotAfter)
	require.Empty(t, gotStates)
}

func TestListJobsLimitDefaultsAndClamps(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ asked, want int32 }{
		{0, DefaultListLimit},
		{7, 7},
		{MaxListLimit, MaxListLimit},
		{MaxListLimit + 1, MaxListLimit},
		{1 << 30, MaxListLimit},
	} {
		var got atomic.Int32
		client, _ := newTestService(t, &fakeStore{
			listJobsFn: func(_ context.Context, _ string, _ []store.State, limit int, _ *uuid.UUID) ([]store.Job, error) {
				got.Store(int32(limit))
				return nil, nil
			},
		})
		_, err := client.ListJobs(ctxT(t), &hopperv1.ListJobsRequest{Queue: "q", Limit: tc.asked})
		require.NoError(t, err)
		require.Equal(t, tc.want, got.Load(), "limit %d", tc.asked)
	}
}

// TestListJobsNextPageToken: a full page hands back the last id as the cursor;
// a short page, including an empty one, ends the traversal.
func TestListJobsNextPageToken(t *testing.T) {
	t.Parallel()

	ks := newKeysetStore(5)
	client, _ := newTestService(t, &fakeStore{listJobsFn: ks.list})

	full, err := client.ListJobs(ctxT(t), &hopperv1.ListJobsRequest{Queue: "q", Limit: 2})
	require.NoError(t, err)
	require.Len(t, full.GetJobs(), 2)
	require.Equal(t, full.GetJobs()[1].GetId(), full.GetNextPageToken())

	short, err := client.ListJobs(ctxT(t), &hopperv1.ListJobsRequest{Queue: "q", Limit: 10})
	require.NoError(t, err)
	require.Len(t, short.GetJobs(), 5)
	require.Empty(t, short.GetNextPageToken(), "a short page ends the traversal")

	empty, err := client.ListJobs(ctxT(t), &hopperv1.ListJobsRequest{Queue: "q", Limit: 10, PageToken: ks.jobs[4].ID.String()})
	require.NoError(t, err)
	require.Empty(t, empty.GetJobs())
	require.Empty(t, empty.GetNextPageToken())
}

// TestListJobsTraversalCoversEveryRow threads the token through the RPC until
// it comes back empty. An exact multiple of the page size yields one final
// empty page, as the proto comment says it may.
func TestListJobsTraversalCoversEveryRow(t *testing.T) {
	t.Parallel()

	for _, n := range []int{0, 1, 4, 5, 6} {
		ks := newKeysetStore(n)
		client, _ := newTestService(t, &fakeStore{listJobsFn: ks.list})

		var got []string
		token, pages := "", 0
		for {
			resp, err := client.ListJobs(ctxT(t), &hopperv1.ListJobsRequest{Queue: "q", Limit: 2, PageToken: token})
			require.NoError(t, err)
			pages++
			for _, j := range resp.GetJobs() {
				got = append(got, j.GetId())
			}
			if token = resp.GetNextPageToken(); token == "" {
				break
			}
		}
		var want []string
		for _, j := range ks.jobs {
			want = append(want, j.ID.String())
		}
		require.Equal(t, want, got, "n=%d: every row once, in ID order", n)
		require.LessOrEqual(t, pages, n/2+2, "n=%d: bounded pages", n)
	}
}

// TestListJobsSurvivesDeletionBetweenPages: the row the token names is deleted
// after page one. The cursor is data, not server state, so page two carries on
// with the rows after it and nothing is skipped.
func TestListJobsSurvivesDeletionBetweenPages(t *testing.T) {
	t.Parallel()

	ks := newKeysetStore(5)
	client, _ := newTestService(t, &fakeStore{listJobsFn: ks.list})

	first, err := client.ListJobs(ctxT(t), &hopperv1.ListJobsRequest{Queue: "q", Limit: 2})
	require.NoError(t, err)
	cursor := first.GetNextPageToken()
	require.NotEmpty(t, cursor)
	ks.remove(uuid.MustParse(cursor))

	rest, err := client.ListJobs(ctxT(t), &hopperv1.ListJobsRequest{Queue: "q", Limit: 10, PageToken: cursor})
	require.NoError(t, err)
	require.Len(t, rest.GetJobs(), 3, "the three rows after the deleted cursor")
}

// TestListJobsInvalidRequestsNeverReachTheStore: every one of these is the
// caller's mistake and must be reported as such, not as an Internal from a
// query that was never going to make sense.
func TestListJobsInvalidRequestsNeverReachTheStore(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	client, _ := newTestService(t, &fakeStore{
		listJobsFn: func(context.Context, string, []store.State, int, *uuid.UUID) ([]store.Job, error) {
			calls.Add(1)
			return nil, nil
		},
	})

	for _, tc := range []struct {
		name string
		req  *hopperv1.ListJobsRequest
	}{
		{"no queue", &hopperv1.ListJobsRequest{}},
		{"negative limit", &hopperv1.ListJobsRequest{Queue: "q", Limit: -1}},
		{"malformed page token", &hopperv1.ListJobsRequest{Queue: "q", PageToken: "not-a-uuid"}},
		{"unspecified state", &hopperv1.ListJobsRequest{Queue: "q", States: []hopperv1.JobState{hopperv1.JobState_JOB_STATE_UNSPECIFIED}}},
		{"unknown state", &hopperv1.ListJobsRequest{Queue: "q", States: []hopperv1.JobState{99}}},
		{"one bad state among good ones", &hopperv1.ListJobsRequest{Queue: "q", States: []hopperv1.JobState{hopperv1.JobState_JOB_STATE_DEAD, 99}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.ListJobs(ctxT(t), tc.req)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
	require.Zero(t, calls.Load())
}

func TestListJobsStoreFailureIsInternalAndSaysNothingElse(t *testing.T) {
	t.Parallel()

	client, _ := newTestService(t, &fakeStore{
		listJobsFn: func(context.Context, string, []store.State, int, *uuid.UUID) ([]store.Job, error) {
			return nil, errors.New(`ERROR: relation "jobs" does not exist (SQLSTATE 42P01)`)
		},
	})
	_, err := client.ListJobs(ctxT(t), &hopperv1.ListJobsRequest{Queue: "q"})
	require.Equal(t, codes.Internal, status.Code(err))
	require.NotContains(t, err.Error(), "SQLSTATE")
}

// TestListJobsPageTooLargeIsResourceExhausted: a page of fat payloads can pass
// the 4 MiB ceiling even at a legal limit. The caller's remedy, documented on
// the field, is a smaller limit, which works.
func TestListJobsPageTooLargeIsResourceExhausted(t *testing.T) {
	t.Parallel()

	fat := []byte(`"` + strings.Repeat("x", 1300<<10) + `"`)
	client, _ := newTestService(t, &fakeStore{
		listJobsFn: func(_ context.Context, _ string, _ []store.State, limit int, _ *uuid.UUID) ([]store.Job, error) {
			jobs := make([]store.Job, limit)
			for i := range jobs {
				jobs[i] = sampleJob("q")
				jobs[i].Payload = fat
			}
			return jobs, nil
		},
	})

	_, err := client.ListJobs(ctxT(t), &hopperv1.ListJobsRequest{Queue: "q", Limit: 5})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))

	resp, err := client.ListJobs(ctxT(t), &hopperv1.ListJobsRequest{Queue: "q", Limit: 2})
	require.NoError(t, err)
	require.Len(t, resp.GetJobs(), 2)
}
