//go:build chaos

package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/client"
)

// TestEnqueueStormDeduplicates has sixteen producers enqueue through both
// brokers, singly and in batches, for 25s while one broker crashes and
// PostgreSQL restarts under them. Every 100ms brings four new keys that all
// producers contend for, so first inserts race each other throughout both
// disruptions, beside lookups of older keys. Each producer retries a failed
// call with the same keys until it succeeds, as a producer that must not lose
// a job would, including calls whose insert may or may not have committed.
// Dedup must hold: one row per key, counted in SQL, and every answer for a
// key names that row.
func TestEnqueueStormDeduplicates(t *testing.T) {
	const producers, perSlot = 16, 4
	const slot, storm = 100 * time.Millisecond, 25 * time.Second
	s := up(t, "storm")
	run := fmt.Sprintf("storm-%d", time.Now().UnixNano())
	key := func(k int) string { return fmt.Sprintf("%s-%d", run, k) }
	clients := []*client.Client{s.client(s.brokerAddr), s.client(s.standbyAddr)}
	// The storm and every retry in it end within this budget, so a producer
	// stuck behind a dead broker cannot hold the test past its cleanup.
	ctx, cancel := context.WithTimeout(s.ctx, storm+90*time.Second)
	defer cancel()

	type answer struct {
		key          string
		id           uuid.UUID
		deduplicated bool
	}
	var (
		mu        sync.Mutex
		answers   []answer
		attempted = map[string]bool{}
		answered  = map[string]bool{}
		// inDoubt are keys a call failed on before any producer had an answer
		// for them: their insert may or may not have committed.
		inDoubt        = map[string]bool{}
		calls, retries atomic.Int64
	)
	// call retries one enqueue, alternating brokers, until it succeeds.
	call := func(p int, rng *rand.Rand, ks []int) error {
		jobs := make([]client.Job, len(ks))
		mu.Lock()
		for i, k := range ks {
			jobs[i] = client.Job{Queue: "storm", Payload: json.RawMessage(`{}`), IdempotencyKey: key(k)}
			attempted[key(k)] = true
		}
		mu.Unlock()
		for attempt := 0; ; attempt++ {
			cl := clients[(p+attempt)%len(clients)]
			actx, acancel := context.WithTimeout(ctx, 5*time.Second)
			var results []client.Result
			var err error
			if len(jobs) == 1 {
				var r client.Result
				r, err = cl.Enqueue(actx, jobs[0])
				results = []client.Result{r}
			} else {
				results, err = cl.EnqueueBatch(actx, jobs)
			}
			acancel()
			calls.Add(1)
			mu.Lock()
			if err == nil {
				for i, r := range results {
					answers = append(answers, answer{key: jobs[i].IdempotencyKey, id: r.ID, deduplicated: r.Deduplicated})
					answered[jobs[i].IdempotencyKey] = true
				}
			} else {
				for _, j := range jobs {
					if !answered[j.IdempotencyKey] {
						inDoubt[j.IdempotencyKey] = true
					}
				}
			}
			mu.Unlock()
			if err == nil {
				return nil
			}
			if ctx.Err() != nil {
				return fmt.Errorf("producer %d: keys %v: gave up after %d attempts when the storm's budget ran out: %w", p, ks, attempt+1, err)
			}
			retries.Add(1)
			select {
			case <-ctx.Done():
			case <-time.After(50*time.Millisecond + time.Duration(rng.Int64N(int64(50*time.Millisecond)))):
			}
		}
	}

	began := time.Now()
	// fresh is the first of the current slot's keys.
	fresh := func() int { return int(time.Since(began)/slot) * perSlot }
	errs := make(chan error, producers)
	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(p), uint64(began.UnixNano())))
			for time.Since(began) < storm && ctx.Err() == nil {
				base := fresh()
				var ks []int
				switch n := rng.IntN(3); {
				case n == 0: // one of the slot's new keys
					ks = []int{base + rng.IntN(perSlot)}
				case n == 1: // three of them, in one batch
					for _, i := range rng.Perm(perSlot)[:3] {
						ks = append(ks, base+i)
					}
				case base > 0: // an older key: a lookup
					ks = []int{rng.IntN(base)}
				default:
					ks = []int{base}
				}
				if err := call(p, rng, ks); err != nil {
					errs <- err
					return
				}
				select { // about 800 calls a second across producers
				case <-ctx.Done():
				case <-time.After(20 * time.Millisecond):
				}
			}
		}()
	}
	time.Sleep(storm / 4)
	broker := s.containers("broker")[0]
	s.kill(broker)
	s.start(broker)
	time.Sleep(time.Until(began.Add(storm / 2)))
	s.restart(s.containers("postgres")[0])
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	s.waitDatabase()

	// Rows per key, counted by the database: a map from key to id would
	// collapse duplicate rows into one.
	qctx, qcancel := s.within(reportLimit)
	defer qcancel()
	rows, err := s.db.Query(qctx, `select idempotency_key, count(*), (array_agg(id))[1] from jobs
	                               where queue = 'storm' and idempotency_key like $1 || '-%' group by idempotency_key`, run)
	require.NoError(t, err)
	perKey, stored := map[string]int{}, map[string]uuid.UUID{}
	for rows.Next() {
		var k string
		var n int
		var id uuid.UUID
		require.NoError(t, rows.Scan(&k, &n, &id))
		perKey[k], stored[k] = n, id
	}
	require.NoError(t, rows.Err())
	var total int
	require.NoError(t, s.db.QueryRow(qctx, `select count(*) from jobs where queue = 'storm' and idempotency_key like $1 || '-%'`, run).Scan(&total))

	mu.Lock()
	defer mu.Unlock()
	t.Logf("run %s: %d calls (%d retried) gave %d answers for %d keys, %d of them in doubt when a call failed; %d rows",
		run, calls.Load(), retries.Load(), len(answers), len(attempted), len(inDoubt), total)
	require.Equal(t, len(attempted), total, "rows, counted in SQL, against keys tried")
	for k := range attempted {
		require.Equal(t, 1, perKey[k], "key %s has %d rows", k, perKey[k])
	}
	require.Len(t, perKey, len(attempted), "rows for keys no producer tried")
	inserted := map[string]int{}
	for _, a := range answers {
		require.Equal(t, stored[a.key], a.id, "key %s was answered with a job that is not its row", a.key)
		if !a.deduplicated {
			inserted[a.key]++
		}
	}
	for k, n := range inserted {
		require.LessOrEqual(t, n, 1, "key %s was reported inserted %d times", k, n)
	}
	require.Positive(t, retries.Load(), "no call failed; the disruptions missed the storm")
	require.NotEmpty(t, inDoubt, "no call failed on a key before it had an answer; inserts with an unknown outcome were not exercised")
}
