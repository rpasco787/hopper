package store

import "time"

// Observer is told about job transitions a Store has committed. It exists for
// metrics (docs/semantics.md §14) and is not a ledger. Each transition is
// reported at most once, and only after the store has observed definite
// commit success, before the store method returns. A retry that finds its
// transition already applied reports nothing. A transition that committed but
// whose success the store never observed (an ambiguous commit result, a
// context cancelled after commit, a crash before the call, or an observer
// panic) is never reported. Counts built on this interface can therefore
// undercount and can never overcount. The database is the ledger.
//
// Calls are synchronous on the store's hot paths and come from many
// goroutines at once. Implementations must be safe for concurrent use, must
// not block, must not call back into the Store, and must not retain the jobs
// slice passed to Leased.
type Observer interface {
	// Enqueued: Enqueue inserted a row. A deduplicated Enqueue is not reported.
	Enqueued(queue string)
	// Leased: Lease granted these deliveries. Attempts == 1 marks a job's first
	// delivery since it was enqueued or redriven.
	Leased(jobs []Job)
	// Completed: Ack moved a leased job to completed. Both instants are the
	// database's. leasedAt is zero if the row had none, which the schema
	// permits but Lease never produces.
	Completed(queue string, leasedAt, finishedAt time.Time)
	// Nacked: Nack applied the retry policy. dead reports that the job was
	// dead-lettered rather than rescheduled.
	Nacked(queue string, dead bool)
	// LeaseExpired: the reaper took back an expired lease; dead as for Nacked.
	LeaseExpired(queue string, dead bool)
}

// NopObserver observes nothing. It is what a Store uses until told otherwise.
type NopObserver struct{}

func (NopObserver) Enqueued(string)                        {}
func (NopObserver) Leased([]Job)                           {}
func (NopObserver) Completed(string, time.Time, time.Time) {}
func (NopObserver) Nacked(string, bool)                    {}
func (NopObserver) LeaseExpired(string, bool)              {}
