package accounting

import (
	"cmp"
	"slices"
	"time"

	"github.com/google/uuid"
)

// Acknowledged is a job whose Enqueue returned without error: its producer
// was told it was committed.
type Acknowledged struct {
	ID uuid.UUID
	At time.Time // when the answer arrived
}

// Durability is the check behind "a job is never lost once Enqueue has
// returned" (docs/semantics.md §1), made against what the database holds
// after a failure.
type Durability struct {
	Acknowledged int            // distinct jobs acknowledged
	Lost         []Acknowledged // acknowledged and in no row, by first answer
}

// CheckDurability looks for every acknowledged job among rows. A job
// acknowledged more than once, as a retried key is, counts once, at its first
// answer.
func CheckDurability(acked []Acknowledged, rows []Row) Durability {
	present := make(map[uuid.UUID]bool, len(rows))
	for _, r := range rows {
		present[r.ID] = true
	}
	first := map[uuid.UUID]time.Time{}
	for _, a := range acked {
		if at, ok := first[a.ID]; !ok || a.At.Before(at) {
			first[a.ID] = a.At
		}
	}
	d := Durability{Acknowledged: len(first)}
	for id, at := range first {
		if !present[id] {
			d.Lost = append(d.Lost, Acknowledged{ID: id, At: at})
		}
	}
	slices.SortFunc(d.Lost, func(a, b Acknowledged) int {
		return cmp.Or(a.At.Compare(b.At), cmp.Compare(a.ID.String(), b.ID.String()))
	})
	return d
}
