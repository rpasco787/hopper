package worker

import "time"

// freshFraction sets the start rule's threshold: a delivery read off its
// stream less than grant/freshFraction after the credit it was matched to was
// sent starts without an ownership check. At a tenth, the deadline leaves at
// least 0.8 of the grant after receipt, so both renewals at G/3 and 2G/3 fall
// before it (docs/design/credit-anchored-freshness.md, docs/semantics.md §11.3).
const freshFraction = 10

// freshDeadline is the start rule. creditSent is when the credit unit the
// delivery was matched to went to the transport, received when the delivery
// was read off the stream, both on the local monotonic clock.
//
// The broker leases only against credit it has received, so the lease began
// after creditSent and runs until at least creditSent + grant: the deadline
// deadlineFrom(creditSent, grant) is safe whatever the delivery's age. The
// age decides only whether that leaves enough of the lease to be worth
// skipping the check, which would also have bought a fresh grant.
func freshDeadline(creditSent, received time.Time, grant time.Duration) (time.Time, bool) {
	if creditSent.IsZero() || grant <= 0 {
		return time.Time{}, false
	}
	age := received.Sub(creditSent)
	if age < 0 || age >= grant/freshFraction {
		return time.Time{}, false
	}
	return deadlineFrom(creditSent, grant), true
}
