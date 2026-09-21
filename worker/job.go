package worker

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Job is one delivery. It is what a handler gets, and it deliberately omits the
// lease token: the SDK owns the lease, and a handler that acked or extended it
// behind the SDK's back would break the accounting that keeps a handler's
// context honest. (The token is not a secret — §12 of docs/semantics.md is
// clear that this API has no authorization at all — it is simply not the
// handler's to hold.)
type Job struct {
	ID    uuid.UUID
	Queue string
	// Payload is the JSON the producer enqueued, as stored. The broker sends
	// bytes; hopper never interprets them.
	Payload json.RawMessage
	// Attempt is which delivery this is, counting from 1. It describes
	// delivery history, so it is useful for logs and for giving up early. It is
	// not an idempotency key: two deliveries of the same job carry different
	// Attempt values, so deduplicating on it deduplicates nothing. Use ID, or
	// a business key inside the payload.
	Attempt int32
	// MaxAttempts is the dead-letter threshold. Attempt == MaxAttempts means a
	// returned error sends this job to dead rather than back for a retry.
	MaxAttempts int32
	// EnqueuedAt is when the row was created, not when this delivery started.
	EnqueuedAt time.Time
}

// Unmarshal decodes the payload into v.
//
// It exists so handlers do not have to remember that Payload is raw JSON:
// hopper's own wire type is `bytes`, and proto3 JSON would have encoded it as
// base64, which is the kind of surprise that costs an afternoon.
func (j Job) Unmarshal(v any) error {
	if len(j.Payload) == 0 {
		return fmt.Errorf("worker: job %s has an empty payload", j.ID)
	}
	if err := json.Unmarshal(j.Payload, v); err != nil {
		return fmt.Errorf("worker: unmarshal payload of job %s: %w", j.ID, err)
	}
	return nil
}
