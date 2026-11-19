package integration

import (
	"os"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/broker"
	"github.com/rpasco787/hopper/internal/store"
	"github.com/rpasco787/hopper/internal/store/postgres"
)

// batchingOn reports whether this run of the suite confirms in batches: the
// broker default unless HOPPER_TEST_CONFIRM_BATCH names another size.
func batchingOn() bool {
	v := os.Getenv("HOPPER_TEST_CONFIRM_BATCH")
	if v == "" {
		return broker.DefaultConfirmBatch > 1
	}
	n, err := strconv.Atoi(v)
	return err == nil && n > 1
}

// batchOf is the BatchConfirmer inside a test wrapper. Every wrapper around a
// store forwards AckMany and ExtendLeaseMany through it, so turning batching
// on never silently sends a test down the one-row path, and a wrapper that
// injects a fault into Ack or ExtendLease injects it into the batch form too.
func batchOf(st store.Store) store.BatchConfirmer { return st.(store.BatchConfirmer) }

// repeatErr is err for each of n inputs.
func repeatErr(err error, n int) []error {
	errs := make([]error, n)
	for i := range errs {
		errs[i] = err
	}
	return errs
}

// A separately constructed restart broker must honor the suite switch too.
func TestRestartBrokerHonorsConfirmationBatchSwitch(t *testing.T) {
	t.Setenv("HOPPER_TEST_CONFIRM_BATCH", "64")
	st := &renewals{Store: postgres.New(pool), results: make(chan error, 1)}
	b := serveLiveBroker(t, "127.0.0.1:0", st, broker.Config{})
	_, err := newClientAt(t, b.addr).Heartbeat(t.Context(), &hopperv1.HeartbeatRequest{
		Id: uuid.NewString(), LeaseId: uuid.NewString(),
	})
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Positive(t, st.batches.Load(), "restart broker bypassed confirmation batching")
}

// confirmationConfig applies the suite switch to every broker constructor.
func confirmationConfig(t *testing.T, st store.Store, cfg broker.Config) broker.Config {
	t.Helper()
	// HOPPER_TEST_CONFIRM_BATCH sets the suite's confirmation batch size
	// (docs/design/confirmation-batching.md); unset, brokers take the default,
	// and 1 runs the suite with batching off.
	if v := os.Getenv("HOPPER_TEST_CONFIRM_BATCH"); v != "" && cfg.ConfirmBatch == 0 {
		n, err := strconv.Atoi(v)
		require.NoError(t, err, "HOPPER_TEST_CONFIRM_BATCH=%q", v)
		cfg.ConfirmBatch = n
	}
	size := cfg.ConfirmBatch
	if size == 0 {
		size = broker.DefaultConfirmBatch
	}
	// A store that can't batch would quietly confirm one call at a time and
	// pass every test without exercising batching.
	if size > 1 {
		_, ok := st.(store.BatchConfirmer)
		require.True(t, ok, "%T hides store.BatchConfirmer: give it AckMany and ExtendLeaseMany (batching_test.go)", st)
	}

	return cfg
}
