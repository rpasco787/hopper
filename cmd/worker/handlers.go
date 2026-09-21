package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	workersdk "github.com/rpasco787/hopper/worker"
)

const defaultWorkDuration = 100 * time.Millisecond

type durationPayload struct {
	Milliseconds *int64 `json:"ms"`
}

func workDuration(job workersdk.Job) (time.Duration, error) {
	var payload durationPayload
	if err := job.Unmarshal(&payload); err != nil {
		return 0, err
	}
	if payload.Milliseconds == nil {
		return defaultWorkDuration, nil
	}
	ms := *payload.Milliseconds
	if ms < 0 {
		return 0, errors.New("ms must be non-negative")
	}
	if ms > math.MaxInt64/int64(time.Millisecond) {
		return 0, errors.New("ms overflows time.Duration")
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func sleepHandler(ctx context.Context, job workersdk.Job) error {
	duration, err := workDuration(job)
	if err != nil {
		return err
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type failurePayload struct {
	Fail int32 `json:"fail"`
}

func failNTimesHandler(_ context.Context, job workersdk.Job) error {
	var payload failurePayload
	if err := job.Unmarshal(&payload); err != nil {
		return err
	}
	if payload.Fail < 0 {
		return errors.New("fail must be non-negative")
	}
	if job.Attempt <= payload.Fail {
		return fmt.Errorf("intentional failure on attempt %d of %d", job.Attempt, payload.Fail)
	}
	return nil
}

var cpuChecksum atomic.Uint64

func cpuBurnHandler(ctx context.Context, job workersdk.Job) error {
	duration, err := workDuration(job)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(duration)
	x := uint64(job.Attempt) + 1
	for time.Now().Before(deadline) {
		for range 1024 {
			x = x*6364136223846793005 + 1442695040888963407
		}
		select {
		case <-ctx.Done():
			cpuChecksum.Store(x)
			return ctx.Err()
		default:
		}
	}
	cpuChecksum.Store(x)
	return nil
}
