package accounting_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/test/accounting"
)

func TestEveryAcknowledgedJobPresentIsDurable(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	t0 := time.Unix(1000, 0)
	d := accounting.CheckDurability(
		[]accounting.Acknowledged{{ID: a, At: t0}, {ID: b, At: t0.Add(time.Second)}, {ID: a, At: t0.Add(2 * time.Second)}},
		[]accounting.Row{{ID: a, State: "completed"}, {ID: b, State: "available"}, {ID: uuid.New(), State: "completed"}})
	require.Equal(t, 2, d.Acknowledged, "a retried key counts once")
	require.Empty(t, d.Lost)
}

func TestAMissingAcknowledgedJobIsLostAtItsFirstAnswer(t *testing.T) {
	kept, x, y := uuid.New(), uuid.New(), uuid.New()
	t0 := time.Unix(1000, 0)
	d := accounting.CheckDurability([]accounting.Acknowledged{
		{ID: x, At: t0.Add(3 * time.Second)}, {ID: kept, At: t0},
		{ID: y, At: t0.Add(2 * time.Second)}, {ID: x, At: t0.Add(time.Second)},
	}, []accounting.Row{{ID: kept}})
	require.Equal(t, 3, d.Acknowledged)
	require.Equal(t, []accounting.Acknowledged{{ID: x, At: t0.Add(time.Second)}, {ID: y, At: t0.Add(2 * time.Second)}}, d.Lost,
		"lost jobs in order of their first answer")
}
