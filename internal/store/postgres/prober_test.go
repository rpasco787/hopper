package postgres

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

func TestProberSeesUpAndDown(t *testing.T) {
	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)
	p := NewProber(cfg)
	t.Cleanup(p.Close)
	require.NoError(t, p.Probe(ctxT(t)))
	require.NoError(t, p.Probe(ctxT(t)), "reuses its session")

	dead, err := pgx.ParseConfig("postgres://hopper:hopper@127.0.0.1:1/hopper?connect_timeout=1")
	require.NoError(t, err)
	err = NewProber(dead).Probe(ctxT(t))
	require.ErrorIs(t, err, store.ErrNotSent)
}
