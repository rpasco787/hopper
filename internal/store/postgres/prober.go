package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// Prober checks one database over a session of its own, outside any pool, so
// a pool busy with real work never makes a healthy shard look down
// (docs/design/sharding.md). It is not safe for concurrent use: one goroutine
// probes.
type Prober struct {
	cfg  *pgx.ConnConfig
	conn *pgx.Conn
}

// NewProber returns a Prober that connects with a copy of cfg.
func NewProber(cfg *pgx.ConnConfig) *Prober { return &Prober{cfg: cfg.Copy()} }

// Probe connects if it has no session, then runs one trivial statement. A
// failure closes the session, so the next Probe dials again.
func (p *Prober) Probe(ctx context.Context) error {
	if p.conn == nil {
		c, err := pgx.ConnectConfig(ctx, p.cfg)
		if err != nil {
			return wrap("probe: connect", err)
		}
		p.conn = c
	}
	if _, err := p.conn.Exec(ctx, "select 1"); err != nil {
		p.Close()
		return wrap("probe", err)
	}
	return nil
}

// Close ends the probe's session, if it has one.
func (p *Prober) Close() {
	if p.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = p.conn.Close(ctx)
	p.conn = nil
}
