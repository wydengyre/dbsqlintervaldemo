package leader_test

import (
	"context"
	"database/sql"
	"github.com/fergusstrange/embedded-postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPgx(t *testing.T) {
	testLeader(t, "pgx")
}

func TestPq(t *testing.T) {
	testLeader(t, "postgres")
}

func testLeader(t *testing.T, driverName string) {
	conf := embeddedpostgres.DefaultConfig()
	pg := embeddedpostgres.NewDatabase(conf)
	t.Log("Starting embedded postgres")
	err := pg.Start()
	require.NoError(t, err)
	t.Cleanup(func() {
		err := pg.Stop()
		require.NoError(t, err)
	})
	t.Log("Started embedded postgres")

	t.Log("migrating river")
	connStr := conf.GetConnectionURL() + "?sslmode=disable"
	db, err := sql.Open(driverName, connStr)
	require.NoError(t, err)
	riverDriver := riverdatabasesql.New(db)
	migrator := rivermigrate.New(riverDriver, nil)
	_, err = migrator.Migrate(context.Background(), rivermigrate.DirectionUp, nil)
	require.NoError(t, err)
	t.Log("migrated river")

	t.Log("electing leader")
	executor := riverDriver.GetExecutor()
	params := riverdriver.LeaderElectParams{
		LeaderID: "test",
		TTL:      5 * time.Second,
	}
	elected, err := executor.LeaderAttemptElect(context.Background(), &params)
	require.NoError(t, err)
	require.True(t, elected)
	t.Log("elected leader")

	t.Log("checking leader times")
	var electedAt, expiresAt time.Time
	row := db.QueryRow("SELECT elected_at, expires_at FROM river_leader")
	err = row.Scan(&electedAt, &expiresAt)
	require.NoError(t, err)
	interval := expiresAt.Sub(electedAt)
	t.Logf("elected_at: %v, expires_at: %v", electedAt, expiresAt)
	require.Equalf(t, params.TTL, interval,
		"expected expires_at - elected_at (%v) to be equal to TTL (%v)",
		interval, params.TTL)
}
