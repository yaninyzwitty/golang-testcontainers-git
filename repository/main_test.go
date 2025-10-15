package repository_test

import (
	"context"
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dreamsofcode-io/testcontainers/database"
	"github.com/gocql/gocql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/cassandra"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var connURL = ""
var connectionHost = ""
var parallel = false
var sleepTime = time.Millisecond * 500

func TestMain(m *testing.M) {
	ctx := context.Background()

	cassandraContainer, err := cassandra.Run(ctx,
		"cassandra:4.1.3",
		cassandra.WithInitScripts(filepath.Join("testdata", "init.cql")),
		cassandra.WithConfigFile(filepath.Join("testdata", "init.yaml")),
	)

	defer func() {
		if err := testcontainers.TerminateContainer(cassandraContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()

	if err != nil {
		log.Printf("failed to start container: %s", err)
		return
	}

	connectionHost, err = cassandraContainer.ConnectionHost(ctx)
	if err != nil {
		log.Printf("failed to get connection host: %s", err)
		return
	}

	container, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("foobar"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second),
		),
	)
	if err != nil {
		log.Fatalln("failed to load container:", err)
	}

	connURL, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalln("failed to get connection string:", err)
	}

	migrate, err := database.Migrate(connURL)
	if err != nil {
		log.Fatal("failed to migrate db: ", err)
	}

	res := m.Run()

	migrate.Drop()

	os.Exit(res)
}

func getConnection(ctx context.Context) (*sql.DB, error) {
	return database.Connect(connURL)
}
func getCassandraConnection(ctx context.Context) (*gocql.Session, error) {
	return database.ConnectCassandra(connectionHost)
}

func cleanup() {
	conn, err := database.Connect(connURL)
	if err != nil {
		return
	}

	conn.Exec("DELETE FROM spell")
}

func checkParallel(t *testing.T) {
	if parallel {
		t.Parallel()
	}
}
