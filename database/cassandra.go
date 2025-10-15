package database

import (
	"time"

	"github.com/gocql/gocql"
)

func ConnectCassandra(hosts string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(hosts)
	cluster.Keyspace = "init_sh_keyspace" // ✅ Set correct keyspace
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second

	return cluster.CreateSession()
}
