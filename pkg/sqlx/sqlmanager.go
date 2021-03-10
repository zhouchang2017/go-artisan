package sqlx

import (
	"database/sql"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/tal-tech/go-zero/core/syncx"
)

const (
	maxIdleConns = 64
	maxOpenConns = 64
	maxLifetime  = time.Minute
)

var connManager = syncx.NewResourceManager()

type pingedDB struct {
	*sql.DB
	once sync.Once
}

func getCachedSqlConn(cfg Conf) (*pingedDB, error) {

	val, err := connManager.GetResource(getManagerKey(cfg), func() (io.Closer, error) {
		conn, err := newDBConnection(cfg)
		if err != nil {
			return nil, err
		}

		return &pingedDB{
			DB: conn,
		}, nil
	})
	if err != nil {
		return nil, err
	}

	return val.(*pingedDB), nil
}

func getSqlConn(cfg Conf) (*sql.DB, error) {
	pdb, err := getCachedSqlConn(cfg)
	if err != nil {
		return nil, err
	}

	pdb.once.Do(func() {
		err = pdb.Ping()
	})
	if err != nil {
		return nil, err
	}

	return pdb.DB, nil
}

func newDBConnection(cfg Conf) (*sql.DB, error) {
	fmt.Printf("conn string: %s\n", cfg.String())
	conn, err := sql.Open(cfg.GetDriver(), cfg.String())
	if err != nil {
		return nil, err
	}

	cfg.Setup(conn)

	return conn, nil
}
