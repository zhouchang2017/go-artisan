package mysqlx

import (
	"context"
	"database/sql/driver"
)

type stmt struct {
	driver.Stmt
	hooks []Hook
}

func (s stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	panic("implement me")
}

func (s stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	panic("implement me")
}

func (s stmt) Close() error {
	panic("implement me")
}

func (s stmt) NumInput() int {
	panic("implement me")
}

func (s stmt) Exec(args []driver.Value) (driver.Result, error) {
	panic("implement me")
}

func (s stmt) Query(args []driver.Value) (driver.Rows, error) {
	panic("implement me")
}
