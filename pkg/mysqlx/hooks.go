package mysqlx

import (
	"context"
	"database/sql/driver"
)

type OnQueryer func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)
