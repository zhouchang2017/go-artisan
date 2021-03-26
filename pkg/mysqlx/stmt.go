package mysqlx

import (
	"context"
	"database/sql/driver"
)

type stmt struct {
	driver.Stmt
	query string
	*hook
}

func (s stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	handler := func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		if queryerContext, ok := s.Stmt.(driver.StmtQueryContext); ok {
			rows, err := queryerContext.QueryContext(ctx, args)
			return rows, err
		}
		values, err := namedValueToValue(args)
		if err != nil {
			return nil, err
		}
		return s.Query(values)
	}
	if s.hook != nil && s.hook.onQueryerHook != nil {
		return s.hook.onQueryerHook(handler)(ctx, s.query, args)
	}
	return handler(ctx, s.query, args)
}

func (s stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	handler := func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
		if execerContext, ok := s.Stmt.(driver.StmtExecContext); ok {
			return execerContext.ExecContext(ctx, args)
		}
		values, err := namedValueToValue(args)
		if err != nil {
			return nil, err
		}
		return s.Exec(values)
	}
	if s.hook != nil && s.hook.onExecerHook != nil {
		return s.hook.onExecerHook(handler)(ctx, s.query, args)
	}
	return handler(ctx, s.query, args)
}
