package mysqlx

import (
	"context"
	"database/sql/driver"
)

type conn struct {
	driver.Conn
	*hook
}

func (c conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	handler := func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		if queryerContext, ok := c.Conn.(driver.QueryerContext); ok {
			rows, err := queryerContext.QueryContext(ctx, query, args)
			return rows, err
		}
		values, err := namedValueToValue(args)
		if err != nil {
			return nil, err
		}
		return c.Query(query, values)
	}
	if len(args) == 0 && c.hook != nil && c.hook.onQueryerHook != nil {
		return c.hook.onQueryerHook(handler)(ctx, query, args)
	}
	return handler(ctx, query, args)
}

func (c conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if queryer, ok := c.Conn.(driver.Queryer); ok {
		return queryer.Query(query, args)
	}
	return nil, ErrUnsupported
}

func (c conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	handler := func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
		if execerContext, ok := c.Conn.(driver.ExecerContext); ok {
			r, err := execerContext.ExecContext(ctx, query, args)
			return r, err
		}
		values, err := namedValueToValue(args)
		if err != nil {
			return nil, err
		}
		return c.Exec(query, values)
	}
	if len(args) == 0 && c.hook != nil && c.hook.onExecerHook != nil {
		return c.hook.onExecerHook(handler)(ctx, query, args)
	}
	return handler(ctx, query, args)
}

func (c conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if execer, ok := c.Conn.(driver.Execer); ok {
		return execer.Exec(query, args)
	}
	return nil, ErrUnsupported
}

func (c conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if connPrepareContext, ok := c.Conn.(driver.ConnPrepareContext); ok {
		s, err := connPrepareContext.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		return &stmt{Stmt: s, query: query, hook: c.hook}, nil
	}
	return c.Conn.Prepare(query)
}

func (c conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if connBeginTx, ok := c.Conn.(driver.ConnBeginTx); ok {
		t, err := connBeginTx.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		return &tx{Tx: t}, nil
	}
	return c.Conn.Begin()
}

func (c conn) Prepare(query string) (driver.Stmt, error) {
	s, err := c.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &stmt{Stmt: s, query: query, hook: c.hook}, nil
}

func (c conn) Begin() (driver.Tx, error) {
	t, err := c.Conn.Begin()
	if err != nil {
		return nil, err
	}
	return &tx{Tx: t}, nil
}
