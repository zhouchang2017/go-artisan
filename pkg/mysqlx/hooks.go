package mysqlx

import (
	"context"
	"database/sql/driver"
)

type Hook struct {
	OnQueryerHook
	OnExecerHook
}

type hook struct {
	onQueryerHook OnQueryerHook
	onExecerHook  OnExecerHook
}

type OnQueryerFunc func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)

type OnQueryerHook func(next OnQueryerFunc) OnQueryerFunc

func queryerHookChain(outer OnQueryerHook, others ...OnQueryerHook) OnQueryerHook {
	return func(next OnQueryerFunc) OnQueryerFunc {
		for i := len(others) - 1; i >= 0; i-- {
			next = others[i](next)
		}
		return outer(next)
	}
}

type OnExecerFunc func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error)

type OnExecerHook func(next OnExecerFunc) OnExecerFunc

func execerHookChain(outer OnExecerHook, others ...OnExecerHook) OnExecerHook {
	return func(next OnExecerFunc) OnExecerFunc {
		for i := len(others) - 1; i >= 0; i-- {
			next = others[i](next)
		}
		return outer(next)
	}
}
