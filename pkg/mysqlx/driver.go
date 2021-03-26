package mysqlx

import (
	"database/sql/driver"
	"errors"
)

var ErrUnsupported = errors.New("operation unsupported by the underlying driver")

type sqlDriver struct {
	driver.Driver
	hooks []Hook
}

func New(driver driver.Driver, hooks ...Hook) driver.Driver {
	return &sqlDriver{
		Driver: driver,
		hooks:  hooks,
	}
}

func (s *sqlDriver) Open(dns string) (driver.Conn, error) {
	c, err := s.Driver.Open(dns)
	if err != nil {
		return nil, err
	}
	h := &hook{
		onQueryerHook: nil,
		onExecerHook:  nil,
	}
	var onQueryerHook []OnQueryerHook
	var onExecerHook []OnExecerHook
	for _, item := range s.hooks {
		if item.OnQueryerHook != nil {
			onQueryerHook = append(onQueryerHook, item.OnQueryerHook)
		}
		if item.OnExecerHook != nil {
			onExecerHook = append(onExecerHook, item.OnExecerHook)
		}
	}
	if len(onQueryerHook) > 0 {
		h.onQueryerHook = queryerHookChain(onQueryerHook[0], onQueryerHook[1:]...)
	}
	if len(onExecerHook) > 0 {
		h.onExecerHook = execerHookChain(onExecerHook[0], onExecerHook[1:]...)
	}

	return &conn{
		Conn: c,
		hook: h,
	}, nil
}
