package mysqlx

import (
	"database/sql/driver"
	"errors"
)

var ErrUnsupported = errors.New("operation unsupported by the underlying driver")


type SQLDriver struct {
	driver.Driver
	hooks []Hook
}

func (s *SQLDriver) AddHooks(hooks ...Hook) {
	s.hooks = append(s.hooks, hooks...)
}

func (s *SQLDriver) Open(dns string) (driver.Conn, error) {
	c, err := s.Driver.Open(dns)
	if err != nil {
		return nil, err
	}
	return &conn{
		Conn:  c,
		hooks: s.hooks,
	}, nil
}
