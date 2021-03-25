package mysqlx

import "database/sql/driver"

type tx struct {
	driver.Tx
}
