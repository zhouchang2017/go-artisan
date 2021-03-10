package sqlx

import (
	"context"
	"database/sql"

	"github.com/didi/gendry/scanner"
	"github.com/tal-tech/go-zero/core/breaker"
)

var ErrNotFound = sql.ErrNoRows

type (
	Session interface {
		// Prepare creates a prepared statement for later queries or executions.
		// Multiple queries or executions may be run concurrently from the
		// returned statement.
		// The caller must call the statement's Close method
		// when the statement is no longer needed.
		Prepare(query string) (StmtSession, error)

		// PrepareContext creates a prepared statement for later queries or executions.
		// Multiple queries or executions may be run concurrently from the
		// returned statement.
		// The caller must call the statement's Close method
		// when the statement is no longer needed.
		//
		// The provided context is used for the preparation of the statement, not for the
		// execution of the statement.
		PrepareContext(ctx context.Context, query string) (StmtSession, error)

		// ExecContext executes a query without returning any rows.
		// The args are for any placeholder parameters in the query.
		ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

		// Exec executes a query without returning any rows.
		// The args are for any placeholder parameters in the query.
		Exec(query string, args ...interface{}) (sql.Result, error)

		// QueryContext executes a query that returns rows, typically a SELECT.
		// The args are for any placeholder parameters in the query.
		QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

		// Query executes a query that returns rows, typically a SELECT.
		// The args are for any placeholder parameters in the query.
		Query(query string, args ...interface{}) (*sql.Rows, error)

		// QueryRowContext executes a query that is expected to return at most one row.
		// QueryRowContext always returns a non-nil value. Errors are deferred until
		// Row's Scan method is called.
		// If the query selects no rows, the *Row's Scan will return ErrNoRows.
		// Otherwise, the *Row's Scan scans the first selected row and discards
		// the rest.
		QueryRowContext(ctx context.Context, query string, args ...interface{}) (row *sql.Row, err error)

		// QueryRow executes a query that is expected to return at most one row.
		// QueryRow always returns a non-nil value. Errors are deferred until
		// Row's Scan method is called.
		// If the query selects no rows, the *Row's Scan will return ErrNoRows.
		// Otherwise, the *Row's Scan scans the first selected row and discards
		// the rest.
		QueryRow(query string, args ...interface{}) (row *sql.Row, err error)
	}

	SqlConn interface {
		Session
		Transact(func(session Session) error) error
	}

	StmtSession interface {

		// ExecContext executes a prepared statement with the given arguments and
		// returns a Result summarizing the effect of the statement.
		ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error)

		// Exec executes a prepared statement with the given arguments and
		// returns a Result summarizing the effect of the statement.
		Exec(args ...interface{}) (sql.Result, error)

		// QueryContext executes a prepared query statement with the given arguments
		// and returns the query results as a *Rows.
		QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error)

		// Query executes a prepared query statement with the given arguments
		// and returns the query results as a *Rows.
		Query(args ...interface{}) (*sql.Rows, error)

		// QueryRowContext executes a prepared query statement with the given arguments.
		// If an error occurs during the execution of the statement, that error will
		// be returned by a call to Scan on the returned *Row, which is always non-nil.
		// If the query selects no rows, the *Row's Scan will return ErrNoRows.
		// Otherwise, the *Row's Scan scans the first selected row and discards
		// the rest.
		QueryRowContext(ctx context.Context, args ...interface{}) (row *sql.Row)

		// QueryRow executes a prepared query statement with the given arguments.
		// If an error occurs during the execution of the statement, that error will
		// be returned by a call to Scan on the returned *Row, which is always non-nil.
		// If the query selects no rows, the *Row's Scan will return ErrNoRows.
		// Otherwise, the *Row's Scan scans the first selected row and discards
		// the rest.
		//
		// Example usage:
		//
		//  var name string
		//  err := nameByUseridStmt.QueryRow(id).Scan(&name)
		QueryRow(args ...interface{}) (row *sql.Row)
	}

	sessionConn interface {
		ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	}

	stmtConn interface {
		ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error)
		QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row
	}

	Rows interface {
		scanner.Rows

		// ScanStruct scans data from rows to target
		// Don't forget to close the rows
		// When the target is not a pointer of slice, ErrEmptyResult
		// may be returned if the queryBuilder result is empty
		ScanStruct(target interface{}) error

		// ScanStructClose is the same as Scan and helps you Close the rows
		// Don't exec the rows.Close after calling this
		ScanStructClose(target interface{}) error

		// ScanMap returns the result in the form of []map[string]interface{}
		// json.Marshal encodes []byte as a base64 string, while in most cases
		// it's expected to be encoded as string or int. If you want this, use
		// ScanMapDecode instead.
		ScanMap() ([]map[string]interface{}, error)

		// ScanMapClose is the same as ScanMap and close the rows
		ScanMapClose() ([]map[string]interface{}, error)

		// ScanMapDecode returns the result in the form of []map[string]interface{}
		// If possible, it will convert []uint8 to int or float64, or it will convert
		// []uint8 to string
		ScanMapDecode() ([]map[string]interface{}, error)

		// ScanMapDecodeClose returns the result in the form of []map[string]interface{}
		// If possible, it will convert []uint8 to int or float64, or it will convert
		// []uint8 to string. It will close the rows in the end.
		ScanMapDecodeClose() ([]map[string]interface{}, error)
	}

	scannerRows struct {
		*sql.Rows
	}

	commonSqlConn struct {
		cfg     Conf
		beginTx beginnable
		brk     breaker.Breaker
		accept  func(error) bool
	}

	statement struct {
		stmt *sql.Stmt
	}

	SqlOption func(*commonSqlConn)
)

func NewSqlConn(cfg Conf, opts ...SqlOption) SqlConn {
	conn := &commonSqlConn{
		cfg:     cfg,
		beginTx: begin,
		brk:     breaker.NewBreaker(),
	}
	for _, opt := range opts {
		opt(conn)
	}

	return conn
}

func (db *commonSqlConn) Transact(fn func(session Session) error) error {
	return db.brk.DoWithAcceptable(func() error {
		return transact(db, db.beginTx, fn)
	}, db.acceptable)
}

func (db *commonSqlConn) Prepare(query string) (stmt StmtSession, err error) {
	return db.PrepareContext(context.Background(), query)
}

func (db *commonSqlConn) PrepareContext(ctx context.Context, query string) (stmt StmtSession, err error) {
	err = db.brk.DoWithAcceptable(func() error {
		var conn *sql.DB
		conn, err = getSqlConn(db.cfg)
		if err != nil {
			logInstanceError(db.cfg, err)
			return err
		}

		if st, err := conn.PrepareContext(ctx, query); err != nil {
			return err
		} else {
			stmt = statement{
				stmt: st,
			}
			return nil
		}
	}, db.acceptable)

	return
}

func (db *commonSqlConn) ExecContext(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error) {
	err = db.brk.DoWithAcceptable(func() error {
		var conn *sql.DB
		conn, err = getSqlConn(db.cfg)
		if err != nil {
			logInstanceError(db.cfg, err)
			return err
		}

		result, err = exec(ctx, conn, query, args...)
		return err
	}, db.acceptable)

	return
}

func (db *commonSqlConn) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.ExecContext(context.Background(), query, args...)
}

func (db *commonSqlConn) QueryContext(ctx context.Context, q string, args ...interface{}) (result *sql.Rows, err error) {
	err = db.brk.DoWithAcceptable(func() error {
		var conn *sql.DB
		conn, err = getSqlConn(db.cfg)
		if err != nil {
			logInstanceError(db.cfg, err)
			return err
		}

		result, err = query(ctx, conn, q, args...)
		return err
	}, db.acceptable)

	return
}

func (db *commonSqlConn) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

func (db *commonSqlConn) QueryRowContext(ctx context.Context, query string, args ...interface{}) (row *sql.Row, err error) {
	err = db.brk.DoWithAcceptable(func() error {
		var conn *sql.DB
		conn, err = getSqlConn(db.cfg)
		if err != nil {
			logInstanceError(db.cfg, err)
			return err
		}

		row = queryRow(ctx, conn, query, args...)
		return err
	}, db.acceptable)

	return
}

func (db *commonSqlConn) QueryRow(query string, args ...interface{}) (row *sql.Row, err error) {
	return db.QueryRowContext(context.Background(), query, args...)
}

func (db *commonSqlConn) acceptable(err error) bool {
	ok := err == nil || err == sql.ErrNoRows || err == sql.ErrTxDone
	if db.accept == nil {
		return ok
	} else {
		return ok || db.accept(err)
	}
}

func (s statement) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	return execStmt(ctx, s.stmt, args...)
}

func (s statement) Exec(args ...interface{}) (sql.Result, error) {
	return s.ExecContext(context.Background(), args...)
}

func (s statement) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	return queryStmt(ctx, s.stmt, args...)
}

func (s statement) Query(args ...interface{}) (*sql.Rows, error) {
	return s.QueryContext(context.Background(), args...)
}

func (s statement) QueryRowContext(ctx context.Context, args ...interface{}) (row *sql.Row) {
	return queryStmtRow(ctx, s.stmt, args...)
}

func (s statement) QueryRow(args ...interface{}) (row *sql.Row) {
	return s.QueryRowContext(context.Background(), args...)
}

func (s scannerRows) ScanStruct(target interface{}) error {
	return scanner.Scan(s.Rows, target)
}

func (s scannerRows) ScanStructClose(target interface{}) error {
	return scanner.ScanClose(s.Rows, target)
}

func (s scannerRows) ScanMap() ([]map[string]interface{}, error) {
	return scanner.ScanMap(s.Rows)
}

func (s scannerRows) ScanMapClose() ([]map[string]interface{}, error) {
	return scanner.ScanMapClose(s.Rows)
}

func (s scannerRows) ScanMapDecode() ([]map[string]interface{}, error) {
	return scanner.ScanMapDecode(s.Rows)
}

func (s scannerRows) ScanMapDecodeClose() ([]map[string]interface{}, error) {
	return scanner.ScanMapDecodeClose(s.Rows)
}
