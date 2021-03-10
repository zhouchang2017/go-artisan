package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/tal-tech/go-zero/core/timex"
)

const slowThreshold = time.Millisecond * 500

func exec(ctx context.Context, conn sessionConn, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := format(query, args...)
	if err != nil {
		return nil, err
	}

	startTime := timex.Now()
	result, err := conn.ExecContext(ctx, query, args...)
	duration := timex.Since(startTime)
	if duration > slowThreshold {
		logWithDurationSlow(duration, "[SQL] exec: slowcall - %s", stmt)
	} else {
		logWithDurationInfo(duration, "sql exec: %s", stmt)
	}
	if err != nil {
		logSqlError(stmt, err)
	}

	return result, err
}

func execStmt(ctx context.Context, conn stmtConn, args ...interface{}) (sql.Result, error) {
	stmt := fmt.Sprint(args...)
	startTime := timex.Now()
	result, err := conn.ExecContext(ctx, args...)
	duration := timex.Since(startTime)
	if duration > slowThreshold {
		logWithDurationSlow(duration, "[SQL] execStmt: slowcall - %s", stmt)
	} else {
		logWithDurationInfo(duration, "sql execStmt: %s", stmt)
	}
	if err != nil {
		logSqlError(stmt, err)
	}

	return result, err
}

func query(ctx context.Context, conn sessionConn, q string, args ...interface{}) (res *sql.Rows, err error) {
	stmt, err := format(q, args...)
	if err != nil {
		return nil, err
	}

	startTime := timex.Now()
	rows, err := conn.QueryContext(ctx, q, args...)
	duration := timex.Since(startTime)
	if duration > slowThreshold {
		logWithDurationSlow(duration, "[SQL] query: slowcall - %s", stmt)
	} else {
		logWithDurationInfo(duration, "sql query: %s", stmt)
	}
	if err != nil {
		logSqlError(stmt, err)
		return nil, err
	}

	return rows, nil
}

func queryStmt(ctx context.Context, conn stmtConn, args ...interface{}) (res *sql.Rows, err error) {
	stmt := fmt.Sprint(args...)
	startTime := timex.Now()
	rows, err := conn.QueryContext(ctx, args...)
	duration := timex.Since(startTime)
	if duration > slowThreshold {
		logWithDurationSlow(duration, "[SQL] queryStmt: slowcall - %s", stmt)
	} else {
		logWithDurationInfo(duration, "sql queryStmt: %s", stmt)
	}
	if err != nil {
		logSqlError(stmt, err)
		return nil, err
	}

	return rows, nil
}

func queryRow(ctx context.Context, conn sessionConn, query string, args ...interface{}) (row *sql.Row) {
	stmt := fmt.Sprint(args...)
	startTime := timex.Now()
	row = conn.QueryRowContext(ctx, query, args...)
	duration := timex.Since(startTime)
	if duration > slowThreshold {
		logWithDurationSlow(duration, "[SQL] queryRow: slowcall - %s", stmt)
	} else {
		logWithDurationInfo(duration, "sql queryRow: %s", stmt)
	}
	return row
}

func queryStmtRow(ctx context.Context, conn stmtConn, args ...interface{}) (row *sql.Row) {
	stmt := fmt.Sprint(args...)
	startTime := timex.Now()
	row = conn.QueryRowContext(ctx, args...)
	duration := timex.Since(startTime)
	if duration > slowThreshold {
		logWithDurationSlow(duration, "[SQL] queryStmtRow: slowcall - %s", stmt)
	} else {
		logWithDurationInfo(duration, "sql queryStmtRow: %s", stmt)
	}
	return row
}
