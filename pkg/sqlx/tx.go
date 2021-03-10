package sqlx

import (
	"context"
	"database/sql"
	"fmt"
)

type (
	beginnable func(*sql.DB) (trans, error)

	trans interface {
		Session
		Commit() error
		Rollback() error
	}

	txSession struct {
		*sql.Tx
	}
)

func (t txSession) Prepare(query string) (StmtSession, error) {
	return t.PrepareContext(context.Background(), query)
}

func (t txSession) PrepareContext(ctx context.Context, query string) (StmtSession, error) {
	if stmt, err := t.Tx.PrepareContext(ctx, query); err != nil {
		return nil, err
	} else {
		return statement{
			stmt: stmt,
		}, nil
	}
}

func (t txSession) QueryContext(ctx context.Context, q string, args ...interface{}) (*sql.Rows, error) {
	return query(ctx, t.Tx, q, args...)
}

func (t txSession) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return t.QueryContext(context.Background(), query, args...)
}

func (t txSession) QueryRowContext(ctx context.Context, q string, args ...interface{}) (row *sql.Row, err error) {
	return queryRow(ctx, t.Tx, q, args...), nil
}

func (t txSession) QueryRow(query string, args ...interface{}) (row *sql.Row, err error) {
	return t.QueryRowContext(context.Background(), query, args...)
}

func begin(db *sql.DB) (trans, error) {
	if tx, err := db.Begin(); err != nil {
		return nil, err
	} else {
		return txSession{
			Tx: tx,
		}, nil
	}
}

func transact(db *commonSqlConn, b beginnable, fn func(Session) error) (err error) {
	conn, err := getSqlConn(db.cfg)
	if err != nil {
		logInstanceError(db.cfg, err)
		return err
	}

	return transactOnConn(conn, b, fn)
}

func transactOnConn(conn *sql.DB, b beginnable, fn func(Session) error) (err error) {
	var tx trans
	tx, err = b(conn)
	if err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			if e := tx.Rollback(); e != nil {
				err = fmt.Errorf("recover from %#v, rollback failed: %s", p, e)
			} else {
				err = fmt.Errorf("recoveer from %#v", p)
			}
		} else if err != nil {
			if e := tx.Rollback(); e != nil {
				err = fmt.Errorf("transaction failed: %s, rollback failed: %s", err, e)
			}
		} else {
			err = tx.Commit()
		}
	}()

	return fn(tx)
}
