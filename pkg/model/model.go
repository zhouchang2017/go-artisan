package model

import (
	"context"
	"database/sql"
	"errors"
	"go-artisan/pkg/sqlx"

	"github.com/didi/gendry/builder"
	"github.com/didi/gendry/scanner"
)

func init() {
	scanner.SetTagName("db")
	scanner.ErrEmptyResult = sqlx.ErrNotFound
}

var (
	defaultPerPage uint = 15
)

type (
	session interface {
		ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	}

	queryCall func(db session, table string) error
	execCall  func(db session, table string) (res sql.Result, err error)
	countCall func(db session, table string) (res int64, err error)

	core struct {
		db      session
		table   string
		perPage uint
	}

	modelTx struct {
		db *sql.Tx
		*core
	}

	Model struct {
		db *sql.DB
		*core
	}

	Option func(m *Model)

	Paginator struct {
		Total       int64 `json:"total"`         // 总计条数
		PerPage     uint  `json:"per_page"`      // 每页的数据条数
		CurrentPage int64 `json:"current_page"`  // 当前页
		LastPage    int64 `json:"last_page"`     // 最后一页页码
		HasNextPage bool  `json:"has_next_page"` // 是否有下一页
	}
)

// 设置分页步长
func SetPerPage(perPage uint) Option {
	return func(m *Model) {
		m.perPage = perPage
	}
}

// NewModel
func New(db *sql.DB, table string, opts ...Option) *Model {
	m := &Model{
		db: db,
		core: &core{
			db:      db,
			table:   table,
			perPage: defaultPerPage,
		},
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// 实例化事务对象
func (m Model) TX(tx *sql.Tx) *modelTx {
	return &modelTx{
		db: tx,
		core: &core{
			db:      tx,
			table:   m.table,
			perPage: m.perPage,
		},
	}
}

// return database handle
func (m Model) DB() *sql.DB {
	return m.db
}

// return database handle
func (m modelTx) DB() *sql.Tx {
	return m.db
}

// Find
func (c *core) Find(ctx context.Context, entity interface{}, conditions map[string]interface{}, fields ...string) error {
	cond, vals, err := builder.BuildSelect(c.table, conditions, fields)
	if err != nil {
		return err
	}

	rows, err := c.db.QueryContext(ctx, cond, vals...)
	if err != nil {
		return err
	}
	return scanner.ScanClose(rows, entity)
}

// Insert
func (c *core) Insert(ctx context.Context, data map[string]interface{}) (res sql.Result, err error) {
	return c.Inserts(ctx, data)
}

// Inserts
func (c *core) Inserts(ctx context.Context, data ...map[string]interface{}) (res sql.Result, err error) {
	if len(data) == 0 {
		return nil, errors.New("insert data is empty")
	}
	cond, vals, err := builder.BuildInsert(c.table, data)
	if err != nil {
		return nil, err
	}
	return c.db.ExecContext(ctx, cond, vals...)
}

// Count
func (c *core) Count(ctx context.Context, conditions map[string]interface{}) (res int64, err error) {
	cond, vals, err := builder.BuildSelect(c.table, conditions, []string{"count(*)"})
	if err != nil {
		return res, err
	}

	rows, err := c.db.QueryContext(ctx, cond, vals...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	res = 0
	if rows.Next() {
		err = rows.Scan(&res)
		if err != nil {
			return 0, err
		}
	}
	return res, nil
}

// Delete
func (c *core) Delete(ctx context.Context, conditions map[string]interface{}) (res sql.Result, err error) {
	cond, vals, err := builder.BuildDelete(c.table, conditions)
	if err != nil {
		return nil, err
	}
	return c.db.ExecContext(ctx, cond, vals...)
}

// Update
func (c *core) Update(ctx context.Context, val map[string]interface{}, conditions map[string]interface{}) (res sql.Result, err error) {
	cond, vals, err := builder.BuildUpdate(c.table, conditions, val)
	if err != nil {
		return nil, err
	}
	return c.db.ExecContext(ctx, cond, vals...)
}

// Pagination 分页
func (c *core) Pagination(ctx context.Context, page int64, perPage uint, entity interface{}, conditions map[string]interface{}) (paginator *Paginator, err error) {
	if conditions == nil {
		conditions = make(map[string]interface{})
	}
	// expect _limit

	if perPage == 0 {
		perPage = c.perPage
	}

	if hasLimit, ok := conditions["_limit"]; ok {
		if isLimit, ok := hasLimit.([]uint); ok {
			if len(isLimit) == 2 && isLimit[1] > 0 {
				perPage = isLimit[1]
			}
		}
	}
	delete(conditions, "_limit")

	count, err := c.Count(ctx, conditions)
	if err != nil {
		return nil, err
	}

	defer func() {
		paginator = newPaginator(count, page, perPage)
	}()

	offset := (page - 1) * int64(perPage)

	if offset >= count {
		// 没结果
		return
	}

	conditions["_limit"] = []uint{uint(offset), perPage}

	err = c.Find(ctx, entity, conditions)
	if err != nil {
		return nil, err
	}
	return
}

func (c *core) Table() string {
	return c.table
}

func newPaginator(count int64, page int64, perPage uint) *Paginator {
	paginator := &Paginator{
		Total:       count,
		PerPage:     perPage,
		CurrentPage: page,
		LastPage:    (count + int64(perPage) - 1) / int64(perPage),
	}
	paginator.HasNextPage = page+1 <= paginator.LastPage
	return paginator
}

// tools
func Transaction(db sqlx.SqlConn, fn func(session sqlx.Session) error) (err error) {
	return db.Transact(fn)
}
