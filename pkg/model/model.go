package model

import (
	"context"
	"database/sql"
	"go-artisan/pkg/sqlx"

	"github.com/didi/gendry/builder"
	"github.com/didi/gendry/scanner"
)

func init() {
	scanner.SetTagName("db")
	scanner.ErrEmptyResult = sqlx.ErrNotFound
}

var (
	DefaultPerPage    uint = 15
	DefaultPrimaryKey      = "id"
)

type (

	// Init Model Options
	ModelOption func(*modelTx)

	ModelTX interface {
		sqlx.Session
		Find(ctx context.Context, entity interface{}, conditions map[string]interface{}, fields ...string) error
		Insert(ctx context.Context, data map[string]interface{}) (res sql.Result, err error)
		Inserts(ctx context.Context, data []map[string]interface{}) (res sql.Result, err error)
		Count(ctx context.Context, conditions map[string]interface{}) (count int64, err error)
		Delete(ctx context.Context, conditions map[string]interface{}) (res sql.Result, err error)
		Update(ctx context.Context, val map[string]interface{}, conditions map[string]interface{}) (res sql.Result, err error)
		Pagination(ctx context.Context, page int64, entity interface{}, conditions map[string]interface{}) (paginator *Paginator, err error)
		Table() string
	}

	Model interface {
		ModelTX
		TX(tx sqlx.Session) ModelTX
	}

	modelTx struct {
		sqlx.Session
		tableName string
		perPage   uint // 分页步长
	}

	model struct {
		db sqlx.SqlConn
		*modelTx
	}

	Paginator struct {
		Total       int64 `json:"total"`         // 总计条数
		PerPage     uint  `json:"per_page"`      // 每页的数据条数
		CurrentPage int64 `json:"current_page"`  // 当前页
		LastPage    int64 `json:"last_page"`     // 最后一页页码
		HasNextPage bool  `json:"has_next_page"` // 是否有下一页
	}
)

func SetPerPage(perPage uint) ModelOption {
	return func(tx *modelTx) {
		tx.perPage = perPage
	}
}

func NewModel(db sqlx.SqlConn, tableName string, opts ...ModelOption) Model {
	m := &model{
		db: db,
		modelTx: &modelTx{
			Session:   db,
			tableName: tableName,
			perPage:   DefaultPerPage,
		},
	}
	for _, opt := range opts {
		opt(m.modelTx)
	}
	return m
}

func (m model) TX(tx sqlx.Session) ModelTX {
	return &modelTx{
		Session:   tx,
		tableName: m.tableName,
		perPage:   m.perPage,
	}
}

func (m modelTx) Find(ctx context.Context, entity interface{}, conditions map[string]interface{}, fields ...string) error {
	cond, vals, err := builder.BuildSelect(m.tableName, conditions, fields)
	if err != nil {
		return err
	}

	rows, err := m.Session.QueryContext(ctx, cond, vals...)
	if err != nil {
		return err
	}
	return scanner.ScanClose(rows, entity)
}

func (m modelTx) Insert(ctx context.Context, data map[string]interface{}) (res sql.Result, err error) {
	return m.Inserts(ctx, []map[string]interface{}{data})
}

func (m modelTx) Inserts(ctx context.Context, data []map[string]interface{}) (res sql.Result, err error) {
	cond, vals, err := builder.BuildInsert(m.tableName, data)
	if err != nil {
		return nil, err
	}

	return m.Session.ExecContext(ctx, cond, vals...)
}

func (m modelTx) Count(ctx context.Context, conditions map[string]interface{}) (count int64, err error) {
	cond, vals, err := builder.BuildSelect(m.tableName, conditions, []string{"count(*)"})
	if err != nil {
		return count, err
	}

	rows, err := m.Session.QueryContext(ctx, cond, vals...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count = 0
	if rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}

func (m modelTx) Delete(ctx context.Context, conditions map[string]interface{}) (res sql.Result, err error) {
	cond, vals, err := builder.BuildDelete(m.tableName, conditions)
	if err != nil {
		return nil, err
	}

	return m.Session.ExecContext(ctx, cond, vals...)
}

func (m modelTx) Update(ctx context.Context, val map[string]interface{}, conditions map[string]interface{}) (res sql.Result, err error) {
	cond, vals, err := builder.BuildUpdate(m.tableName, conditions, val)
	if err != nil {
		return nil, err
	}

	return m.Session.ExecContext(ctx, cond, vals...)
}

func (m modelTx) Pagination(ctx context.Context, page int64, entity interface{}, conditions map[string]interface{}) (paginator *Paginator, err error) {
	if conditions == nil {
		conditions = make(map[string]interface{})
	}
	// expect _limit
	take := m.perPage
	if hasLimit, ok := conditions["_limit"]; ok {
		if isLimit, ok := hasLimit.([]uint); ok {
			if len(isLimit) == 2 && isLimit[1] > 0 {
				take = isLimit[1]
			}
		}
	}
	delete(conditions, "_limit")

	count, err := m.Count(ctx, conditions)
	if err != nil {
		return nil, err
	}

	defer func() {
		paginator = newPaginator(count, page, take)
	}()

	offset := (page - 1) * int64(take)

	if offset >= count {
		// 没结果
		return
	}

	conditions["_limit"] = []uint{uint(offset), take}

	err = m.Find(ctx, entity, conditions)
	if err != nil {
		return nil, err
	}
	return
}

func (m modelTx) Table() string {
	return m.tableName
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
