package builder

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	builder2 "github.com/didi/gendry/builder"
	"github.com/didi/gendry/scanner"
	"log"
	"strings"
)

var (
	DebugMode           = false
	DefaultPerPage uint = 15
	DefaultKeyName      = "id"
)

type IExecutor interface {
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	QueryRow(query string, args ...interface{}) *sql.Row
}

type Direction string

const (
	ASC  Direction = "ASC"
	DESC Direction = "DESC"
)

type where struct {
	condition map[string]interface{}
	or        []map[string]interface{}
	take      uint // limit
}

func (w *where) Reset() {
	w.condition = nil
	w.or = nil
	w.take = 0
}

func (w *where) Where(condition map[string]interface{}) *where {
	if w.condition == nil {
		w.condition = map[string]interface{}{}
	}
	for k, v := range condition {
		w.condition[k] = v
	}
	return w
}

func (w *where) OrWhere(condition ...map[string]interface{}) *where {
	if condition == nil {
		return w
	}
	if w.or == nil {
		w.or = make([]map[string]interface{}, 0, len(condition))
	}

	for _, cond := range condition {
		w.or = append(w.or, cond)
	}
	return w
}

func (w *where) WhereBetween(field string, val [2]interface{}) *where {
	if w.condition == nil {
		w.condition = make(map[string]interface{})
	}
	w.condition[fmt.Sprintf("%s BETWEEN", field)] = val
	return w
}

func (w *where) WhereNotBetween(field string, val [2]interface{}) *where {
	if w.condition == nil {
		w.condition = make(map[string]interface{})
	}
	w.condition[fmt.Sprintf("%s NOT BETWEEN", field)] = val
	return w
}

func (w *where) WhereIn(field string, val []interface{}) *where {
	if len(val) == 0 {
		return w
	}
	if w.condition == nil {
		w.condition = make(map[string]interface{})
	}
	w.condition[fmt.Sprintf("%s in", field)] = val
	return w
}

func (w *where) WhereNotIn(field string, val []interface{}) *where {
	if len(val) == 0 {
		return w
	}
	if w.condition == nil {
		w.condition = make(map[string]interface{})
	}
	w.condition[fmt.Sprintf("%s not in", field)] = val
	return w
}

func (w *where) WhereNull(field string) *where {
	if w.condition == nil {
		w.condition = make(map[string]interface{})
	}
	w.condition[field] = builder2.IsNull
	return w
}

func (w *where) WhereNotNull(field string) *where {
	if w.condition == nil {
		w.condition = make(map[string]interface{})
	}
	w.condition[field] = builder2.IsNotNull
	return w
}

func (w *where) WhereDay(field string, op string, day int) *where {

	if w.condition == nil {
		w.condition = make(map[string]interface{})
	}
	buffer := bytes.NewBufferString("DAY(")
	buffer.WriteString(field)
	buffer.WriteString(")")

	if op != "" && op != "=" {
		buffer.WriteByte(' ')
		buffer.WriteString(op)
	}

	w.condition[buffer.String()] = day
	return w
}

func (w *where) WhereMonth(field string, op string, month int) *where {

	if w.condition == nil {
		w.condition = make(map[string]interface{})
	}
	buffer := bytes.NewBufferString("MONTH(")
	buffer.WriteString(field)
	buffer.WriteString(")")

	if op != "" && op != "=" {
		buffer.WriteByte(' ')
		buffer.WriteString(op)
	}

	w.condition[buffer.String()] = month
	return w
}

func (w *where) WhereYear(field string, op string, year int) *where {

	if w.condition == nil {
		w.condition = make(map[string]interface{})
	}
	buffer := bytes.NewBufferString("YEAR(")
	buffer.WriteString(field)
	buffer.WriteString(")")
	if op != "" && op != "=" {
		buffer.WriteByte(' ')
		buffer.WriteString(op)
	}

	w.condition[buffer.String()] = year
	return w
}

func (w *where) WhereTime(field string, op string, t string) *where {

	if w.condition == nil {
		w.condition = make(map[string]interface{})
	}
	buffer := bytes.NewBufferString("TIME(")
	buffer.WriteString(field)
	buffer.WriteString(")")
	if op != "" && op != "=" {
		buffer.WriteByte(' ')
		buffer.WriteString(op)
	}

	w.condition[buffer.String()] = t
	return w
}

func (w *where) WhereDate(field string, op string, date string) *where {

	if w.condition == nil {
		w.condition = make(map[string]interface{})
	}
	buffer := bytes.NewBufferString("DATE(")
	buffer.WriteString(field)
	buffer.WriteString(")")

	if op != "" && op != "=" {
		buffer.WriteByte(' ')
		buffer.WriteString(op)
	}

	w.condition[buffer.String()] = date
	return w
}

func (w *where) Limit(n uint) *where {
	w.take = n
	return w
}

// QueryBuilder
type query struct {
	tableName string
	keyName   string
	w         where
	groupBy   string
	having    map[string]interface{}
	cols      []string
	offset    uint
	orders    map[string]Direction
	db        IExecutor
}

type QueryOption func(q *query)

func SetQueryKeyName(name string) QueryOption {
	return func(q *query) {
		q.keyName = strings.TrimSpace(name)
	}
}

func NewQuery(tableName string, db IExecutor, opts ...QueryOption) *query {
	q := &query{tableName: tableName, db: db}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

func (q *query) Reset() {
	q.w.Reset()
	q.groupBy = ""
	q.having = nil
	q.cols = []string{"*"}
	q.offset = 0
	q.orders = nil
}

func (q *query) KeyName() string {
	if q.keyName != "" {
		return q.keyName
	}
	return DefaultKeyName
}

func (q *query) Where(condition map[string]interface{}) *query {
	q.w.Where(condition)
	return q
}

func (q *query) WhereLike(field string, val string) *query {
	q.w.Where(map[string]interface{}{
		field + " like": val,
	})
	return q
}

func (q *query) OrWhere(condition ...map[string]interface{}) *query {
	q.w.OrWhere(condition...)
	return q
}

func (q *query) WhereBetween(field string, val [2]interface{}) *query {
	q.w.WhereBetween(field, val)
	return q
}

func (q *query) WhereNotBetween(field string, val [2]interface{}) *query {
	q.w.WhereNotBetween(field, val)
	return q
}

func (q *query) WhereIn(field string, val []interface{}) *query {
	q.w.WhereIn(field, val)
	return q
}

func (q *query) WhereNotIn(field string, val []interface{}) *query {
	q.w.WhereNotIn(field, val)
	return q
}

func (q *query) WhereNull(field string) *query {
	q.w.WhereNull(field)
	return q
}

func (q *query) WhereNotNull(field string) *query {
	q.w.WhereNotNull(field)
	return q
}

func (q *query) WhereDay(field string, op string, day int) *query {
	q.w.WhereDay(field, op, day)
	return q
}

func (q *query) WhereMonth(field string, op string, month int) *query {
	q.w.WhereMonth(field, op, month)
	return q
}

func (q *query) WhereYear(field string, op string, year int) *query {
	q.w.WhereYear(field, op, year)
	return q
}

func (q *query) WhereTime(field string, op string, t string) *query {
	q.w.WhereTime(field, op, t)
	return q
}

func (q *query) WhereDate(field string, op string, date string) *query {
	q.w.WhereDate(field, op, date)
	return q
}

func (q *query) Distinct() {

}

func (q *query) Select(fields ...string) *query {
	if len(fields) == 0 {
		fields = []string{"*"}
	}
	q.cols = fields
	return q
}

func (q *query) Take(n uint) *query {
	q.w.Limit(n)
	return q
}

func (q *query) Limit(n uint) *query {
	q.w.Limit(n)
	return q
}

func (q *query) Offset(n uint) *query {
	q.offset = n
	return q
}

func (q *query) Skip(n uint) *query {
	q.offset = n
	return q
}

func (q *query) limit() []uint {
	if q.w.take > 0 {
		return []uint{q.offset, q.w.take}
	}
	return []uint{}
}

func (q *query) OrderBy(field string, dir Direction) *query {
	if q.orders == nil {
		q.orders = map[string]Direction{}
	}
	q.orders[field] = dir
	return q
}

func (q *query) OrderByDesc(field string) *query {
	if q.orders == nil {
		q.orders = map[string]Direction{}
	}
	q.orders[field] = DESC
	return q
}

func (q *query) OrderByAsc(field string) *query {
	if q.orders == nil {
		q.orders = map[string]Direction{}
	}
	q.orders[field] = ASC
	return q
}

func (q *query) orderBy(condition map[string]interface{}) {
	if q.orders == nil {
		return
	}
	buf := &bytes.Buffer{}
	n := 0
	c := len(q.orders) - 1
	for k, v := range q.orders {
		buf.WriteString(k)
		buf.WriteString(" ")
		buf.WriteString(string(v))
		if n < c {
			buf.WriteByte(',')
		}
		n++
	}
	defer buf.Reset()
	orderBy := strings.TrimSpace(buf.String())
	if orderBy != "" {
		condition["_orderby"] = orderBy
	}
}

func (q *query) GroupBy(field string) *query {
	q.groupBy = strings.TrimSpace(field)
	return q
}

func (q *query) Having(condition map[string]interface{}) *query {
	if q.having == nil {
		q.having = make(map[string]interface{})
	}
	for k, v := range condition {
		q.having[k] = v
	}
	return q
}

func (q query) makeCondition(with map[string]interface{}) map[string]interface{} {
	condition := make(map[string]interface{})
	if with != nil {
		for k, v := range with {
			condition[k] = v
		}
	}
	if q.w.condition != nil {
		for k, v := range q.w.condition {
			condition[k] = v
		}
	}
	// with or
	if q.w.or != nil {
		condition["_or"] = q.w.or
	}
	// with having
	if q.having != nil {
		condition["_having"] = q.having
	}
	// with groupBy
	if q.groupBy != "" {
		condition["_groupby"] = q.groupBy
	}
	return condition
}

func (q *query) get(ctx context.Context, condition map[string]interface{}, entity interface{}) error {
	cond, values, err := builder2.BuildSelect(q.tableName, condition, q.cols)
	if err != nil {
		printErr(q.tableName, "get err: %s", err)
	}
	printSQL(q.tableName, cond, values)
	rows, err := q.db.QueryContext(ctx, cond, values...)
	if err != nil {
		printErr(q.tableName, "get query err: %s", err)
		return err
	}

	err = scanner.ScanClose(rows, entity)
	if err != nil {
		printErr(q.tableName, "get Scan err: %s", err)
	}
	return err
}

func (q *query) Get(ctx context.Context, entity interface{}) error {

	condition := map[string]interface{}{}

	if len(q.limit()) > 0 {
		condition["_limit"] = q.limit()
	}

	makeCondition := q.makeCondition(condition)

	q.orderBy(makeCondition)

	return q.get(ctx, makeCondition, entity)
}

func (u *query) Find(ctx context.Context, entity interface{}, key ...interface{}) error {
	switch len(key) {
	case 0:
		printErr(u.tableName, "Find err: invalid params key len must gt 0")
		return errors.New("find err: invalid params key len must gt 0")
	case 1:
		u.Where(map[string]interface{}{
			u.KeyName(): key[0],
		})
	default:
		u.WhereIn(u.KeyName(), key)
	}
	return u.Get(ctx, entity)
}

func (q *query) First(ctx context.Context, entity interface{}) error {
	condition := map[string]interface{}{}

	condition["_limit"] = []uint{0, 1}

	makeCondition := q.makeCondition(condition)

	q.orderBy(makeCondition)

	return q.get(ctx, makeCondition, entity)
}

func (q *query) Count(ctx context.Context) (count int64, err error) {
	cond, values, err := builder2.BuildSelect(q.tableName, q.makeCondition(nil), []string{"count(*)"})
	if err != nil {
		printErr(q.tableName, "Count Builder err: %s", err)
		return 0, err
	}

	printSQL(q.tableName, cond, values)

	rows, err := q.db.QueryContext(ctx, cond, values...)
	if err != nil {
		printErr(q.tableName, "Count query err: %s", err)
		return 0, err
	}
	defer rows.Close()

	count = 0
	if rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			printErr(q.tableName, "Count query rows.Scan err: %s", err)
			return 0, err
		}
	}
	return count, nil
}

type Paginator struct {
	Total       int64 `json:"total"`         // 总计条数
	PerPage     uint  `json:"per_page"`      // 每页的数据条数
	CurrentPage int64 `json:"current_page"`  // 当前页
	LastPage    int64 `json:"last_page"`     // 最后一页页码
	HasNextPage bool  `json:"has_next_page"` // 是否有下一页
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

// 分页查询
func (q *query) Pagination(
	ctx context.Context,
	page int64, entity interface{}) (paginator *Paginator, err error) {

	if q.w.take == 0 {
		q.Limit(DefaultPerPage)
	}

	count, err := q.Count(ctx)
	if err != nil {
		return nil, err
	}

	defer func() {
		paginator = newPaginator(count, page, q.w.take)
	}()

	offset := (page - 1) * int64(q.w.take)

	if offset >= count {
		// 没结果
		return
	}

	q.Offset(uint(offset))

	err = q.Get(ctx, entity)
	if err != nil {
		return nil, err
	}
	return
}

func (q *query) Exec(ctx context.Context, condition map[string]interface{}, cols []string, entity interface{}) error {
	if len(cols) == 0 {
		cols = []string{"*"}
	}
	cond, values, err := builder2.BuildSelect(q.tableName, condition, cols)
	if err != nil {
		printErr(q.tableName, "Exec Builder err: %s", err)
		return err
	}

	printSQL(q.tableName, cond, values)
	rows, err := q.db.QueryContext(ctx, cond, values...)
	if err != nil {
		printErr(q.tableName, "Exec query err: %s", err)
		return err
	}

	err = scanner.ScanClose(rows, entity)
	if err != nil {
		printErr(q.tableName, "Exec Scan err: %s", err)
	}
	return err

}

// executor
type executor struct {
	tableName string
	keyName   string
	w         where
	val       map[string]interface{}
	db        IExecutor
}

type ExecutorOption func(e *executor)

func SetExecutorKeyName(name string) ExecutorOption {
	return func(e *executor) {
		e.keyName = strings.TrimSpace(name)
	}
}

func NewExecutor(tableName string, db IExecutor, opts ...ExecutorOption) *executor {
	exec := &executor{
		tableName: tableName,
		db:        db,
	}
	for _, opt := range opts {
		opt(exec)
	}
	return exec
}

func (u *executor) KeyName() string {
	if u.keyName != "" {
		return u.keyName
	}
	return DefaultKeyName
}

func (u *executor) Where(condition map[string]interface{}) *executor {
	u.w.Where(condition)
	return u
}

func (u *executor) WhereLike(field string, val string) *executor {
	u.w.Where(map[string]interface{}{
		field + "like": val,
	})
	return u
}

func (u *executor) OrWhere(condition ...map[string]interface{}) *executor {
	u.w.OrWhere(condition...)
	return u
}

func (u *executor) WhereBetween(field string, val [2]interface{}) *executor {
	u.w.WhereBetween(field, val)
	return u
}

func (u *executor) WhereNotBetween(field string, val [2]interface{}) *executor {
	u.w.WhereNotBetween(field, val)
	return u
}

func (u *executor) WhereIn(field string, val []interface{}) *executor {
	u.w.WhereIn(field, val)
	return u
}

func (u *executor) WhereNotIn(field string, val []interface{}) *executor {
	u.w.WhereNotIn(field, val)
	return u
}

func (u *executor) WhereNull(field string) *executor {
	u.w.WhereNull(field)
	return u
}

func (u *executor) WhereNotNull(field string) *executor {
	u.w.WhereNotNull(field)
	return u
}

func (u *executor) WhereDay(field string, op string, day int) *executor {
	u.w.WhereDay(field, op, day)
	return u
}

func (u *executor) WhereMonth(field string, op string, month int) *executor {
	u.w.WhereMonth(field, op, month)
	return u
}

func (u *executor) WhereYear(field string, op string, year int) *executor {
	u.w.WhereYear(field, op, year)
	return u
}

func (u *executor) WhereTime(field string, op string, t string) *executor {
	u.w.WhereTime(field, op, t)
	return u
}

func (u *executor) WhereDate(field string, op string, date string) *executor {
	u.w.WhereDate(field, op, date)
	return u
}

func (u *executor) Increment(field string, n int) *executor {
	if u.val == nil {
		u.val = map[string]interface{}{}
	}
	u.val[field] = fmt.Sprintf("%s + %d", field, n)
	return u
}

func (u *executor) Decrement(field string, n int) *executor {
	if u.val == nil {
		u.val = map[string]interface{}{}
	}
	u.val[field] = fmt.Sprintf("%s + %d", field, n)
	return u
}

func (u *executor) Update(ctx context.Context, val map[string]interface{}) (num int64, err error) {
	if u.val == nil {
		u.val = map[string]interface{}{}
	}
	for k, v := range val {
		u.val[k] = v
	}
	cond, vals, err := builder2.BuildUpdate(u.tableName, u.makeCondition(nil), u.val)
	if err != nil {
		printErr(u.tableName, "Update build err: %s", err)
		return 0, err
	}
	printSQL(u.tableName, cond, vals)

	res, err := u.db.ExecContext(ctx, cond, vals...)
	if err != nil {
		printErr(u.tableName, "Update exec err: %s", err)
		return 0, err
	}
	return res.RowsAffected()
}

func (u *executor) Delete(ctx context.Context) (num int64, err error) {
	condition := u.makeCondition(nil)
	if len(condition) == 0 {
		return 0, errors.New("delete err: invalid params")
	}
	cond, vals, err := builder2.BuildDelete(u.tableName, condition)
	if err != nil {
		printErr(u.tableName, "Delete build err: %s", err)
		return 0, err
	}
	printSQL(u.tableName, cond, vals)
	res, err := u.db.ExecContext(ctx, cond, vals...)
	if err != nil {
		printErr(u.tableName, "Delete exec err: %s", err)
		return 0, err
	}
	return res.RowsAffected()
}

func (u *executor) DeleteByKey(ctx context.Context, key interface{}) (num int64, err error) {
	condition := map[string]interface{}{
		u.KeyName(): key,
	}
	cond, vals, err := builder2.BuildDelete(u.tableName, condition)
	if err != nil {
		printErr(u.tableName, "DeleteByKey build err: %s", err)
		return 0, err
	}
	printSQL(u.tableName, cond, vals)
	res, err := u.db.ExecContext(ctx, cond, vals...)
	if err != nil {
		printErr(u.tableName, "DeleteByKey exec err: %s", err)
		return 0, err
	}
	return res.RowsAffected()
}

func (u *executor) Insert(ctx context.Context, data ...map[string]interface{}) (id int64, err error) {
	cond, vals, err := builder2.BuildInsert(u.tableName, data)
	if err != nil {
		printErr(u.tableName, "Insert build err: %s", err)
		return 0, err
	}
	printSQL(u.tableName, cond, vals)
	res, err := u.db.ExecContext(ctx, cond, vals...)
	if err != nil {
		printErr(u.tableName, "Insert exec err: %s", err)
		return 0, err
	}
	return res.LastInsertId()
}

func (u executor) makeCondition(with map[string]interface{}) map[string]interface{} {
	condition := make(map[string]interface{})
	if with != nil {
		for k, v := range with {
			condition[k] = v
		}
	}
	if u.w.condition != nil {
		for k, v := range u.w.condition {
			condition[k] = v
		}
	}
	// with or
	if u.w.or != nil {
		condition["_or"] = u.w.or
	}

	return condition
}

func printErr(tableName string, format string, v ...interface{}) {
	if DebugMode {
		log.Printf("[DEBUG] ["+tableName+"] "+format+"\n", v...)
	}
}

func printSQL(tableName string, cond string, vals []interface{}) {
	if DebugMode {
		log.Printf("[DEBUG] [%s] [SQL]: %s | %v\n", tableName, cond, vals)
	}
}

type Dao struct {
	db          *sql.DB
	keyName     string
	tableName   string
	newQuery    func() *query
	newExecutor func() *executor
}

func NewDao(db *sql.DB, tableName string, keyName string) *Dao {
	dao := &Dao{
		db:        db,
		keyName:   keyName,
		tableName: tableName,
	}

	dao.newQuery = func() *query {
		return NewQuery(dao.tableName, dao.db, SetQueryKeyName(keyName))
	}

	dao.newExecutor = func() *executor {
		return NewExecutor(dao.tableName, dao.db, SetExecutorKeyName(keyName))
	}

	return dao
}

func (d *Dao) Tx(tx *sql.Tx) *Dao {
	dao := &Dao{
		db:        d.db,
		keyName:   d.keyName,
		tableName: d.tableName,
	}

	dao.newQuery = func() *query {
		return NewQuery(dao.tableName, tx, SetQueryKeyName(d.keyName))
	}

	dao.newExecutor = func() *executor {
		return NewExecutor(dao.tableName, tx, SetExecutorKeyName(d.keyName))
	}

	return dao
}

func (d *Dao) Insert(ctx context.Context, data ...map[string]interface{}) (int64, error) {
	return d.NewExecutor().Insert(ctx, data...)
}

func (d *Dao) NewQuery() *query {
	return d.newQuery()
}

func (d *Dao) NewExecutor() *executor {
	return d.newExecutor()
}

func (d *Dao) Table() string {
	return d.tableName
}

func (d *Dao) DB() IExecutor {
	return d.db
}

func (d *Dao) KeyName() string {
	return d.keyName
}

// tools
func Transaction(db *sql.DB, handle func(tx *sql.Tx) error) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()
	if err = handle(tx); err != nil {
		return err
	}
	return tx.Commit()
}
