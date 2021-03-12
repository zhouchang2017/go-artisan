package sqlc

import (
	"context"
	"database/sql"
	"fmt"
	"go-artisan/pkg/model"
	"go-artisan/pkg/sqlx"
	"time"

	"github.com/tal-tech/go-zero/core/stores/cache"
	"github.com/tal-tech/go-zero/core/syncx"
)

// see doc/sql-cache.md
const cacheSafeGapBetweenIndexAndPrimary = time.Second * 5

var (
	DefaultCachedPrefix = "Cached"
	// ErrNotFound is an alias of sqlx.ErrNotFound.
	ErrNotFound = sqlx.ErrNotFound
	// can't use one SharedCalls per conn, because multiple conns may share the same cache key.
	exclusiveCalls = syncx.NewSharedCalls()
	stats          = cache.NewStat("sqlc")

	// 默认缓存键格式
	defaultCachedKeyFn = func(tableName string) func(fieldName string, val interface{}) string {
		return func(fieldName string, val interface{}) string {
			return fmt.Sprintf("%s#%s#%s#%v", DefaultCachedPrefix, tableName, fieldName, val)
		}
	}
	// 默认查询记录
	defaultPrimaryQueryFn = func(key string) func(ctx context.Context, db model.ModelTX, entity interface{}, val interface{}) error {
		return func(ctx context.Context, db model.ModelTX, entity interface{}, val interface{}) error {
			return db.Find(ctx, entity, map[string]interface{}{
				key:      val,
				"_limit": []uint{1},
			})
		}
	}
	// 默认查询单条记录
	defaultIndexQueryFn IndexQueryFn = func(ctx context.Context, db model.ModelTX, entity interface{}, key string, val interface{}) error {
		return db.Find(ctx, entity, map[string]interface{}{
			key:      val,
			"_limit": []uint{1},
		})
	}
)

type (
	CachedKeyFn    func(fieldName string, val interface{}) string
	PrimaryQueryFn func(ctx context.Context, db model.ModelTX, entity interface{}, val interface{}) error
	IndexQueryFn   func(ctx context.Context, db model.ModelTX, entity interface{}, key string, val interface{}) error

	MODEL interface {
		GetID() int64 // mysql primary
	}

	CachedModelTX interface {
		model.ModelTX
		cache.Cache
		// 通过主键查询，默认走缓存
		FindByPrimaryKey(ctx context.Context, entity MODEL, val interface{}) error
		// 通过主键更新，默认删除缓存
		UpdateByPrimaryKey(ctx context.Context, val interface{}, value map[string]interface{}) (res sql.Result, err error)
		// 通过主键删除
		DeleteByPrimaryKey(ctx context.Context, val interface{}) (res sql.Result, err error)
		// 通过索引查询
		FindByKey(ctx context.Context, entity MODEL, fieldName string, val interface{}) error
	}

	CachedModel interface {
		CachedModelTX
		TX(tx sqlx.Session) CachedModelTX
	}

	cachedModelTX struct {
		model.ModelTX
		cache.Cache
		// 对应的主键
		primaryKey string
		// 缓存键拼接方法
		cachedKeyFn CachedKeyFn
		// 通过主键查询
		primaryQueryFn PrimaryQueryFn
		// 通过索引查询
		indexQueryFn IndexQueryFn
	}

	cachedModel struct {
		db model.Model
		*cachedModelTX
	}
)

func (c *cachedModel) TX(tx sqlx.Session) CachedModelTX {
	return &cachedModelTX{
		ModelTX:        c.db.TX(tx),
		Cache:          c.Cache,
		primaryKey:     c.primaryKey,
		cachedKeyFn:    c.cachedKeyFn,
		primaryQueryFn: c.primaryQueryFn,
		indexQueryFn:   c.indexQueryFn,
	}
}

type CachedModelOption func(model *cachedModel)

// 自定义缓存键格式
func SetCachedKeyFn(fn CachedKeyFn) CachedModelOption {
	return func(model *cachedModel) {
		model.cachedKeyFn = fn
	}
}

// 自定义主键查询方法
func SetPrimaryQueryFn(fn PrimaryQueryFn) CachedModelOption {
	return func(model *cachedModel) {
		model.primaryQueryFn = fn
	}
}

// 自定义索引查询方法
func SetIndexQueryFn(fn IndexQueryFn) CachedModelOption {
	return func(model *cachedModel) {
		model.indexQueryFn = fn
	}
}

// 自定义主键名称
func SetPrimaryKey(key string) CachedModelOption {
	return func(model *cachedModel) {
		model.primaryKey = key
	}
}

func NewCachedModel(db model.Model, c cache.CacheConf, opts ...CachedModelOption) func(cacheOpts ...cache.Option) CachedModel {
	return func(cacheOpts ...cache.Option) CachedModel {
		c := &cachedModel{
			db: db,
			cachedModelTX: &cachedModelTX{
				ModelTX:        db,
				Cache:          cache.New(c, exclusiveCalls, stats, sql.ErrNoRows, cacheOpts...),
				primaryKey:     model.DefaultPrimaryKey,
				cachedKeyFn:    nil,
				primaryQueryFn: nil,
				indexQueryFn:   nil,
			},
		}

		for _, opt := range opts {
			opt(c)
		}
		if c.cachedKeyFn == nil {
			c.cachedKeyFn = defaultCachedKeyFn(db.Table())
		}
		if c.primaryQueryFn == nil {
			c.primaryQueryFn = defaultPrimaryQueryFn(c.primaryKey)
		}
		if c.indexQueryFn == nil {
			c.indexQueryFn = defaultIndexQueryFn
		}

		return c
	}
}

func (c cachedModelTX) FindByPrimaryKey(ctx context.Context, entity MODEL, val interface{}) error {
	if c.Cache != nil {
		return c.Take(entity, c.cachedKeyFn(c.primaryKey, val), func(v interface{}) error {
			return c.primaryQueryFn(ctx, c.ModelTX, entity, val)
		})
	}
	return c.primaryQueryFn(ctx, c.ModelTX, entity, val)
}

func (c cachedModelTX) UpdateByPrimaryKey(ctx context.Context, val interface{}, value map[string]interface{}) (res sql.Result, err error) {
	res, err = c.Update(ctx, value, map[string]interface{}{
		c.primaryKey: val,
	})

	if err != nil {
		return nil, err
	}

	// del cache
	if c.Cache != nil {
		if err := c.Del(c.cachedKeyFn(c.primaryKey, val)); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (c cachedModelTX) DeleteByPrimaryKey(ctx context.Context, val interface{}) (res sql.Result, err error) {
	res, err = c.Delete(ctx, map[string]interface{}{
		c.primaryKey: val,
	})

	if err != nil {
		return nil, err
	}

	// del cache
	if c.Cache != nil {
		if err := c.Del(c.cachedKeyFn(c.primaryKey, val)); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (c cachedModelTX) FindByKey(ctx context.Context, entity MODEL, fieldName string, val interface{}) error {
	if c.Cache != nil {
		var primaryKey interface{}
		var found bool

		if err := c.TakeWithExpire(&primaryKey, c.cachedKeyFn(fieldName, val), func(v interface{}, expire time.Duration) (err error) {
			if err = c.indexQueryFn(ctx, c.ModelTX, entity, fieldName, val); err != nil {
				return err
			}
			primaryKey = entity.GetID()
			found = true
			return c.SetWithExpire(c.cachedKeyFn(c.primaryKey, primaryKey), entity, expire+cacheSafeGapBetweenIndexAndPrimary)
		}); err != nil {
			return err
		}

		if found {
			return nil
		}

		return c.Take(entity, c.cachedKeyFn(c.primaryKey, primaryKey), func(v interface{}) error {
			return c.primaryQueryFn(ctx, c.ModelTX, v, primaryKey)
		})
	}
	return c.indexQueryFn(ctx, c.ModelTX, entity, fieldName, val)
}

func (c cachedModelTX) Del(keys ...string) error {
	if c.Cache != nil {
		return c.Cache.Del(keys...)
	}
	return nil
}
func (c cachedModelTX) Get(key string, v interface{}) error {
	if c.Cache != nil {
		return c.Cache.Get(key, v)
	}
	return nil
}
func (c cachedModelTX) IsNotFound(err error) bool {
	if c.Cache != nil {
		return c.Cache.IsNotFound(err)
	}
	return true
}
func (c cachedModelTX) Set(key string, v interface{}) error {
	if c.Cache != nil {
		return c.Cache.Set(key, v)
	}
	return nil
}
func (c cachedModelTX) SetWithExpire(key string, v interface{}, expire time.Duration) error {
	if c.Cache != nil {
		c.Cache.SetWithExpire(key, v, expire)
	}
	return nil
}
func (c cachedModelTX) Take(v interface{}, key string, query func(v interface{}) error) error {
	if c.Cache != nil {
		c.Cache.Take(v, key, query)
	}
	return nil
}
func (c cachedModelTX) TakeWithExpire(v interface{}, key string, query func(v interface{}, expire time.Duration) error) error {
	if c.Cache != nil {
		return c.Cache.TakeWithExpire(v, key, query)
	}
	return nil
}
