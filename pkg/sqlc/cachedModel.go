package sqlc

import (
	"context"
	"database/sql"
	"go-artisan/pkg/model"
	"go-artisan/pkg/sqlx"
	"time"

	"github.com/tal-tech/go-zero/core/stores/cache"
	"github.com/tal-tech/go-zero/core/syncx"
)

// see doc/sql-cache.md
const cacheSafeGapBetweenIndexAndPrimary = time.Second * 5

var (

	// ErrNotFound is an alias of sqlx.ErrNotFound.
	ErrNotFound = sqlx.ErrNotFound
	// can't use one SharedCalls per conn, because multiple conns may share the same cache key.
	exclusiveCalls = syncx.NewSharedCalls()
	stats          = cache.NewStat("sqlc")
)

type (
	Model struct {
		model.Model
		*core
	}

	modelTx struct {
		model.ModelTx
		*core
	}

	core struct {
		model.IModel
		cache cache.Cache
		keys
	}
)

func (c *Model) TX(tx *sql.Tx) *modelTx {
	return &modelTx{
		core: &core{
			IModel: c.Model.TX(tx),
			cache:  c.cache,
			keys:   c.keys,
		},
	}
}

type CachedModelOption func(model *Model)

// 自定义缓存键格式
func AddKey(key string, isPrimary bool, opt *KeyOption) CachedModelOption {
	return func(model *Model) {
		keyOptionWithDefault(key, model.Table(), opt)
		if isPrimary {
			if model.keys.primary != nil {
				// waring is setted primary key
			}
			// model.keys.
			model.keys.primary = opt
		} else {
			if model.keys.items == nil {
				model.keys.items = make(map[string]*KeyOption)
			}
			if _, ok := model.keys.items[key]; ok {
				// waring setted key
			}
			model.keys.items[key] = opt
		}
	}
}

func New(db model.Model, c cache.CacheConf, opts ...CachedModelOption) func(cacheOpts ...cache.Option) *Model {
	return func(cacheOpts ...cache.Option) *Model {
		c := &Model{
			Model: db,
			core: &core{
				IModel: db,
				cache:  cache.New(c, exclusiveCalls, stats, sql.ErrNoRows, cacheOpts...),
				keys:   keys{table: db.Table()},
			},
		}

		for _, opt := range opts {
			opt(c)
		}

		return c
	}
}

func (c core) FindByPrimaryKey(ctx context.Context, entity interface{}, val interface{}) error {
	opt := c.keys.getPrimaryOption()
	key := opt.Keyer(val)
	return c.cache.Take(entity, key, func(v interface{}) error {
		return opt.Query(ctx, c, v, val)
	})
}

func (c core) Exec(ctx context.Context, exec func(ctx context.Context, db model.IModel) (res sql.Result, err error), keys ...string) (sql.Result, error) {
	res, err := exec(ctx, c.IModel)
	if err != nil {
		return nil, err
	}

	if err := c.cache.Del(keys...); err != nil {
		return nil, err
	}

	return res, nil
}

func (c core) FindByKey(ctx context.Context, entity interface{}, fieldName string, val interface{}) error {
	opt := c.keys.getOption(fieldName)
	key := opt.Keyer(val)
	pOpt := c.keys.getPrimaryOption()
	var primaryKey interface{}
	var found bool

	if err := c.cache.TakeWithExpire(&primaryKey, key, func(v interface{}, expire time.Duration) (err error) {

		if err = opt.Query(ctx, c, v, val); err != nil {
			return err
		}

		primaryKey = pOpt.Value(v)
		found = true
		return c.cache.SetWithExpire(pOpt.Keyer(primaryKey), v, expire+cacheSafeGapBetweenIndexAndPrimary)
	}); err != nil {
		return err
	}

	if found {
		return nil
	}

	return c.cache.Take(entity, pOpt.Keyer(primaryKey), func(v interface{}) error {
		return pOpt.Query(ctx, c, entity, primaryKey)
	})
}

func (c core) Cache() cache.Cache {
	return c.cache
}
