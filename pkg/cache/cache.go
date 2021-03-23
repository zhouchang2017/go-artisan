package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"github.com/patrickmn/go-cache"
	"github.com/tal-tech/go-zero/core/logx"
	"golang.org/x/sync/singleflight"
)

const (
	localStore = iota
	redisStore
)

// indicates there is no such value associate with the key
var (
	notFoundPlaceholder = []byte("*")
	errPlaceholder      = errors.New("placeholder")
	json                = jsoniter.ConfigCompatibleWithStandardLibrary

	defaultExpiration         = time.Minute * 60
	defaultNotFoundExpiration = time.Minute * 2

	stores = make(map[string]*store)
)

type (
	Cache interface {
		Take(ctx context.Context, key string, val interface{}, query func(context.Context, interface{}) error) error
		Get(ctx context.Context, key string, val interface{}) error
		Set(ctx context.Context, key string, val interface{}) error
		Del(ctx context.Context, key ...string) error
	}

	Option func(s *store)

	store struct {
		name               string
		mdb                *cache.Cache  // 一级缓存
		rdb                redis.Cmdable // 二级缓存
		enableMdb          bool          // default true
		g                  singleflight.Group
		errNotFound        error
		expiration         time.Duration
		notFoundExpiration time.Duration
	}
)

// 设置缓存时间
func SetExpiration(d time.Duration) Option {
	return func(s *store) {
		s.expiration = d
	}
}

// 设置NotFound缓存时间
func SetNotFoundExpiration(d time.Duration) Option {
	return func(s *store) {
		s.notFoundExpiration = d
	}
}

// 禁用本地缓存
func DisableLocalCache() Option {
	return func(s *store) {
		s.enableMdb = false
	}
}

func New(client redis.Cmdable, errNotFound error, opts ...Option) Cache {
	s := &store{
		enableMdb:          true,
		errNotFound:        errNotFound,
		expiration:         defaultExpiration,
		notFoundExpiration: defaultNotFoundExpiration,
	}
	for _, opt := range opts {
		opt(s)
	}

	s.mdb = cache.New(s.expiration, s.expiration*2)
	if client != nil {
		s.rdb = client
	}
	return s
}

func (c *store) Take(ctx context.Context, key string, val interface{}, query func(context.Context, interface{}) error) error {
	_, err, _ := c.g.Do(key, func() (interface{}, error) {
		if err := c.doGetCache(ctx, key, val); err != nil {
			switch err {
			case errPlaceholder:
				return nil, c.errNotFound
			case c.errNotFound:

				err := query(ctx, val)

				if err != nil {
					if err == c.errNotFound {
						if err2 := c.setCacheWithNotFound(ctx, key); err2 != nil {
							// set cache err
							return nil, err2
						}
						return nil, err
					}
					// db query err
					return nil, err
				}
				// rewrite cache
				if err := c.setCache(ctx, key, val); err != nil {
					return nil, err
				}
			default:
				return nil, err
			}
		}
		return nil, nil
	})
	return err
}

func (c *store) Get(ctx context.Context, key string, val interface{}) error {
	return c.doGetCache(ctx, key, val)
}

func (c *store) Set(ctx context.Context, key string, val interface{}) error {
	return c.setCache(ctx, key, val)
}

func (c *store) Del(ctx context.Context, key ...string) error {
	c.deleteLocalCache(key...)
	if c.rdb != nil {
		return c.rdb.Del(ctx, key...).Err()
	}
	return nil
}

func (c *store) deleteLocalCache(key ...string) {
	if c.enableMdb {
		for _, k := range key {
			c.mdb.Delete(k)
		}
	}
}

func (c *store) doGetCache(ctx context.Context, key string, val interface{}) (err error) {
	if c.enableMdb {
		value, exist := c.mdb.Get(key)
		if exist {
			if v, ok := value.([]byte); ok {
				return c.processCache(ctx, localStore, key, v, val)
			}
		}
	}

	if c.rdb != nil {
		data, err := c.rdb.Get(ctx, key).Bytes()
		if err != nil {
			if err == redis.Nil {
				return c.errNotFound
			}
			return err
		}
		return c.processCache(ctx, redisStore, key, data, val)
	}

	return c.errNotFound
}

func (c *store) setCache(ctx context.Context, key string, val interface{}) error {
	marshal, err := json.Marshal(val)
	if err != nil {
		return err
	}

	if c.enableMdb {
		c.mdb.Set(key, marshal, c.expiration)
	}

	if c.rdb != nil {
		return c.rdb.Set(ctx, key, marshal, c.expiration).Err()
	}
	return nil
}

func (c *store) processCache(ctx context.Context, storeType int, key string, data []byte, v interface{}) error {
	if bytes.Equal(data, notFoundPlaceholder) {
		return errPlaceholder
	}
	if bytes.Equal(data, []byte("")) {
		return c.errNotFound
	}

	err := json.Unmarshal(data, v)
	if err == nil {
		if storeType == redisStore {
			// local store not hit!
			if c.enableMdb {
				c.mdb.Set(key, data, c.expiration)
			}

			logx.Infof("hit cache from redis [%s]", key)
		} else {
			logx.Infof("hit cache from local [%s]", key)
		}
		return nil
	}

	report := fmt.Sprintf("unmarshal cache, key: %s, value: %s, error: %v", key, data, err)
	logx.Error(report)
	//stat.Report(report)

	if e := c.Del(ctx, key); e != nil {
		logx.Errorf("delete invalid cache, key: %s, value: %s, error: %v", key, data, e)
	}

	// returns errNotFound to reload the value by the given queryFn
	return c.errNotFound
}

func (c *store) setCacheWithNotFound(ctx context.Context, key string) error {
	if c.enableMdb {
		c.mdb.Set(key, notFoundPlaceholder, c.notFoundExpiration)
	}

	if c.rdb != nil {
		return c.rdb.Set(ctx, key, notFoundPlaceholder, c.notFoundExpiration).Err()
	}
	return nil
}
