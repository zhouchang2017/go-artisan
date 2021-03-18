package cache

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/sync/singleflight"
)

const notFoundPlaceholder = "*"

// indicates there is no such value associate with the key
var (
	errPlaceholder = errors.New("placeholder")
	json           = jsoniter.ConfigCompatibleWithStandardLibrary

	defaultExpiration         = time.Minute * 60
	defaultNotFoundExpiration = time.Minute * 2
)

type (
	cmd interface {
		Get(ctx context.Context, key string, val interface{}) error
		Set(ctx context.Context, key string, val interface{}, d time.Duration) error
		Del(ctx context.Context, key ...string) error
	}

	Cache interface {
		Take(ctx context.Context, key string, val interface{}, query func(context.Context, interface{}) error) error
		Get(ctx context.Context, key string, val interface{}) error
		Set(ctx context.Context, key string, val interface{}) error
		Del(ctx context.Context, key ...string) error
	}

	store struct {
		cmds               []cmd
		g                  singleflight.Group
		errNotFound        error
		expiration         time.Duration
		notFoundExpiration time.Duration
	}
)

type Option struct {
	Redis              redis.Cmdable
	Local              bool
	Expiration         time.Duration
	NotFoundExpiration time.Duration
}

func New(opt Option, errNotFound error) Cache {
	var cmds []cmd
	if opt.Local {
		cmds = append(cmds, newLocalStore(5*time.Minute, 10*time.Minute, errNotFound))
	}
	if opt.Redis != nil {
		cmds = append(cmds, newRedisStore(opt.Redis, errNotFound))
	}
	if len(cmds) == 0 {
		cmds = append(cmds, newLocalStore(5*time.Minute, 10*time.Minute, errNotFound))
	}
	s := &store{
		cmds:               cmds,
		errNotFound:        errNotFound,
		expiration:         defaultExpiration,
		notFoundExpiration: defaultNotFoundExpiration,
	}
	if opt.Expiration > 0 {
		s.expiration = opt.Expiration
	}
	if opt.NotFoundExpiration > 0 {
		s.notFoundExpiration = opt.NotFoundExpiration
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
	for _, item := range c.cmds {
		if err := item.Set(ctx, key, val, c.expiration); err != nil {
			return err
		}
	}
	return nil
}

func (c *store) Del(ctx context.Context, key ...string) error {
	for _, item := range c.cmds {
		if err := item.Del(ctx, key...); err != nil {
			return err
		}
	}
	return nil
}

func (c *store) doGetCache(ctx context.Context, key string, val interface{}) (err error) {
	for _, item := range c.cmds {
		err = item.Get(ctx, key, val)
		if err != nil {
			switch err {
			case c.errNotFound:

			case errPlaceholder:

			}
			return err

		}
		return nil
	}
	return c.errNotFound
}

func (c *store) setCache(ctx context.Context, key string, val interface{}) error {
	for _, item := range c.cmds {
		if err := item.Set(ctx, key, val, c.expiration); err != nil {
			return err
		}
	}
	return nil
}

func (c *store) setCacheWithNotFound(ctx context.Context, key string) error {
	for _, item := range c.cmds {
		if err := item.Set(ctx, key, notFoundPlaceholder, c.notFoundExpiration); err != nil {
			return err
		}
	}
	return nil
}
