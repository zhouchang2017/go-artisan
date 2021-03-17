package cache

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"github.com/patrickmn/go-cache"
	"golang.org/x/sync/singleflight"
	"reflect"
)

type (
	store struct {
		client redis.Cmdable
		local  *cache.Cache
		g      singleflight.Group
	}
)

func (c *store) getFromLocal(entity interface{}, key string) bool {
	if c.local != nil {
		if value, ok := c.local.Get(key); ok {
			setInterfaceValue(entity, value)
			return true
		}
	}
	return false
}

func (c *store) Take(ctx context.Context, entity interface{}, key string) error {
	_, err, _ := c.g.Do(key, func() (i interface{}, err error) {
		if ok := c.getFromLocal(entity, key); ok {
			return nil, nil
		}
		bytes, err := c.client.Get(ctx, key).Bytes()
		if err != nil {

			return nil, err
		}
		if err := json.Unmarshal(bytes, entity); err != nil {
			return nil, err
		}
		return nil, nil
	})
}

func setInterfaceValue(dst, src interface{}) {
	rx := reflect.ValueOf(dst)
	if rx.Kind() != reflect.Ptr {
		panic("dst type must be ptr")
	}
	if !rx.CanSet() {
		rx = rx.Elem()
	}
	s := reflect.ValueOf(src)
	if s.Kind() != rx.Kind() {
		s = s.Elem()
	}

	rx.Set(s)
}
