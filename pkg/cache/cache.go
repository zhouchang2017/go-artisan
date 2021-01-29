package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/patrickmn/go-cache"
	"reflect"
	"time"
)

type Cache struct {
	cacheKey    string        // default builder:"key"
	cachePrefix string        // default record name
	client      redis.Cmdable // 二级缓存
	local       *cache.Cache  // 一级缓存
	d           time.Duration // 一级缓存时间
	ld          time.Duration // 二级缓存时间
	entityType  reflect.Type
	unmarshal   func(result string) (interface{}, error)
	find        func(ctx context.Context, key interface{}) (interface{}, error)
}

func (p Cache) newEntity() interface{} {
	t := p.entityType
	if p.entityType.Kind() == reflect.Ptr {
		t = p.entityType.Elem()
	}
	value := reflect.New(t)
	return value.Elem().Interface()
}

func (p Cache) GetCacheKey(key interface{}) string {
	return fmt.Sprintf("%s%v", p.cachePrefix, key)
}

func (p *Cache) Set(ctx context.Context, key interface{}, data interface{}) error {
	k := p.GetCacheKey(key)
	if p.local != nil {
		p.local.Set(k, data, p.d)
	}
	if p.client != nil {
		bytes, err := json.Marshal(data)
		if err != nil {
			return err
		}
		if err := p.client.Set(ctx, k, string(bytes), p.ld).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (p *Cache) Get(ctx context.Context, key interface{}) (val interface{}, err error) {
	k := p.GetCacheKey(key)
	var exist bool
	if p.local != nil {
		val, exist = p.local.Get(k)
		if exist {
			return val, nil
		}
	}
	if p.client != nil {
		result, err := p.client.Get(ctx, k).Result()
		if err != nil {
			return nil, err
		}

		entity, err := p.unmarshal(result)
		if err != nil {
			return nil, err
		}
		if p.local != nil && !exist {
			// 写入一级缓存
			p.local.Set(k, entity, p.d)
		}
		return entity, nil

	}
	return nil, errors.New("not hit")
}

func (p *Cache) Delete(ctx context.Context, key interface{}) error {
	k := p.GetCacheKey(key)
	if p.local != nil {
		p.local.Delete(k)
	}
	if p.client != nil {
		_, err := p.client.Del(ctx, k).Result()
		if err != nil {
			return err
		}
	}
	return nil
}
