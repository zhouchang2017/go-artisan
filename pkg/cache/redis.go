package cache

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type redisStore struct {
	client      redis.Cmdable
	errNotFound error
}

func newRedisStore(client redis.Cmdable, errNotFound error) *redisStore {
	return &redisStore{
		client:      client,
		errNotFound: errNotFound,
	}
}

func (r redisStore) Get(ctx context.Context, key string, val interface{}) error {
	result, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			// set not
			return r.errNotFound
		}
		return err
	}
	if result == notFoundPlaceholder {
		return errPlaceholder
	}
	return json.UnmarshalFromString(result, val)
}

func (r redisStore) Set(ctx context.Context, key string, val interface{}, d time.Duration) error {
	data, err := json.MarshalToString(val)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, key, data, d).Err()
}

func (r redisStore) Del(ctx context.Context, key ...string) error {
	return r.client.Del(ctx, key...).Err()
}
