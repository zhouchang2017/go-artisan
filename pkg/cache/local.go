package cache

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/patrickmn/go-cache"
)

type localStore struct {
	client      *cache.Cache
	errNotFound error
}

func newLocalStore(expiration, cleanupInterval time.Duration, errNotFount error) *localStore {
	return &localStore{
		client:      cache.New(expiration, cleanupInterval),
		errNotFound: errNotFount,
	}
}

func setInterfaceValue(dst, src interface{}) error {
	rx := reflect.ValueOf(dst)
	if rx.Kind() != reflect.Ptr {
		errors.New("dst type must be ptr")
	}
	if !rx.CanSet() {
		rx = rx.Elem()
	}
	s := reflect.ValueOf(src)
	if s.Kind() != rx.Kind() {
		s = s.Elem()
	}
	rx.Set(s)
	return nil
}

func (l localStore) Get(_ context.Context, key string, val interface{}) error {
	data, ok := l.client.Get(key)
	if ok {
		return setInterfaceValue(val, data)
	}
	if v, ok := val.(string); ok {
		if v == notFoundPlaceholder {
			return errPlaceholder
		}
	}
	return l.errNotFound
}

func (l localStore) Set(_ context.Context, key string, val interface{}, d time.Duration) error {
	l.client.Set(key, val, d)
	return nil
}

func (l localStore) Del(_ context.Context, key ...string) error {
	for _, k := range key {
		l.client.Delete(k)
	}
	return nil
}
