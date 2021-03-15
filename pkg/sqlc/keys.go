package sqlc

import (
	"context"
	"fmt"
	"go-artisan/pkg/model"
	"reflect"
	"strings"
)

var (
	// 默认主键
	defaultPrimaryKey = "id"

	defaultCachedPrefix = "Cached"
	// 默认缓存键格式
	defaultCachedKeyFn = func(tableName, fieldName string, val interface{}) string {
		return fmt.Sprintf("%s#%s#%s#%v", defaultCachedPrefix, tableName, fieldName, val)
	}
	// 默认查询单条记录
	defaultQueryFn = func(key string) func(ctx context.Context, db model.IModel, entity interface{}, val interface{}) error {
		return func(ctx context.Context, db model.IModel, entity, val interface{}) error {
			return db.Find(ctx, entity, map[string]interface{}{
				key:      val,
				"_limit": []uint{1},
			})
		}
	}
)

type (
	keys struct {
		table   string
		primary *KeyOption
		items   map[string]*KeyOption
	}

	// map[string]*keyOption
	KeyOption struct {
		key   string
		Keyer func(val interface{}) string
		Query func(ctx context.Context, db model.IModel, entity interface{}, val interface{}) error
		Value func(entity interface{}) interface{}
	}
)

func getValue(entity interface{}, tag, key string) interface{} {
	typeOf := reflect.TypeOf(entity)
	valueOf := reflect.ValueOf(entity)
	for i := 0; i < typeOf.NumField(); i++ {
		lookup, ok := typeOf.Field(i).Tag.Lookup(tag)
		if ok {
			split := strings.Split(strings.TrimSpace(lookup), ",")
			for _, k := range split {
				if k == key {
					return valueOf.Field(i).Interface()
				}
			}
		}
	}
	return nil
}

func keyOptionWithDefault(key string, table string, opt *KeyOption) {
	opt.key = key

	if opt.Keyer == nil {
		opt.Keyer = func(val interface{}) string {
			return defaultCachedKeyFn(table, opt.key, val)
		}
	}
	if opt.Query == nil {
		opt.Query = func(ctx context.Context, db model.IModel, entity, val interface{}) error {
			return defaultQueryFn(opt.key)(ctx, db, entity, val)
		}
	}
	if opt.Value == nil {
		opt.Value = func(entity interface{}) interface{} {
			return getValue(entity, model.ScannerTag, key)
		}
	}
}

func (k keys) getOption(field string) *KeyOption {
	if opt, ok := k.items[field]; ok {
		return opt
	}
	opt := &KeyOption{}
	keyOptionWithDefault(field, k.table, opt)
	k.items[field] = opt
	return opt
}

func (k keys) getPrimaryOption() *KeyOption {
	if k.primary != nil {
		return k.primary
	}
	opt := &KeyOption{}
	keyOptionWithDefault(defaultPrimaryKey, k.table, opt)
	k.primary = opt
	return opt
}

func (k keys) exist(key string) (*KeyOption, bool) {
	for field, opt := range k.items {
		if field == key {
			return opt, true
		}
	}
	return nil, false
}

func (k keys) Keys(val interface{}) []string {
	res := make([]string, 0, len(k.items)+1)
	if k.primary != nil {
		res = append(res, k.primary.Keyer(val))
	}
	for _, opt := range k.items {
		res = append(res, opt.Keyer(val))
	}
	return res
}
