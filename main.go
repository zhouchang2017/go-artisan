package main

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/patrickmn/go-cache"
	"reflect"
	"time"
)

var c = cache.New(5*time.Minute, 10*time.Minute)

func main() {
	stuA := Student{
		Id:   100,
		Name: "zhangsan",
		Age:  18,
	}
	SetCache("zhangsan100", stuA)

	var stuB Student

	ok := GetCache(&stuB, "zhangsan100")
	if ok {
		spew.Dump(stuB)
	}
}

type Student struct {
	Id   int64
	Name string
	Age  int
}

func GetCache(entity interface{}, key string) bool {
	if value, ok := c.Get(key); ok {
		setter(entity, value)
		return true
	}
	return false
}

func SetCache(key string, value interface{}) {
	c.SetDefault(key, value)
}

func setter(entity interface{}, setter interface{}) {

	rx := reflect.ValueOf(entity)
	if rx.Kind() != reflect.Ptr {
		panic("entity type must be ptr")
	}
	if !rx.CanSet() {
		rx = rx.Elem()
	}
	src := reflect.ValueOf(setter)
	if src.Kind() != rx.Kind() {
		src = src.Elem()
	}

	rx.Set(src)
}
