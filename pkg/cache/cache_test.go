package cache

import (
	"reflect"
	"testing"
	"time"
)

func TestCache_newEntity(t *testing.T) {
	type Student struct {
		Name string
		Age  int
	}

	var student Student
	var stu *Student
	var int1 int
	var int2 *int
	var string1 string
	var string2 *string
	var create time.Time
	var update *time.Time

	fields := []interface{}{
		student, stu, int1, int2, string1, string2, create, update,
	}

	for _, field := range fields {
		ty := reflect.TypeOf(field)
		t.Logf("[%v]\n Name: %s\nKind: %s\n\n", field, ty.Name(), ty.Kind())
	}
}
