package utils

import (
	"bytes"
	"unicode"
)

var Strings _strings

type _strings struct{}

// Snake 是将指定字符串转换为蛇形命名
// snakeCase -> snake_case
func (_strings) Snake(name string) string {
	buffer := bytes.NewBufferString("")
	for i, r := range name {
		if unicode.IsUpper(r) {
			if i != 0 {
				buffer.WriteByte('_')
			}
			buffer.WriteRune(unicode.ToLower(r))
		} else {
			buffer.WriteRune(r)
		}
	}
	return buffer.String()
}

// Camel 是将指定字符串转换为驼峰式命名
// foo_bar -> fooBar
func (_strings) Camel(name string) string {
	buffer := bytes.NewBufferString("")
	lastUpper := -1
	for i, r := range name {
		if unicode.IsUpper(r) {
			if i != 0 {
				if lastUpper == i-1 {
					buffer.WriteRune(unicode.ToLower(r))
				} else {
					buffer.WriteRune(r)
				}
			} else {
				buffer.WriteRune(unicode.ToLower(r))
			}
			lastUpper = i
		} else {
			buffer.WriteRune(r)
		}
	}
	return buffer.String()
}
