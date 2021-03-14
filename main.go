package main

import (
	"bytes"
	"encoding/json"
	"github.com/davecgh/go-spew/spew"
)

func main() {
	var num int
	bufferString := bytes.NewBufferString("8")

	err := json.Unmarshal(bufferString.Bytes(), &num)
	if err!=nil {
		panic(err)
	}
	spew.Dump(num)
}
