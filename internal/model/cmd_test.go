package model

import (
	"os"
	"path"
	"path/filepath"
	"testing"
)

func Test_fromDDl(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	dir = path.Join(filepath.Dir(dir), "*.sql")
	fromDDl(dir, "", true)
}
