package model

import (
	"errors"
	"go-artisan/internal/generate"
	"io/ioutil"
	"strings"

	"github.com/spf13/cobra"
	"github.com/tal-tech/go-zero/tools/goctl/model/sql/parser"
	"github.com/tal-tech/go-zero/tools/goctl/model/sql/util"
)

var errNotMatched = errors.New("sql not matched")

func Exec(cmd *cobra.Command, args []string) {

}

func fromDDl(src, dir string, cache bool) error {
	src = strings.TrimSpace(src)
	if len(src) == 0 {
		return errors.New("expected path or path globbing patterns, but nothing found")
	}

	files, err := util.MatchFiles(src)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return errNotMatched
	}

	var source []string
	for _, file := range files {
		data, err := ioutil.ReadFile(file)
		if err != nil {
			return err
		}

		source = append(source, string(data))
	}

	generator, err := generate.NewDefaultGenerator(dir)

	if err != nil {
		return err
	}

	startFromDDL(source, cache)
}

func startFromDDL(ddls []string, cache bool) {
	var tables []*parser.Table
	for _, ddl := range ddls {
		table, err := parser.Parse(ddl)
		if err != nil {
			// throw err
		}
		tables = append(tables, table)
	}

	// make buffer
	for _, table := range tables {

	}
}

func makeBuffer() {

}
