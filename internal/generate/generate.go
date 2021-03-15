package generate

import (
	"path/filepath"

	"github.com/tal-tech/go-zero/tools/goctl/util"
	"github.com/tal-tech/go-zero/tools/goctl/util/console"
)

const (
	pwd             = "."
	createTableFlag = `(?m)^(?i)CREATE\s+TABLE` // ignore case
)

// Option defines a function with argument defaultGenerator
type Option func(generator *defaultGenerator)

type defaultGenerator struct {
	// source string
	dir string
	console.Console
	pkg string
}

// NewDefaultGenerator creates an instance for defaultGenerator
func NewDefaultGenerator(dir string, opt ...Option) (*defaultGenerator, error) {
	if dir == "" {
		dir = pwd
	}
	dirAbs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	dir = dirAbs
	pkg := filepath.Base(dirAbs)
	err = util.MkdirIfNotExist(dir)
	if err != nil {
		return nil, err
	}

	generator := &defaultGenerator{dir: dir, pkg: pkg}
	var optionList []Option
	optionList = append(optionList, newDefaultOption())
	optionList = append(optionList, opt...)
	for _, fn := range optionList {
		fn(generator)
	}

	return generator, nil
}

func newDefaultOption() Option {
	return func(generator *defaultGenerator) {
		generator.Console = console.NewColorConsole()
	}
}
