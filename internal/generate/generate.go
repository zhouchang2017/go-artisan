package generate

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"

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

func (d defaultGenerator) NewWriter() *Writer {
	return NewWriter(d.pkg)
}

func NewWriter(pkgName string) *Writer {
	return &Writer{
		pkgName: pkgName,
		imports: make(map[string]interface{}),
	}
}

// 代码生成器
type Writer struct {
	Buf
	pkgName string
	imports map[string]interface{}
}

// frame bakes the built up source body into an formatted Go source file.
func (g *Writer) Frame() ([]byte, error) {
	if g.Len() == 0 {
		return nil, fmt.Errorf("empty bytes")
	}
	var buf Buf

	buf.P("package ", g.pkgName)
	buf.P()
	buf.P()
	if len(g.imports) > 0 {
		buf.P("import (")
		imps := make([]string, 0, len(g.imports))
		for path := range g.imports {
			imps = append(imps, path)
		}
		sort.Strings(imps)
		for _, path := range imps {
			// Omit the local package identifier if it matches the package name.
			fmt.Fprintf(&buf, "\t%q\n", path)
		}
		buf.P(")")
		buf.P()
	}

	buf.Write(g.Bytes())
	return buf.Bytes(), nil
	//source, err := format.Source(buf.Bytes())
	//if err != nil {
	//	return nil, err
	//}
	//return source, nil
}

func (g *Writer) AddImport(path string) {
	if g.imports == nil {
		g.imports = make(map[string]interface{})
	}
	g.imports[path] = new(interface{})
}

// ==================================================
// Buf
// ==================================================

type Buf struct {
	indent string
	bytes.Buffer
}

// printAtom prints the (atomic, non-annotation) argument to the generated output.
func (b *Buf) printAtom(v interface{}) {
	switch v := v.(type) {
	case string:
		b.WriteString(v)
	case *string:
		b.WriteString(*v)
	case bool:
		fmt.Fprint(b, v)
	case *bool:
		fmt.Fprint(b, *v)
	case int:
		fmt.Fprint(b, v)
	case *int32:
		fmt.Fprint(b, *v)
	case *int64:
		fmt.Fprint(b, *v)
	case float64:
		fmt.Fprint(b, v)
	case *float64:
		fmt.Fprint(b, *v)
	case []byte:
		b.Write(v)
	default:
		panic(fmt.Sprintf("unknown type in printer: %T", v))
	}
}

// In Indents the output one tab stop.
func (b *Buf) In() { b.indent += "\t" }

// Out unindents the output one tab stop.
func (b *Buf) Out() {
	if len(b.indent) > 0 {
		b.indent = b.indent[1:]
	}
}

// P prints the arguments to the generated output.
func (b *Buf) P(str ...interface{}) {
	b.WriteString(b.indent)
	for _, v := range str {
		b.printAtom(v)
	}
	b.WriteByte('\n')
}
