package model

import (
	"bytes"
	"errors"
	"fmt"
	"go-artisan/internal/generate"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/tal-tech/go-zero/tools/goctl/model/sql/util"
)

var errNotMatched = errors.New("sql not matched")

func Exec(cmd *cobra.Command, args []string) {

}

type modelCachedKeyMethod struct {
	name   string
	fields []Field
}

type modelGenerate struct {
	table            Table
	cache            bool
	gen              *generate.Writer
	typesBuf         *generate.Buf
	interfaceBuf     *generate.Buf
	methodsBuf       *generate.Buf
	defaultModelName string
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

	generator, err := generate.NewDefaultGenerator(dir)

	// var tables []*Table
	for _, file := range files {
		data, err := ioutil.ReadFile(file)
		if err != nil {
			return err
		}

		table, err := Parse(string(data))
		if err != nil {
			return err
		}

		gen := modelGenerate{
			table:            *table,
			cache:            cache,
			gen:              generator.NewWriter(),
			typesBuf:         &generate.Buf{},
			interfaceBuf:     &generate.Buf{},
			methodsBuf:       &generate.Buf{},
			defaultModelName: "default" + table.StructName(),
		}
		model, err := gen.genModel()
		if err != nil {
			panic(err)
		}
		fmt.Fprint(os.Stdout, string(model))

	}
	panic(1)
}

func (m modelGenerate) genModel() ([]byte, error) {
	if m.table.ContainsTime() {
		m.gen.AddImport("time")
	}

	var modelBuf generate.Buf

	m.gen.AddImport("go-artisan/pkg/model")
	modelBuf.In()
	modelBuf.P(m.defaultModelName, " struct {")
	modelBuf.In()
	modelBuf.P("model model.Model")
	if m.cache {
		// import
		m.gen.AddImport("go-artisan/pkg/cache")
		modelBuf.P("cache cache.Cache")
	}
	modelBuf.P("table string")
	modelBuf.Out()
	modelBuf.P("}")
	modelBuf.Out()

	m.genMethods()
	// merge
	if m.interfaceBuf.Len() > 0 || modelBuf.Len() > 0 {
		m.gen.P()
		m.gen.P("type (")
		// inject interface
		if m.interfaceBuf.Len() > 0 {
			m.gen.P(m.interfaceBuf.Bytes())
			m.gen.P()
		}
		// inject model
		if modelBuf.Len() > 0 {
			m.gen.P(modelBuf.Bytes())
			m.gen.P()
		}

		// Model Struct
		m.gen.P(m.genModelStruct())
		m.gen.P(")")
	}
	m.gen.P(m.methodsBuf.Bytes())
	m.gen.P(m.genCachedKeys())
	return m.gen.Frame()

}

func (m modelGenerate) genMethods() {
	m.gen.AddImport("context")

	defer func() {
		m.interfaceBuf.Out()
		m.interfaceBuf.P("}")
	}()
	m.interfaceBuf.In()
	m.interfaceBuf.P(m.table.StructName(), "Model interface {")
	m.interfaceBuf.In()

	// Insert(ctx context.Context, task *Task) (res sql.Result, err error)
	m.genMethodInsert()
	// FindOne(ctx context.Context, intId int64) (*Model, error)
	m.genMethodFindOne()
	// FindOneBy{{Key}}(ctx context.Context, key type) (*Model, error)
	for _, field := range m.table.UniqueIndex {
		m.genMethodFindOneByKeys(field)
	}
	// Update(data *Model) error
	m.genMethodUpdate()
	// Delete(intId int64) error

}

func (m modelGenerate) genModelStruct() []byte {
	var buf generate.Buf
	buf.In()
	buf.P(m.table.StructName(), " struct {")
	buf.In()
	for _, field := range m.table.Fields {
		tag := fmt.Sprintf(" `db:%s`", strconv.Quote(field.Name.Source()))
		if field.Comment != "" {
			buf.P(field.Name.ToCamel(), " ", field.DataType, tag, " // ", field.Comment)
		} else {
			buf.P(field.Name.ToCamel(), " ", field.DataType, tag)
		}

	}
	buf.Out()
	buf.P("}")
	buf.Out()
	defer buf.Reset()
	return buf.Bytes()
}

func (m modelGenerate) genMethodInsert() {
	funcName := "Insert"
	funcSignature := fmt.Sprintf("%s(ctx context.Context, %s *%s) (res sql.Result, err error)",
		funcName,
		m.table.StructArgName(),
		m.table.StructName())

	m.interfaceBuf.P("// ", funcName, " 插入新纪录")
	m.interfaceBuf.P(funcSignature)

	m.methodsBuf.P("func (m *", m.defaultModelName, ") ", funcSignature, " {")
	m.methodsBuf.In()
	m.methodsBuf.P("res, err = m.model.Insert(ctx, map[string]interface{}{")
	m.methodsBuf.In()
	for _, field := range m.table.Fields {
		if field.IsPrimaryKey() {
			continue
		}
		m.methodsBuf.P(strconv.Quote(field.Name.Source()), ":", m.table.StructArgName(), ".", field.Name.ToCamel(), ",")
	}
	m.methodsBuf.Out()
	m.methodsBuf.P("})")

	m.methodsBuf.P("if err != nil {")
	m.methodsBuf.In()
	m.methodsBuf.P("return nil, err")
	m.methodsBuf.Out()
	m.methodsBuf.P("}")
	if m.cache {
		if m.table.PrimaryKey != nil {
			m.methodsBuf.P("id, _ := res.LastInsertId()")
			m.methodsBuf.P("return m.cache.Del(ctx, m.primaryCachedKey(id))")
		}
	}
	m.methodsBuf.Out()
	m.methodsBuf.P("}")
}

func (m modelGenerate) genMethodFindOne() {
	funcName := "FindOne"
	if m.table.PrimaryKey != nil {
		funcSignature := fmt.Sprintf("%s(ctx context.Context, %s %s) (%s *%s, err error)",
			funcName,
			m.table.PrimaryKey.ArgName(),
			m.table.PrimaryKey.DataType,
			m.table.StructArgName(),
			m.table.StructName())

		m.interfaceBuf.P("// ", funcName, " 通过主键查询", m.table.StructName())
		m.interfaceBuf.P(funcSignature)

		m.methodsBuf.P("func (m *", m.defaultModelName, ") ", funcSignature, " {")
		m.methodsBuf.In()

		m.methodsBuf.P("var resp ", m.table.StructName())
		if m.cache {
			m.methodsBuf.P("err := m.Model.FindByPrimaryKey(ctx, &resp, ", m.table.PrimaryKey.ArgName(), ")")
		} else {
			m.methodsBuf.P("err:= m.Model.Find(ctx, &resp, map[string]interface{}{")
			m.methodsBuf.In()
			m.methodsBuf.P(m.table.PrimaryKey.Name.Source(), " : ", m.table.PrimaryKey.ArgName(), ",")
			m.methodsBuf.P("_limit : []uint{0},")
			m.methodsBuf.Out()
			m.methodsBuf.P("})")
		}
		m.methodsBuf.P("switch err {")

		m.methodsBuf.P("case nil:")
		m.methodsBuf.In()
		m.methodsBuf.P("return &resp, nil")
		m.methodsBuf.Out()

		m.methodsBuf.P("sqlc.ErrNotFound:")
		m.methodsBuf.In()
		m.methodsBuf.P("return nil, ErrNotFound")
		m.methodsBuf.Out()

		m.methodsBuf.P("default:")
		m.methodsBuf.In()
		m.methodsBuf.P("return nil, err")
		m.methodsBuf.Out()

		m.methodsBuf.P("}")

		m.methodsBuf.Out()
		m.methodsBuf.P("}")
	}
}

func (m modelGenerate) genMethodFindOneByKeys(fields []*Field) {
	var (
		funcName bytes.Buffer
		args     bytes.Buffer
	)
	funcName.WriteString("FindOneBy")
	for index, field := range fields {
		funcName.WriteString(field.Name.ToCamel())
		args.WriteString(field.ArgName())
		args.WriteString(" ")
		args.WriteString(field.DataType)
		if index < len(fields)-1 {
			funcName.WriteString("And")
			args.WriteByte(',')
		}
	}

	funcSignature := fmt.Sprintf("%s(ctx context.Context, %s) (%s *%s, err error)",
		funcName.String(),
		args.String(),
		m.table.StructArgName(),
		m.table.StructName())

	m.interfaceBuf.P("// ", funcName.String(), " 通过key查询", m.table.StructName())
	m.interfaceBuf.P(funcSignature)

	m.methodsBuf.P("func (m *", m.defaultModelName, ") ", funcSignature, " {")
	m.methodsBuf.In()

	m.methodsBuf.P("var resp ", m.table.StructName())
	if m.cache {
		m.methodsBuf.P("err := m.Model.FindByKey(ctx, &resp, map[string]interface{}{")
		m.methodsBuf.In()
		for _, field := range fields {
			m.methodsBuf.P(field.Name.Source(), " : ", field.ArgName(), ",")
		}
		m.methodsBuf.Out()
		m.methodsBuf.P("})")
	} else {
		m.methodsBuf.P("err:= m.Model.Find(ctx, &resp, map[string]interface{}{")
		m.methodsBuf.In()
		for _, field := range fields {
			m.methodsBuf.P(field.Name.Source(), " : ", field.ArgName(), ",")
		}
		m.methodsBuf.P("_limit : []uint{0},")
		m.methodsBuf.Out()
		m.methodsBuf.P("})")
	}
	m.methodsBuf.P("switch err {")

	m.methodsBuf.P("case nil:")
	m.methodsBuf.In()
	m.methodsBuf.P("return &resp, nil")
	m.methodsBuf.Out()

	m.methodsBuf.P("sqlc.ErrNotFound:")
	m.methodsBuf.In()
	m.methodsBuf.P("return nil, ErrNotFound")
	m.methodsBuf.Out()

	m.methodsBuf.P("default:")
	m.methodsBuf.In()
	m.methodsBuf.P("return nil, err")
	m.methodsBuf.Out()

	m.methodsBuf.P("}")

	m.methodsBuf.Out()
	m.methodsBuf.P("}")
}

func (m modelGenerate) genMethodUpdate() {
	funcName := "Update"
	funcSignature := fmt.Sprintf("%s(ctx context.Context, %s *%s) (err error)",
		funcName,
		m.table.StructArgName(),
		m.table.StructName())

	m.interfaceBuf.P("// ", funcName, " 更新纪录")
	m.interfaceBuf.P(funcSignature)

	m.methodsBuf.P("func (m *", m.defaultModelName, ") ", funcSignature, " {")
	m.methodsBuf.In()
	if m.cache {
		m.methodsBuf.P("return m.Model.Exec(ctx, func(ctx context.Context, db model.IModel) (res sql.Result, err error) {")
		m.methodsBuf.In()
		m.methodsBuf.P("return db.Update(ctx, map[string]interface{}{")

		m.methodsBuf.In()
		for _, field := range m.table.Fields {
			if field.IsPrimaryKey() {
				continue
			}
			m.methodsBuf.P(field.Name.Source(), ":", m.table.StructArgName(), ".", field.Name.ToCamel(), ",")
		}
		m.methodsBuf.Out()

		m.methodsBuf.P("}, map[string]interface{}{")
		m.methodsBuf.In()
		m.methodsBuf.P(m.table.PrimaryKey.Name.Source(), " : ", m.table.PrimaryKey.ArgName(), ",")
		m.methodsBuf.Out()
		m.methodsBuf.P("})")
		m.methodsBuf.Out()
		if m.table.PrimaryKey != nil {
			m.methodsBuf.P("}, m.CachedPrimaryKey(", m.table.StructArgName(), ".", m.table.PrimaryKey.Name.ToCamel(), "))")
		} else {
			m.methodsBuf.P("})")
		}

	} else {
		m.methodsBuf.P("return m.Model.Insert(ctx, map[string]interface{}{")

		m.methodsBuf.In()
		for _, field := range m.table.Fields {
			if field.IsPrimaryKey() {
				continue
			}
			m.methodsBuf.P(field.Name.Source(), ":", m.table.StructArgName(), ".", field.Name.ToCamel(), ",")
		}
		m.methodsBuf.Out()

		m.methodsBuf.P("})")
	}
	m.methodsBuf.Out()
	m.methodsBuf.P("}")
}

func (m modelGenerate) genMethodDelete() {

}

func (m modelGenerate) genCachedKeys() []byte {
	m.gen.AddImport("fmt")
	var buf generate.Buf
	if m.table.PrimaryKey != nil {
		buf.P("func (m ", m.defaultModelName, ") primaryCachedKey(", m.table.PrimaryKey.ArgName(), " interface{}) string {")
		buf.In()
		buf.P("return fmt.Sprintf(\"cached#%s#", m.table.PrimaryKey.ArgName(), "#%v\", m.table,", m.table.PrimaryKey.ArgName(), ")")
		buf.Out()
		buf.P("}")
	}

	for _, fields := range m.table.UniqueIndex {
		var (
			funcName bytes.Buffer
			args     bytes.Buffer
			ss       bytes.Buffer
		)
		for index, field := range fields {
			funcName.WriteString(field.Name.ToCamel())
			ss.WriteString(field.ArgName())
			ss.WriteString("#")
			args.WriteString(field.ArgName())
			args.WriteString(" ")
			args.WriteString(field.DataType)
			if index < len(fields)-1 {
				funcName.WriteString("And")
				args.WriteByte(',')
			}
		}
		funcName.WriteString("CachedKey")

		buf.P("func (m ", m.defaultModelName, ")", funcName.String(), "(", args.String(), ") string {")
		buf.In()

		buf.P("return fmt.Sprintf(\"cached#%s#", ss.String(), "#%v\", m.table,", m.table.PrimaryKey.ArgName(), ")")
		buf.Out()
		buf.P("}")

	}

	return buf.Bytes()
}
