package model

import (
	"bytes"
	"text/template"
)

var findOneSignatureTemplate, _ = template.New("findOneSignature").Parse(`FindOne(ctx context.Context, {{.arg}} {{.argType}}) ({{.structArg}} *{{.structType}}, err error)`)

func (m modelGenerate) getFindOneSignature() ([]byte, error) {
	var buf bytes.Buffer
	err := findOneSignatureTemplate.Execute(&buf, map[string]interface{}{
		"arg":        m.keyField.ArgName(),
		"argType":    m.keyField.DataType,
		"structArg":  m.table.StructArgName(),
		"structType": m.table.StructName(),
	})
	return buf.Bytes(), err
}

var findOneTemplate, _ = template.New("findOne").Parse(`func (m *{{.structName}}) FindOne(ctx context.Context, {{.arg}} {{.argType}}) ({{.structArg}} *{{.structType}}, err error) {
	var resp {{.structType}}
	{{if .withCache}}
	err = m.cache.Take(ctx, {{.keyer}}({{.arg}}), &resp, func(ctx context.Context, i interface{}) error {
		return m.model.Find(ctx, i, map[string]interface{}{
			{{.field}}: {{.arg}},
			"_limit": []uint{1},
		})
	})
	{{else}}
	err = m.model.Find(ctx, i, map[string]interface{}{
		{{.field}}: {{.arg}},
		"_limit": []uint{1},
	})
	{{end}}

	switch err {
	case nil:
		return &resp, nil
	default:
		return nil, err
	}
}`)

func (m modelGenerate) getFindOne() ([]byte, error) {
	var buf bytes.Buffer
	err := findOneTemplate.Execute(&buf, map[string]interface{}{
		"withCache":  m.cacheable(),
		"structName": m.defaultModelName,
		"arg":        m.keyField.ArgName(),
		"argType":    m.keyField.DataType,
		"structArg":  m.table.StructArgName(),
		"structType": m.table.StructName(),
		"keyer":      m.keyField.GetKeyerFuncName(),
		"field":      "m." + modelKeyField,
	})
	return buf.Bytes(), err
}
