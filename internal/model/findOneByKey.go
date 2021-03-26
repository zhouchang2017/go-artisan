package model

import (
	"bytes"
	"text/template"
)

var findOneByKeySignatureTemplate, _ = template.New("findOneByKeySignature").Parse(`FindOneBy{{.fieldName}}(ctx context.Context, {{.arg}} {{.argType}}) ({{.structArg}} *{{.structType}}, err error)`)

func (m modelGenerate) getFindOneByKeySignature(field Field) ([]byte, error) {
	var buf bytes.Buffer
	err := findOneByKeySignatureTemplate.Execute(&buf, map[string]interface{}{
		"fieldName":  field.Name.ToCamel(),
		"arg":        field.ArgName(),
		"argType":    field.DataType,
		"structArg":  m.table.StructArgName(),
		"structType": m.table.StructName(),
	})
	return buf.Bytes(), err
}

var findOneByKeyTemplate, _ = template.New("findOneByKey").Parse(`func (m *{{.structName}}) FindOneBy{{.fieldName}}(ctx context.Context, {{.arg}} {{.argType}}) ({{.structArg}} *{{.structType}}, err error) {
	resp := &{{.structType}}{}
	{{if .withCache}}
	var primaryKey {{.primaryKeyDataType}}
	var found bool
	key:= m.{{.arg}}CachedKey({{.arg}})
	err = m.cache.Take(ctx, {{.keyer}}({{.arg}}), &primaryKey, func(ctx context.Context, i interface{}) error {
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

/*
func (s service) FindOneByKey(ctx context.Context, keyField string) (res *Project, err error) {
	res = &Project{}
	var primaryKey int64
	var found bool
	key := fmt.Sprintf("project#key#%v", keyField)

	err = s.store.Take(ctx, key, &primaryKey, func(ctx context.Context, i interface{}) error {
		rows, err := s.db.QueryContext(ctx, "select * from t_project where project_key = ? limit 1", keyField)
		if err != nil {
			return err
		}
		if err := scanner.ScanClose(rows, res); err != nil {
			return err
		}
		primaryKey = res.ID
		found = true

		return s.store.Set(ctx, fmt.Sprintf("project#id#%d", res.ID), res)
	})
	if err != nil {
		return nil, err
	}
	if found {
		return res, nil
	}

	return s.FindOne(ctx, primaryKey)
}
*/
func (m modelGenerate) getFindOneByKey() ([]byte, error) {
	var buf bytes.Buffer
	err := findOneByKeyTemplate.Execute(&buf, map[string]interface{}{
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
