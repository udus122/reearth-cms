package exporters

import (
	"context"
	"io"
	"reflect"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/reearth/reearth-cms/server/pkg/asset"
	"github.com/reearth/reearth-cms/server/pkg/item"
	"github.com/reearth/reearth-cms/server/pkg/schema"
	"github.com/reearth/reearth-cms/server/pkg/value"
	"github.com/samber/lo"
)

// ParquetExporter handles Parquet format exports.
// Parquet keeps its metadata in a footer, so rows are written as batches arrive
// and the file is completed in EndExport.
type ParquetExporter struct {
	writer  io.Writer
	pw      *parquet.Writer
	columns []parquetColumn
}

// parquetColumn maps one schema field (or the item id) to a Parquet column.
type parquetColumn struct {
	name  string
	field *schema.Field
	node  parquet.Node
}

// NewParquetExporter creates a new Parquet exporter
func NewParquetExporter(w io.Writer) Exporter {
	return &ParquetExporter{
		writer: w,
	}
}

// ValidateRequest validates the export request
func (e *ParquetExporter) ValidateRequest(req *ExportRequest) error {
	if req.Schema.Schema() == nil {
		return ErrSchemaRequired
	}
	return nil
}

// Export performs the Parquet export
func (e *ParquetExporter) Export(ctx context.Context, req *ExportRequest, il item.List, al asset.List) error {
	if err := e.StartExport(ctx, req); err != nil {
		return err
	}
	if err := e.ProcessBatch(ctx, il, al); err != nil {
		return err
	}
	return e.EndExport(ctx, nil)
}

// StartExport initializes the streaming Parquet export
func (e *ParquetExporter) StartExport(ctx context.Context, req *ExportRequest) error {
	e.columns = ParquetColumns(&req.Schema)
	e.pw = parquet.NewWriter(e.writer, parquetSchema(e.columns))
	return nil
}

// ProcessBatch processes a batch of items for streaming Parquet export
func (e *ParquetExporter) ProcessBatch(ctx context.Context, il item.List, assets asset.List) error {
	if e.pw == nil {
		return ErrInvalidRequest
	}
	rows := lo.Map(il, func(itm *item.Item, _ int) parquet.Row {
		return parquetRow(itm, e.columns)
	})
	_, err := e.pw.WriteRows(rows)
	return err
}

// EndExport finalizes the streaming Parquet export
func (e *ParquetExporter) EndExport(ctx context.Context, extra map[string]any) error {
	if e.pw == nil {
		return ErrInvalidRequest
	}
	err := e.pw.Close()
	e.pw = nil
	e.columns = nil
	return err
}

// ParquetColumns returns the columns written for a schema: the item id, then every
// single-valued primitive or geometry field. Geometry is stored as GeoJSON text.
func ParquetColumns(sp *schema.Package) []parquetColumn {
	idCol := parquetColumn{name: "id", node: parquet.String()}
	fields := lo.FilterMap(parquetFields(sp), func(f *schema.Field, _ int) (parquetColumn, bool) {
		node, ok := parquetNode(f.Type())
		return parquetColumn{name: f.Key().String(), field: f, node: parquet.Optional(node)}, ok
	})
	return append([]parquetColumn{idCol}, fields...)
}

func parquetFields(sp *schema.Package) schema.FieldList {
	if sp == nil || sp.Schema() == nil {
		return nil
	}
	return lo.Filter(sp.Schema().Fields(), func(f *schema.Field, _ int) bool {
		return !f.Multiple() && (isFieldSupported(f) || f.IsGeometryField())
	})
}

func parquetNode(t value.Type) (parquet.Node, bool) {
	switch t {
	case value.TypeText, value.TypeTextArea, value.TypeRichText, value.TypeMarkdown,
		value.TypeSelect, value.TypeTag, value.TypeURL,
		value.TypeGeometryObject, value.TypeGeometryEditor:
		return parquet.String(), true
	case value.TypeInteger:
		return parquet.Int(64), true
	case value.TypeNumber:
		return parquet.Leaf(parquet.DoubleType), true
	case value.TypeBool, value.TypeCheckbox:
		return parquet.Leaf(parquet.BooleanType), true
	case value.TypeDateTime:
		return parquet.Timestamp(parquet.Millisecond), true
	default:
		return nil, false
	}
}

// parquetSchema keeps the column order of the CMS schema.
// parquet.Group would sort columns by name, so the schema is built from an ordered struct-like group.
func parquetSchema(cols []parquetColumn) *parquet.Schema {
	return parquet.NewSchema("item", orderedGroup(cols))
}

func parquetRow(itm *item.Item, cols []parquetColumn) parquet.Row {
	return lo.Map(cols, func(c parquetColumn, i int) parquet.Value {
		if c.field == nil {
			return parquet.ByteArrayValue([]byte(itm.ID().String())).Level(0, 0, i)
		}
		v, ok := parquetValue(itm.Field(c.field.ID()))
		if !ok {
			return parquet.NullValue().Level(0, 0, i)
		}
		return v.Level(0, 1, i)
	})
}

func parquetValue(f *item.Field) (parquet.Value, bool) {
	if f == nil || f.Value().First() == nil {
		return parquet.Value{}, false
	}
	vv := f.Value().First()
	switch vv.Type() {
	case value.TypeInteger:
		v, ok := vv.ValueInteger()
		return parquet.Int64Value(v), ok
	case value.TypeNumber:
		v, ok := vv.ValueNumber()
		return parquet.DoubleValue(v), ok
	case value.TypeBool, value.TypeCheckbox:
		v, ok := vv.ValueBool()
		return parquet.BooleanValue(v), ok
	case value.TypeDateTime:
		v, ok := vv.ValueDateTime()
		return parquet.Int64Value(v.UnixMilli()), ok
	case value.TypeGeometryObject, value.TypeGeometryEditor:
		var s string
		vv.Match(value.Match{
			GeometryObject: func(v value.String) { s = v },
			GeometryEditor: func(v value.String) { s = v },
		})
		return parquet.ByteArrayValue([]byte(s)), s != ""
	default:
		s := toCSVValue(vv)
		return parquet.ByteArrayValue([]byte(s)), s != ""
	}
}

// orderedGroup is a parquet.Group whose fields keep the given order.
// Rows are written as parquet.Row values, so Field.Value is never used.
type orderedGroup []parquetColumn

func (g orderedGroup) group() parquet.Group {
	return lo.SliceToMap(g, func(c parquetColumn) (string, parquet.Node) { return c.name, c.node })
}

func (g orderedGroup) ID() int                     { return 0 }
func (g orderedGroup) String() string              { return g.group().String() }
func (g orderedGroup) Type() parquet.Type          { return g.group().Type() }
func (g orderedGroup) Optional() bool              { return false }
func (g orderedGroup) Repeated() bool              { return false }
func (g orderedGroup) Required() bool              { return true }
func (g orderedGroup) Leaf() bool                  { return false }
func (g orderedGroup) Encoding() encoding.Encoding { return nil }
func (g orderedGroup) Compression() compress.Codec { return nil }
func (g orderedGroup) GoType() reflect.Type        { return g.group().GoType() }
func (g orderedGroup) Fields() []parquet.Field {
	return lo.Map(g, func(c parquetColumn, _ int) parquet.Field { return orderedField{Node: c.node, name: c.name} })
}

type orderedField struct {
	parquet.Node
	name string
}

func (f orderedField) Name() string                           { return f.name }
func (f orderedField) Value(base reflect.Value) reflect.Value { return reflect.Value{} }
