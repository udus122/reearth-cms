package exporters

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/reearth/reearth-cms/server/pkg/id"
	"github.com/reearth/reearth-cms/server/pkg/item"
	"github.com/reearth/reearth-cms/server/pkg/schema"
	"github.com/reearth/reearth-cms/server/pkg/value"
	"github.com/reearth/reearthx/account/accountdomain"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParquetExporter(t *testing.T) {
	sid, mid, pid := id.NewSchemaID(), id.NewModelID(), id.NewProjectID()
	gst := schema.GeometryObjectSupportedTypeList{schema.GeometryObjectSupportedTypePoint}
	sfName := schema.NewField(schema.NewText(nil).TypeProperty()).NewID().Name("name").Key(id.NewKey("name")).MustBuild()
	sfPop := schema.NewField(schema.MustNewInteger(nil, nil).TypeProperty()).NewID().Name("population").Key(id.NewKey("population")).MustBuild()
	sfArea := schema.NewField(lo.Must(schema.NewNumber(nil, nil)).TypeProperty()).NewID().Name("area").Key(id.NewKey("area")).MustBuild()
	sfDate := schema.NewField(schema.NewDateTime().TypeProperty()).NewID().Name("updated").Key(id.NewKey("updated")).MustBuild()
	sfGeo := schema.NewField(schema.NewGeometryObject(gst).TypeProperty()).NewID().Name("location").Key(id.NewKey("location")).MustBuild()
	s := schema.New().ID(sid).Fields([]*schema.Field{sfName, sfPop, sfArea, sfDate, sfGeo}).Workspace(accountdomain.NewWorkspaceID()).Project(pid).MustBuild()
	sp := schema.NewPackage(s, nil, nil, nil)

	updated := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newItem := func(fields ...*item.Field) *item.Item {
		return item.New().NewID().Schema(sid).Project(pid).Model(mid).Thread(id.NewThreadID().Ref()).Fields(fields).Anonymous(true).MustBuild()
	}
	i1 := newItem(
		item.NewField(sfName.ID(), value.TypeText.Value("練馬区").AsMultiple(), nil),
		item.NewField(sfPop.ID(), value.TypeInteger.Value(int64(740000)).AsMultiple(), nil),
		item.NewField(sfArea.ID(), value.TypeNumber.Value(48.08).AsMultiple(), nil),
		item.NewField(sfDate.ID(), value.TypeDateTime.Value(updated).AsMultiple(), nil),
		item.NewField(sfGeo.ID(), value.TypeGeometryObject.Value(`{"type":"Point","coordinates":[139.65,35.73]}`).AsMultiple(), nil),
	)
	i2 := newItem(item.NewField(sfName.ID(), value.TypeText.Value("日野市").AsMultiple(), nil))

	buf := &bytes.Buffer{}
	e := NewParquetExporter(buf)
	req := &ExportRequest{Format: FormatParquet, Schema: *sp}
	ctx := context.Background()
	require.NoError(t, e.StartExport(ctx, req))
	require.NoError(t, e.ProcessBatch(ctx, item.List{i1}, nil))
	require.NoError(t, e.ProcessBatch(ctx, item.List{i2}, nil))
	require.NoError(t, e.EndExport(ctx, nil))

	if p := os.Getenv("PARQUET_EXPORT_OUT"); p != "" {
		require.NoError(t, os.WriteFile(p, buf.Bytes(), 0o644))
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	assert.Equal(t, int64(2), f.NumRows())
	assert.Equal(t, []string{"id", "name", "population", "area", "updated", "location"},
		func() []string {
			names := []string{}
			for _, c := range f.Schema().Columns() {
				names = append(names, c[0])
			}
			return names
		}())

	rows := make([]parquet.Row, 2)
	n, _ := f.RowGroups()[0].Rows().ReadRows(rows)
	require.Equal(t, 2, n)
	assert.Equal(t, i1.ID().String(), rows[0][0].String())
	assert.Equal(t, "練馬区", rows[0][1].String())
	assert.Equal(t, int64(740000), rows[0][2].Int64())
	assert.Equal(t, 48.08, rows[0][3].Double())
	assert.Equal(t, updated.UnixMilli(), rows[0][4].Int64())
	assert.Equal(t, `{"type":"Point","coordinates":[139.65,35.73]}`, rows[0][5].String())
	assert.True(t, rows[1][2].IsNull())
}
