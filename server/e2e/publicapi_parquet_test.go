package e2e

import (
	"bytes"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gavv/httpexpect/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/reearth/reearth-cms/server/internal/app"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublicAPI_Model_Parquet(t *testing.T) {
	e, url := StartServerAndGetURL(t, &app.Config{}, true, publicAPISeeder)

	body := e.GET("/api/p/{workspace}/{project}/{model}.parquet", pApiW1Alias, pApiP1Alias, pApiP1M1Key).
		Expect().
		Status(http.StatusOK).
		HasContentType("application/vnd.apache.parquet").
		Body().Raw()

	f, err := parquet.OpenFile(bytes.NewReader([]byte(body)), int64(len(body)))
	require.NoError(t, err)
	assert.Equal(t, int64(4), f.NumRows())
	assert.Equal(t, "id", f.Schema().Columns()[0][0])
	assert.Equal(t, pApiP1S1F1Key, f.Schema().Columns()[1][0])

	// DuckDB reads the footer first with a Range request
	e.GET("/api/p/{workspace}/{project}/{model}.parquet", pApiW1Alias, pApiP1Alias, pApiP1M1Key).
		WithHeader("Range", "bytes=-8").
		Expect().
		Status(http.StatusPartialContent).
		Body().HasSuffix("PAR1")

	e.HEAD("/api/p/{workspace}/{project}/{model}.parquet", pApiW1Alias, pApiP1Alias, pApiP1M1Key).
		Expect().
		Status(http.StatusOK).
		Header("Accept-Ranges").IsEqual("bytes")

	// Set PUBLICAPI_DEMO_WAIT=60s to keep the server up and query it from DuckDB
	if d, err := time.ParseDuration(os.Getenv("PUBLICAPI_DEMO_WAIT")); err == nil {
		t.Logf("public api: %s/api/p/%s/%s", url, pApiW1Alias, pApiP1Alias)
		time.Sleep(d)
	}
}

func TestPublicAPI_Catalog(t *testing.T) {
	e, url := StartServerAndGetURL(t, &app.Config{}, true, publicAPISeeder)

	res := e.GET("/api/p/{workspace}/{project}/catalog.jsonld", pApiW1Alias, pApiP1Alias).
		Expect().
		Status(http.StatusOK).
		JSON(httpexpect.ContentOpts{MediaType: "application/ld+json"}).Object()

	res.Value("@type").IsEqual("dcat:Catalog")
	datasets := res.Value("dcat:dataset").Array()
	keys := []string{}
	for _, d := range datasets.Iter() {
		u := d.Object().Value("dcat:distribution").Array().Value(0).Object().Value("dcat:downloadURL").String().Raw()
		keys = append(keys, u)
	}
	assert.Contains(t, keys, url+"/api/p/"+pApiW1Alias+"/"+pApiP1Alias+"/"+pApiP1M1Key+".parquet")
}
