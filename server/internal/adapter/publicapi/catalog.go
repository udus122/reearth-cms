package publicapi

import (
	"context"
	"time"

	"github.com/reearth/reearth-cms/server/internal/adapter"
	"github.com/reearth/reearth-cms/server/pkg/model"
	"github.com/reearth/reearthx/usecasex"
	"github.com/samber/lo"
)

// Catalog is a DCAT catalog of the public models of a project, written as JSON-LD.
// Harvesters (data portals, federated catalogs) read this one document to find
// every public model and where to download it.
type Catalog struct {
	Context     map[string]string `json:"@context"`
	Type        string            `json:"@type"`
	Title       string            `json:"dct:title"`
	Description string            `json:"dct:description,omitempty"`
	License     string            `json:"dct:license,omitempty"`
	Modified    time.Time         `json:"dct:modified"`
	Datasets    []CatalogDataset  `json:"dcat:dataset"`
}

type CatalogDataset struct {
	Type          string                `json:"@type"`
	Identifier    string                `json:"dct:identifier"`
	Title         string                `json:"dct:title"`
	Description   string                `json:"dct:description,omitempty"`
	Modified      time.Time             `json:"dct:modified"`
	ConformsTo    string                `json:"dct:conformsTo"`
	Distributions []CatalogDistribution `json:"dcat:distribution"`
}

type CatalogDistribution struct {
	Type        string `json:"@type"`
	DownloadURL string `json:"dcat:downloadURL"`
	MediaType   string `json:"dcat:mediaType"`
}

var catalogContext = map[string]string{
	"dcat": "http://www.w3.org/ns/dcat#",
	"dct":  "http://purl.org/dc/terms/",
}

// GetCatalog lists the models of a project that the caller can read.
// baseURL is the public API URL of the project, e.g. https://example.com/api/p/ws/project
func (c *Controller) GetCatalog(ctx context.Context, wsAlias, pAlias, baseURL string) (Catalog, error) {
	wpm, err := c.loadWPMContext(ctx, wsAlias, pAlias, "")
	if err != nil {
		return Catalog{}, err
	}

	models, _, err := c.usecases.Model.FindByProject(ctx, wpm.Project.ID(), usecasex.OffsetPagination{Limit: 1000}.Wrap(), nil)
	if err != nil {
		return Catalog{}, err
	}

	a11y := wpm.Project.Accessibility()
	keyId := adapter.APIKeyId(ctx)
	public := lo.Filter(models, func(m *model.Model, _ int) bool {
		return a11y.IsModelPublic(m.ID(), keyId)
	})

	return Catalog{
		Context:     catalogContext,
		Type:        "dcat:Catalog",
		Title:       wpm.Project.Name(),
		Description: wpm.Project.Description(),
		License:     wpm.Project.License(),
		Modified:    wpm.Project.UpdatedAt(),
		Datasets: lo.Map(public, func(m *model.Model, _ int) CatalogDataset {
			return catalogDataset(m, baseURL)
		}),
	}, nil
}

func catalogDataset(m *model.Model, baseURL string) CatalogDataset {
	u := baseURL + "/" + m.Key().String()
	return CatalogDataset{
		Type:        "dcat:Dataset",
		Identifier:  m.ID().String(),
		Title:       m.Name(),
		Description: m.Description(),
		Modified:    m.UpdatedAt(),
		ConformsTo:  u + ".schema.json",
		Distributions: []CatalogDistribution{
			{Type: "dcat:Distribution", DownloadURL: u + ".parquet", MediaType: "application/vnd.apache.parquet"},
			{Type: "dcat:Distribution", DownloadURL: u + ".csv", MediaType: "text/csv"},
		},
	}
}
