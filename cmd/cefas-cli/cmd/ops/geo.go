package ops

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
	"github.com/osvaldoandrade/cefas/pkg/client"
	"github.com/osvaldoandrade/cefas/pkg/ddbjson"
)

func registerGeo(root *cobra.Command) {
	grp := &cobra.Command{
		Use:   "geo",
		Short: "Geospatial audience selection",
	}
	grp.AddCommand(geoAudienceCmd())
	root.AddCommand(grp)
}

func geoAudienceCmd() *cobra.Command {
	var (
		table, index, center, radius string
		limit                        int
	)
	c := &cobra.Command{
		Use:   "audience",
		Short: "Select audience members inside a geo radius",
		Long: `Mirrors aws dynamodb geo-audience. Composes the geohash plugin
candidate generation with the haversine post-filter.

Example:
  cefas geo audience \
    --table Stores \
    --center "-23.9608,-46.3336" \
    --radius 1500m`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" || center == "" || radius == "" {
				return fmt.Errorf("--table, --center, --radius are required")
			}
			lat, lon, err := parseCenter(center)
			if err != nil {
				return err
			}
			rmeters, err := parseRadius(radius)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			items, err := cli.GeoAudience(ctx, client.GeoAudienceRequest{
				Table: table, Index: index,
				Lat: lat, Lon: lon, RadiusMeters: rmeters,
				Limit: limit,
			})
			if err != nil {
				return fmt.Errorf("geo audience: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			wire := make([]map[string]ddbjson.Attribute, 0, len(items))
			for _, it := range items {
				wire = append(wire, ddbjson.EncodeItem(it))
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"Items": wire,
				"Count": len(wire),
			})
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table", "", "Target table (required)")
	f.StringVar(&index, "index", "loc_geo", "Geohash index name (must exist; create with cefas create-index --type geohash)")
	f.StringVar(&center, "center", "", "Center as 'lat,lon' (required)")
	f.StringVar(&radius, "radius", "", "Radius with unit suffix m|km (required, e.g. 1500m or 5km)")
	f.IntVar(&limit, "limit", 0, "Cap on items returned (0 = no cap)")
	return c
}

func parseCenter(s string) (lat, lon float64, err error) {
	parts := strings.SplitN(s, ",", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("--center must be 'lat,lon' (got %q)", s)
	}
	lat, err = strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	if err != nil {
		return 0, 0, fmt.Errorf("--center lat: %w", err)
	}
	lon, err = strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return 0, 0, fmt.Errorf("--center lon: %w", err)
	}
	return lat, lon, nil
}

func parseRadius(s string) (float64, error) {
	s = strings.TrimSpace(s)
	switch {
	case strings.HasSuffix(s, "km"):
		v, err := strconv.ParseFloat(strings.TrimSuffix(s, "km"), 64)
		if err != nil {
			return 0, fmt.Errorf("--radius: %w", err)
		}
		return v * 1000, nil
	case strings.HasSuffix(s, "m"):
		v, err := strconv.ParseFloat(strings.TrimSuffix(s, "m"), 64)
		if err != nil {
			return 0, fmt.Errorf("--radius: %w", err)
		}
		return v, nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("--radius: %w", err)
	}
	return v, nil
}
