package client

import (
	"context"
	"fmt"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/pkg/types"
)

// CreateTableWithRequiredLSIs persists a table and returns the stored descriptor.
//
// It returns codes.FailedPrecondition when the server omits, reorders, or changes
// any requested LSI. Callers may write rows only after this method succeeds.
func (c *Client) CreateTableWithRequiredLSIs(
	ctx context.Context,
	td types.TableDescriptor,
) (types.TableDescriptor, error) {
	created, err := c.CreateTableWithDescriptor(ctx, td)
	if err != nil {
		return types.TableDescriptor{}, err
	}
	if err := validateReturnedLSIs(td.LSIs, created.LSIs); err != nil {
		return types.TableDescriptor{}, status.Errorf(
			codes.FailedPrecondition,
			"cefas: create table %q returned incompatible descriptor: %v",
			td.Name,
			err,
		)
	}
	stored, err := c.DescribeTable(ctx, td.Name)
	if err != nil {
		return types.TableDescriptor{}, fmt.Errorf(
			"cefas: describe table %q after create: %w",
			td.Name,
			err,
		)
	}
	if err := validateReturnedLSIs(td.LSIs, stored.LSIs); err != nil {
		return types.TableDescriptor{}, status.Errorf(
			codes.FailedPrecondition,
			"cefas: describe table %q returned incompatible descriptor: %v",
			td.Name,
			err,
		)
	}
	return stored, nil
}

func validateReturnedLSIs(requested, returned []types.LSIDescriptor) error {
	if len(returned) != len(requested) {
		return fmt.Errorf("LSI count = %d, want %d", len(returned), len(requested))
	}
	for i := range requested {
		want, got := requested[i], returned[i]
		switch {
		case got.Name != want.Name:
			return fmt.Errorf("LSI %d name = %q, want %q", i, got.Name, want.Name)
		case got.SK != want.SK:
			return fmt.Errorf("LSI %d sort key = %q, want %q", i, got.SK, want.SK)
		case got.Projection.Mode != want.Projection.Mode:
			return fmt.Errorf(
				"LSI %d projection mode = %q, want %q",
				i,
				got.Projection.Mode,
				want.Projection.Mode,
			)
		case !slices.Equal(got.Projection.Include, want.Projection.Include):
			return fmt.Errorf(
				"LSI %d projection include = %v, want %v",
				i,
				got.Projection.Include,
				want.Projection.Include,
			)
		}
	}
	return nil
}
