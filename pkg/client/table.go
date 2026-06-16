package client

import (
	"context"

	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// ---------- schema ----------

// CreateTable persists a new table descriptor on the leader.
func (c *Client) CreateTable(ctx context.Context, td types.TableDescriptor) error {
	_, err := c.CreateTableWithDescriptor(ctx, td)
	return err
}

// CreateTableWithDescriptor persists a table and returns the normalized
// descriptor the server stored.
func (c *Client) CreateTableWithDescriptor(ctx context.Context, td types.TableDescriptor) (types.TableDescriptor, error) {
	resp, err := c.stub.CreateTable(c.withAuth(ctx), &cefaspb.CreateTableRequest{Descriptor_: tdToPB(td)})
	if err != nil {
		return types.TableDescriptor{}, err
	}
	return tdFromPB(resp.GetDescriptor_()), nil
}

// DescribeTable returns the descriptor for `name`.
func (c *Client) DescribeTable(ctx context.Context, name string) (types.TableDescriptor, error) {
	resp, err := c.stub.DescribeTable(c.withAuth(ctx), &cefaspb.DescribeTableRequest{Name: name})
	if err != nil {
		return types.TableDescriptor{}, err
	}
	return tdFromPB(resp.GetDescriptor_()), nil
}

// ListTables returns every table the server knows about.
func (c *Client) ListTables(ctx context.Context) ([]types.TableDescriptor, error) {
	resp, err := c.stub.ListTables(c.withAuth(ctx), &cefaspb.ListTablesRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]types.TableDescriptor, 0, len(resp.GetTables()))
	for _, t := range resp.GetTables() {
		out = append(out, tdFromPB(t))
	}
	return out, nil
}

// TTLState mirrors the wire response of DescribeTimeToLive in a
// caller-friendly shape. Enabled is true iff Status == "ENABLED".
type TTLState struct {
	Enabled       bool
	AttributeName string
}

// UpdateTimeToLive enables or disables TTL on `table`. When enabling,
// `attribute` names the numeric epoch-seconds column the reaper uses.
// When disabling, `attribute` is ignored.
func (c *Client) UpdateTimeToLive(ctx context.Context, table, attribute string, enabled bool) (TTLState, error) {
	resp, err := c.stub.UpdateTimeToLive(c.withAuth(ctx), &cefaspb.UpdateTimeToLiveRequest{
		TableName: table,
		TimeToLiveSpecification: &cefaspb.TimeToLiveSpecification{
			Enabled:       enabled,
			AttributeName: attribute,
		},
	})
	if err != nil {
		return TTLState{}, err
	}
	spec := resp.GetTimeToLiveSpecification()
	return TTLState{Enabled: spec.GetEnabled(), AttributeName: spec.GetAttributeName()}, nil
}

// DescribeTimeToLive returns the current TTL configuration for `table`.
func (c *Client) DescribeTimeToLive(ctx context.Context, table string) (TTLState, error) {
	resp, err := c.stub.DescribeTimeToLive(c.withAuth(ctx), &cefaspb.DescribeTimeToLiveRequest{TableName: table})
	if err != nil {
		return TTLState{}, err
	}
	return TTLState{
		Enabled:       resp.GetStatus() == "ENABLED",
		AttributeName: resp.GetAttributeName(),
	}, nil
}

// DropTable removes a table descriptor.
func (c *Client) DropTable(ctx context.Context, name string) error {
	_, err := c.stub.DropTable(c.withAuth(ctx), &cefaspb.DropTableRequest{Name: name})
	return err
}
