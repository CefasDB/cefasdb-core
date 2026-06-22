package server

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/pkg/plugin"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// QueryIndex runs a plugin-backed secondary index query against this
// node's local slice and streams the resulting candidates back to
// the caller. Coordinators use it to fan out across every node so
// the merged result represents the full table.
//
// The handler refuses tables that do not exist locally and indices
// whose plugin is not registered on this node — both surface as a
// descriptive error so the coordinator's fan-in helper can pick the
// next peer or fail cleanly.
func (s *GRPCServer) QueryIndex(req *cefaspb.QueryIndexRequest, stream cefaspb.Replica_QueryIndexServer) error {
	if req.GetTable() == "" || req.GetIndexName() == "" {
		return status.Error(codes.InvalidArgument, "table and index_name are required")
	}

	desc, ok, err := s.lookupPluginIndexDescriptor(req.GetTable(), req.GetIndexName())
	if err != nil {
		return mapStorageErr(err)
	}
	if !ok {
		return status.Errorf(codes.NotFound, "plugin index %s/%s not registered",
			req.GetTable(), req.GetIndexName())
	}

	raw, ok := s.pluginRegistry().Lookup(desc.PluginName)
	if !ok {
		return status.Errorf(codes.FailedPrecondition,
			"plugin %q not registered on this node", desc.PluginName)
	}
	ip, ok := raw.(plugin.IndexPlugin)
	if !ok {
		return status.Errorf(codes.InvalidArgument,
			"plugin %q is not an IndexPlugin", desc.PluginName)
	}

	if err := s.ensurePluginIndexLocalState(desc); err != nil {
		return err
	}

	binds := make(map[string]types.AttributeValue, len(req.GetBinds()))
	for k, v := range req.GetBinds() {
		av, err := pbToAttr(v)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "bind %q: %v", k, err)
		}
		binds[k] = av
	}

	cs, err := ip.Query(desc, plugin.IndexQuery{
		Binds: binds,
		Limit: int(req.GetLimit()),
	})
	if err != nil {
		return status.Errorf(codes.Internal, "plugin query: %v", err)
	}
	defer cs.Close()

	ctx := stream.Context()
	for {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		c, ok := cs.Next()
		if !ok {
			break
		}
		out := &cefaspb.IndexCandidate{
			Key:   itemToPB(c.Key),
			Score: c.Score,
		}
		if err := stream.Send(out); err != nil {
			return err
		}
	}
	return cs.Err()
}
