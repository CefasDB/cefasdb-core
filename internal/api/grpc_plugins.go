package api

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/auth"
	"github.com/osvaldoandrade/cefas/internal/tracing"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
)

// ListPlugins enumerates every plugin registered with the server's
// plugin registry. The optional kind filter narrows to a single
// plugin kind ("index" / "distance" / "estimator" / "audience").
//
// Public-ish: requires ScopeTableDescribe — same posture as
// DescribeTable.
func (s *GRPCServer) ListPlugins(ctx context.Context, req *cefaspb.ListPluginsRequest) (*cefaspb.ListPluginsResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "ListPlugins")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	reg := s.pluginRegistry()
	var snap []plugin.Status
	if req.GetKind() != "" {
		// Per-kind path mirrors the registry's typed lookup.
		kind, err := parsePluginKind(req.GetKind())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		// Run Snapshot over a scratch registry so the kind filter applies.
		scratch := plugin.NewRegistry()
		for _, p := range reg.LookupByKind(kind) {
			_ = scratch.Register(p)
		}
		snap = plugin.Snapshot(scratch, defaultStateLookup(reg), nil)
	} else {
		snap = plugin.Snapshot(reg, defaultStateLookup(reg), nil)
	}
	out := make([]*cefaspb.PluginDescriptor, 0, len(snap))
	for _, s := range snap {
		out = append(out, pluginStatusToPB(s))
	}
	return &cefaspb.ListPluginsResponse{Plugins: out}, nil
}

// DescribePlugin returns the descriptor for a single plugin.
func (s *GRPCServer) DescribePlugin(ctx context.Context, req *cefaspb.DescribePluginRequest) (*cefaspb.DescribePluginResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "DescribePlugin")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	reg := s.pluginRegistry()
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	if _, ok := reg.Lookup(name); !ok && !reg.IsDisabled(name) {
		return nil, status.Errorf(codes.NotFound, "plugin %q not registered", name)
	}
	// Snapshot returns one entry per registered plugin; pick ours.
	snap := plugin.Snapshot(reg, defaultStateLookup(reg), nil)
	for _, st := range snap {
		if st.Name == name {
			return &cefaspb.DescribePluginResponse{Plugin: pluginStatusToPB(st)}, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "plugin %q not registered", name)
}

// defaultStateLookup synthesises a State per plugin name from the
// registry's Disable bit. Engines that run a plugin.Manager and want
// to surface running/failed states inject a richer lookup via
// AttachPluginRegistry + a custom Snapshot call.
func defaultStateLookup(reg *plugin.Registry) func(string) plugin.State {
	return func(name string) plugin.State {
		if reg.IsDisabled(name) {
			return plugin.StateDisabled
		}
		return plugin.StateLoaded
	}
}

func parsePluginKind(s string) (plugin.Kind, error) {
	switch s {
	case "index":
		return plugin.KindIndex, nil
	case "distance":
		return plugin.KindDistance, nil
	case "estimator":
		return plugin.KindEstimator, nil
	case "audience":
		return plugin.KindAudience, nil
	}
	return 0, status.Errorf(codes.InvalidArgument, "unknown plugin kind %q", s)
}

func pluginStatusToPB(s plugin.Status) *cefaspb.PluginDescriptor {
	return &cefaspb.PluginDescriptor{
		Name:          s.Name,
		Kind:          s.Kind,
		Version:       s.Version,
		Description:   s.Description,
		State:         s.State,
		LastError:     s.LastError,
		LastErrorUnix: s.LastErrorAtUnix,
		ItemsIndexed:  s.ItemsIndexed,
		StartedAtUnix: s.StartedAtUnix,
	}
}
