// Package domain holds the pure validators, normalizers, and
// stream-metadata helpers used by the catalog adapter. None of
// these functions touch storage — they operate on
// types.TableDescriptor and types.StreamDescriptor values and are
// therefore safe to call from any layer.
//
// The split exists so the catalog adapter
// (internal/catalog/catalog.go) keeps a thin Pebble-backed shape
// while invariants (storage class, view types, ARN format, deep
// cloning) live next to the data they govern.
package domain

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/pkg/types"
)

var (
	streamLabelMu           sync.Mutex
	lastStreamLabelUnixNano int64
)

// NormalizeDescriptor canonicalizes a freshly-decoded
// types.TableDescriptor: lowercased storage class with default,
// uppercased attribute types with vector-dimension validation, and
// the stream-spec normalization NormalizeStreamDescriptor performs.
// It mutates td in place and returns any validation error.
func NormalizeDescriptor(td *types.TableDescriptor) error {
	switch strings.ToLower(strings.TrimSpace(td.StorageClass)) {
	case "", types.StorageClassDisk:
		td.StorageClass = types.StorageClassDisk
	case types.StorageClassMemory:
		td.StorageClass = types.StorageClassMemory
	default:
		return fmt.Errorf("storageClass %q must be %q or %q", td.StorageClass, types.StorageClassDisk, types.StorageClassMemory)
	}
	for i := range td.AttributeDefinitions {
		td.AttributeDefinitions[i].Type = strings.ToUpper(strings.TrimSpace(td.AttributeDefinitions[i].Type))
		if td.AttributeDefinitions[i].Type == "V" && td.AttributeDefinitions[i].VectorDimensions <= 0 {
			return fmt.Errorf("attributeDefinitions[%d]: V requires vectorDimensions > 0", i)
		}
	}
	if err := NormalizeStreamDescriptor(td); err != nil {
		return err
	}
	return nil
}

// NormalizeStreamDescriptor canonicalizes the table descriptor's
// stream spec. When streams are disabled it clears stream metadata;
// when enabled it fills view type, ARN, label, and status defaults.
func NormalizeStreamDescriptor(td *types.TableDescriptor) error {
	if td.StreamSpecification == nil || !td.StreamSpecification.StreamEnabled {
		td.StreamSpecification = nil
		td.LatestStreamArn = ""
		td.LatestStreamLabel = ""
		td.StreamStatus = ""
		return nil
	}
	view := types.NormalizeStreamViewType(td.StreamSpecification.StreamViewType)
	if view == "" {
		view = types.StreamViewTypeNewAndOldImages
	}
	if !types.IsValidStreamViewType(view) {
		return fmt.Errorf("streamViewType %q must be one of %q, %q, %q, %q",
			td.StreamSpecification.StreamViewType,
			types.StreamViewTypeKeysOnly,
			types.StreamViewTypeNewImage,
			types.StreamViewTypeOldImage,
			types.StreamViewTypeNewAndOldImages)
	}
	td.StreamSpecification = &types.StreamSpecification{
		StreamEnabled:  true,
		StreamViewType: view,
	}
	if td.LatestStreamLabel == "" {
		td.LatestStreamLabel = NextStreamLabel()
	}
	if td.LatestStreamArn == "" {
		td.LatestStreamArn = StreamARN(td.Name, td.LatestStreamLabel)
	}
	if td.StreamStatus == "" {
		td.StreamStatus = types.StreamStatusEnabled
	}
	return nil
}

// StreamEnabled reports whether td has streams switched on.
func StreamEnabled(td types.TableDescriptor) bool {
	return td.StreamSpecification != nil && td.StreamSpecification.StreamEnabled
}

// StreamViewType returns the normalized stream view type for td,
// defaulting to NEW_AND_OLD_IMAGES when the spec omits one.
func StreamViewType(td types.TableDescriptor) string {
	if td.StreamSpecification == nil {
		return ""
	}
	view := types.NormalizeStreamViewType(td.StreamSpecification.StreamViewType)
	if view == "" {
		return types.StreamViewTypeNewAndOldImages
	}
	return view
}

// ApplyStreamUpdateSemantics enforces the in-place-update contract
// for streams: view type cannot change while a stream is enabled,
// and the existing ARN / label / status carry over.
func ApplyStreamUpdateSemantics(existing types.TableDescriptor, td *types.TableDescriptor) error {
	if !StreamEnabled(existing) || !StreamEnabled(*td) {
		return nil
	}
	oldView := StreamViewType(existing)
	newView := StreamViewType(*td)
	if oldView != newView {
		return fmt.Errorf("streamViewType cannot be changed while stream is enabled; disable and re-enable the stream")
	}
	td.LatestStreamLabel = existing.LatestStreamLabel
	td.LatestStreamArn = existing.LatestStreamArn
	td.StreamStatus = types.StreamStatusEnabled
	return nil
}

// NormalizeStreamMetadata fills shard defaults on a freshly
// decoded stream descriptor — used after reading a descriptor
// blob from storage.
func NormalizeStreamMetadata(desc *types.StreamDescriptor) {
	if desc.StreamStatus == "" {
		desc.StreamStatus = types.StreamStatusEnabled
	}
	if len(desc.Shards) == 0 {
		desc.Shards = []types.StreamShardDescriptor{
			{
				ShardID: model.StreamShardIDSingle.String(),
				SequenceNumberRange: types.StreamSequenceNumberRange{
					StartingSequenceNumber: "1",
				},
			},
		}
		return
	}
	for i := range desc.Shards {
		if desc.Shards[i].ShardID == "" {
			desc.Shards[i].ShardID = model.StreamShardIDSingle.String()
		}
		if desc.Shards[i].SequenceNumberRange.StartingSequenceNumber == "" {
			desc.Shards[i].SequenceNumberRange.StartingSequenceNumber = "1"
		}
	}
}

// NextStreamLabel returns a monotonically increasing UTC label
// suitable for embedding in a stream ARN.
func NextStreamLabel() string {
	streamLabelMu.Lock()
	defer streamLabelMu.Unlock()
	now := time.Now().UTC().UnixNano()
	if now <= lastStreamLabelUnixNano {
		now = lastStreamLabelUnixNano + 1
	}
	lastStreamLabelUnixNano = now
	return time.Unix(0, now).UTC().Format("2006-01-02T15:04:05.000000000Z")
}

// StreamARN composes the canonical stream ARN cefas hands out for
// a (table, label) pair.
func StreamARN(table, label string) string {
	return fmt.Sprintf("arn:cefas:dynamodb:local:000000000000:table/%s/stream/%s", table, label)
}

// CloneTableDescriptor returns a deep copy of td so callers can
// mutate the result without leaking changes back into the
// catalog's in-memory cache.
func CloneTableDescriptor(td types.TableDescriptor) types.TableDescriptor {
	if td.AttributeDefinitions != nil {
		td.AttributeDefinitions = append([]types.AttributeDefinition(nil), td.AttributeDefinitions...)
	}
	if td.GSIs != nil {
		gsis := make([]types.GSIDescriptor, len(td.GSIs))
		for i, gsi := range td.GSIs {
			gsi.Projection = CloneIndexProjection(gsi.Projection)
			if gsi.Projected != nil {
				gsi.Projected = append([]string(nil), gsi.Projected...)
			}
			gsis[i] = gsi
		}
		td.GSIs = gsis
	}
	if td.LSIs != nil {
		lsis := make([]types.LSIDescriptor, len(td.LSIs))
		for i, lsi := range td.LSIs {
			lsi.Projection = CloneIndexProjection(lsi.Projection)
			lsis[i] = lsi
		}
		td.LSIs = lsis
	}
	if td.SpatialIndexes != nil {
		spatial := make([]types.SpatialIndexDescriptor, len(td.SpatialIndexes))
		for i, idx := range td.SpatialIndexes {
			if idx.Attributes != nil {
				idx.Attributes = append([]string(nil), idx.Attributes...)
			}
			if idx.Ranges != nil {
				idx.Ranges = append([]types.NumRange(nil), idx.Ranges...)
			}
			spatial[i] = idx
		}
		td.SpatialIndexes = spatial
	}
	if td.StreamSpecification != nil {
		spec := *td.StreamSpecification
		td.StreamSpecification = &spec
	}
	return td
}

// CloneIndexProjection deep-copies an IndexProjection so callers
// can mutate the result without aliasing the original Include
// slice.
func CloneIndexProjection(in types.IndexProjection) types.IndexProjection {
	if in.Include != nil {
		in.Include = append([]string(nil), in.Include...)
	}
	return in
}

// NormalizeMVDescriptor validates a materialized view descriptor in
// place and applies defaults. Default Mode is RefreshModeEager.
func NormalizeMVDescriptor(mv *types.MaterializedViewDescriptor) error {
	if mv == nil {
		return fmt.Errorf("nil materialized view descriptor")
	}
	mv.Name = strings.TrimSpace(mv.Name)
	mv.BaseTable = strings.TrimSpace(mv.BaseTable)
	if mv.Name == "" {
		return fmt.Errorf("materialized view name required")
	}
	if mv.BaseTable == "" {
		return fmt.Errorf("materialized view %q: base_table required", mv.Name)
	}
	if mv.KeySchema.PK == "" {
		return fmt.Errorf("materialized view %q: KeySchema.PK required", mv.Name)
	}
	switch mv.RefreshPolicy.Mode {
	case types.RefreshModeUnspecified:
		mv.RefreshPolicy.Mode = types.RefreshModeEager
		mv.RefreshPolicy.IntervalSeconds = 0
	case types.RefreshModeEager:
		mv.RefreshPolicy.IntervalSeconds = 0
	case types.RefreshModeScheduled:
		if mv.RefreshPolicy.IntervalSeconds <= 0 {
			return fmt.Errorf("materialized view %q: REFRESH EVERY requires interval > 0", mv.Name)
		}
	case types.RefreshModeFast:
		if mv.RefreshPolicy.IntervalSeconds <= 0 {
			return fmt.Errorf("materialized view %q: REFRESH FAST requires interval > 0", mv.Name)
		}
	case types.RefreshModeOnDemand:
		mv.RefreshPolicy.IntervalSeconds = 0
	default:
		return fmt.Errorf("materialized view %q: unsupported refresh mode %q", mv.Name, mv.RefreshPolicy.Mode)
	}
	if mv.Status == "" {
		mv.Status = types.MVStatusBuilding
	}
	if mv.ProjectedAttributes != nil {
		seen := map[string]struct{}{}
		out := make([]string, 0, len(mv.ProjectedAttributes))
		for _, a := range mv.ProjectedAttributes {
			a = strings.TrimSpace(a)
			if a == "" {
				continue
			}
			if _, dup := seen[a]; dup {
				continue
			}
			seen[a] = struct{}{}
			out = append(out, a)
		}
		mv.ProjectedAttributes = out
	}
	return nil
}

// CloneMVDescriptor deep-copies a materialized view descriptor.
func CloneMVDescriptor(in types.MaterializedViewDescriptor) types.MaterializedViewDescriptor {
	out := in
	if in.ProjectedAttributes != nil {
		out.ProjectedAttributes = append([]string(nil), in.ProjectedAttributes...)
	}
	return out
}
