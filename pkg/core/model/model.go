// Package model is a deprecated public-API shim that re-exports
// every symbol from internal/core/model.
//
// Deprecated: import "github.com/osvaldoandrade/cefas/internal/core/model"
// directly. This shim exists so external code that imported the
// public path before the PR 5 carve-out keeps building during a
// migration window. It will be removed in a future minor release.
package model

import (
	internalmodel "github.com/osvaldoandrade/cefas/internal/core/model"
)

// Value-object IDs.

// ShardID aliases internal/core/model.ShardID.
//
// Deprecated: use internal/core/model.ShardID.
type ShardID = internalmodel.ShardID

// UnroutedShardID aliases internal/core/model.UnroutedShardID.
//
// Deprecated: use internal/core/model.UnroutedShardID.
var UnroutedShardID = internalmodel.UnroutedShardID

// NewShardID aliases internal/core/model.NewShardID.
//
// Deprecated: use internal/core/model.NewShardID.
func NewShardID(v uint32) (ShardID, error) { return internalmodel.NewShardID(v) }

// MustShardID aliases internal/core/model.MustShardID.
//
// Deprecated: use internal/core/model.MustShardID.
func MustShardID(v uint32) ShardID { return internalmodel.MustShardID(v) }

// NodeID aliases internal/core/model.NodeID.
//
// Deprecated: use internal/core/model.NodeID.
type NodeID = internalmodel.NodeID

// NewNodeID aliases internal/core/model.NewNodeID.
//
// Deprecated: use internal/core/model.NewNodeID.
func NewNodeID(v string) (NodeID, error) { return internalmodel.NewNodeID(v) }

// MustNodeID aliases internal/core/model.MustNodeID.
//
// Deprecated: use internal/core/model.MustNodeID.
func MustNodeID(v string) NodeID { return internalmodel.MustNodeID(v) }

// StreamShardID aliases internal/core/model.StreamShardID.
//
// Deprecated: use internal/core/model.StreamShardID.
type StreamShardID = internalmodel.StreamShardID

// StreamShardIDSingle aliases internal/core/model.StreamShardIDSingle.
//
// Deprecated: use internal/core/model.StreamShardIDSingle.
var StreamShardIDSingle = internalmodel.StreamShardIDSingle

// NewStreamShardID aliases internal/core/model.NewStreamShardID.
//
// Deprecated: use internal/core/model.NewStreamShardID.
func NewStreamShardID(v string) (StreamShardID, error) {
	return internalmodel.NewStreamShardID(v)
}

// MustStreamShardID aliases internal/core/model.MustStreamShardID.
//
// Deprecated: use internal/core/model.MustStreamShardID.
func MustStreamShardID(v string) StreamShardID {
	return internalmodel.MustStreamShardID(v)
}

// TableID aliases internal/core/model.TableID.
//
// Deprecated: use internal/core/model.TableID.
type TableID = internalmodel.TableID

// NewTableID aliases internal/core/model.NewTableID.
//
// Deprecated: use internal/core/model.NewTableID.
func NewTableID(v string) (TableID, error) { return internalmodel.NewTableID(v) }

// MustTableID aliases internal/core/model.MustTableID.
//
// Deprecated: use internal/core/model.MustTableID.
func MustTableID(v string) TableID { return internalmodel.MustTableID(v) }

// Re-exports from pkg/types kept for source compatibility.

// Item aliases internal/core/model.Item (itself an alias to pkg/types.Item).
//
// Deprecated: use pkg/types.Item.
type Item = internalmodel.Item

// AttributeValue aliases internal/core/model.AttributeValue.
//
// Deprecated: use pkg/types.AttributeValue.
type AttributeValue = internalmodel.AttributeValue

// AttrType aliases internal/core/model.AttrType.
//
// Deprecated: use pkg/types.AttrType.
type AttrType = internalmodel.AttrType

// KeySchema aliases internal/core/model.KeySchema.
//
// Deprecated: use pkg/types.KeySchema.
type KeySchema = internalmodel.KeySchema

// TableDescriptor aliases internal/core/model.TableDescriptor.
//
// Deprecated: use pkg/types.TableDescriptor.
type TableDescriptor = internalmodel.TableDescriptor

// Attribute-kind constants re-exported from internal/core/model
// (which itself re-exports them from pkg/types) so existing plugin
// code that switches on `av.T` against `model.AttrS` keeps working.
//
// Deprecated: use pkg/types.AttrS etc. directly.
const (
	AttrS    = internalmodel.AttrS
	AttrN    = internalmodel.AttrN
	AttrB    = internalmodel.AttrB
	AttrBOOL = internalmodel.AttrBOOL
	AttrNull = internalmodel.AttrNull
	AttrSS   = internalmodel.AttrSS
	AttrNS   = internalmodel.AttrNS
	AttrBS   = internalmodel.AttrBS
	AttrL    = internalmodel.AttrL
	AttrM    = internalmodel.AttrM
	AttrVec  = internalmodel.AttrVec
)
