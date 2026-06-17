package index

import "github.com/osvaldoandrade/cefas/internal/core/model"

// Descriptor names an index and points at the plugin that owns it.
// PluginName is "" for built-in (GSI / LSI / spatial) indexes.
type Descriptor struct {
	Table        string
	Name         string
	PluginName   string
	PluginConfig []byte // opaque to the engine; the plugin parses it
	KeySchema    model.KeySchema
	Projection   []string
}

// Lifecycle is the verb surface against which CreateIndex /
// DescribeIndex / RebuildIndex / DropIndex flow at the engine
// boundary. Implementations route plugin-backed indexes through the
// plugin registry; built-in indexes use the storage maintenance
// path.
type Lifecycle interface {
	Create(Descriptor) error
	Describe(table, name string) (Descriptor, error)
	Rebuild(table, name string) error
	Drop(table, name string) error
}
