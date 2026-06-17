package pebble

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/CefasDb/cefasdb/internal/storage"

	"github.com/CefasDb/cefasdb/internal/core/index"
)

const PluginIndexDescriptorVersion = 1

// PluginIndexDescriptorRecord is the durable envelope for plugin index
// descriptors. Version gives future migrations an explicit discriminator.
type PluginIndexDescriptorRecord struct {
	Version    int              `json:"version"`
	Descriptor index.Descriptor `json:"descriptor"`
}

func EncodePluginIndexDescriptor(desc index.Descriptor) ([]byte, error) {
	if desc.Table == "" || desc.Name == "" || desc.PluginName == "" {
		return nil, fmt.Errorf("plugin index descriptor requires table, name, and pluginName")
	}
	rec := PluginIndexDescriptorRecord{
		Version:    PluginIndexDescriptorVersion,
		Descriptor: desc,
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("marshal plugin index descriptor: %w", err)
	}
	return raw, nil
}

func DecodePluginIndexDescriptor(raw []byte) (index.Descriptor, error) {
	var rec PluginIndexDescriptorRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return index.Descriptor{}, fmt.Errorf("decode plugin index descriptor: %w", err)
	}
	if rec.Version != PluginIndexDescriptorVersion {
		return index.Descriptor{}, fmt.Errorf("unsupported plugin index descriptor version %d", rec.Version)
	}
	if rec.Descriptor.Table == "" || rec.Descriptor.Name == "" || rec.Descriptor.PluginName == "" {
		return index.Descriptor{}, fmt.Errorf("plugin index descriptor requires table, name, and pluginName")
	}
	return rec.Descriptor, nil
}

func (d *DB) PutPluginIndexDescriptor(desc index.Descriptor) error {
	raw, err := EncodePluginIndexDescriptor(desc)
	if err != nil {
		return err
	}
	if err := d.Set(storage.KeyPluginIndexDescriptor(desc.Table, desc.Name), raw); err != nil {
		return fmt.Errorf("persist plugin index descriptor: %w", err)
	}
	return nil
}

func (d *DB) GetPluginIndexDescriptor(table, name string) (index.Descriptor, bool, error) {
	raw, err := d.Get(storage.KeyPluginIndexDescriptor(table, name))
	if err == ErrNotFound {
		return index.Descriptor{}, false, nil
	}
	if err != nil {
		return index.Descriptor{}, false, err
	}
	desc, err := DecodePluginIndexDescriptor(raw)
	if err != nil {
		return index.Descriptor{}, false, err
	}
	return desc, true, nil
}

func (d *DB) ListPluginIndexDescriptors() ([]index.Descriptor, error) {
	lower, upper := storage.PrefixPluginIndexDescriptors()
	it, err := d.Iter(lower, upper)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var out []index.Descriptor
	for valid := it.First(); valid; valid = it.Next() {
		raw := it.Value()
		cp := make([]byte, len(raw))
		copy(cp, raw)
		desc, err := DecodePluginIndexDescriptor(cp)
		if err != nil {
			return nil, fmt.Errorf("decode plugin index descriptor at %s: %w", it.Key(), err)
		}
		out = append(out, desc)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Table != out[j].Table {
			return out[i].Table < out[j].Table
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (d *DB) DeletePluginIndexDescriptor(table, name string) error {
	if err := d.Delete(storage.KeyPluginIndexDescriptor(table, name)); err != nil {
		return fmt.Errorf("delete plugin index descriptor: %w", err)
	}
	return nil
}

func (d *DB) DeletePluginIndexDescriptorsForTable(table string) error {
	lower, upper := storage.PrefixPluginIndexTableDescriptors(table)
	it, err := d.Iter(lower, upper)
	if err != nil {
		return err
	}
	defer it.Close()

	b := d.Batch()
	defer b.Close()
	deleted := 0
	for valid := it.First(); valid; valid = it.Next() {
		key := it.Key()
		cp := make([]byte, len(key))
		copy(cp, key)
		if err := b.Delete(cp, nil); err != nil {
			return err
		}
		deleted++
	}
	if err := it.Error(); err != nil {
		return err
	}
	if deleted == 0 {
		return nil
	}
	if err := d.CommitBatch(b); err != nil {
		return fmt.Errorf("delete plugin index descriptors for table: %w", err)
	}
	return nil
}
