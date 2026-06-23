package catalog_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/CefasDb/cefasdb/internal/catalog"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func openCat(t *testing.T) *catalog.Catalog {
	t.Helper()
	c, _ := openCatWithDB(t)
	return c
}

func openCatWithDB(t *testing.T) (*catalog.Catalog, *pebble.DB) {
	t.Helper()
	dir := t.TempDir()
	db, err := pebble.Open(pebble.Options{Path: dir})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	c, err := catalog.New(db)
	if err != nil {
		t.Fatalf("new catalog: %v", err)
	}
	return c, db
}

func TestUpdateTableSetsTTL(t *testing.T) {
	c := openCat(t)
	td := types.TableDescriptor{
		Name:      "Sessions",
		KeySchema: types.KeySchema{PK: "pk"},
	}
	if err := c.Create(td); err != nil {
		t.Fatalf("create: %v", err)
	}

	td.TTLAttribute = "expires_at"
	if err := c.UpdateTable(td); err != nil {
		t.Fatalf("update: %v", err)
	}

	got, err := c.Describe("Sessions")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if got.TTLAttribute != "expires_at" {
		t.Fatalf("TTLAttribute = %q, want %q", got.TTLAttribute, "expires_at")
	}
}

func TestUpdateTableClearsTTL(t *testing.T) {
	c := openCat(t)
	td := types.TableDescriptor{
		Name:         "Sessions",
		KeySchema:    types.KeySchema{PK: "pk"},
		TTLAttribute: "expires_at",
	}
	if err := c.Create(td); err != nil {
		t.Fatalf("create: %v", err)
	}
	td.TTLAttribute = ""
	if err := c.UpdateTable(td); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, _ := c.Describe("Sessions")
	if got.TTLAttribute != "" {
		t.Fatalf("TTLAttribute = %q, want empty", got.TTLAttribute)
	}
}

func TestUpdateTableUnknownTable(t *testing.T) {
	c := openCat(t)
	err := c.UpdateTable(types.TableDescriptor{Name: "ghost", KeySchema: types.KeySchema{PK: "pk"}})
	if !errors.Is(err, types.ErrTableNotFound) {
		t.Fatalf("want ErrTableNotFound, got %v", err)
	}
}

func TestCreateTableValidatesCounterColumns(t *testing.T) {
	c := openCat(t)
	if err := c.Create(types.TableDescriptor{
		Name:      "Counters",
		KeySchema: types.KeySchema{PK: "id"},
		AttributeDefinitions: []types.AttributeDefinition{{
			Name: "count",
			Type: "counter",
		}},
	}); err != nil {
		t.Fatalf("create counter table: %v", err)
	}
	got, err := c.Describe("Counters")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if got.AttributeDefinitions[0].Type != types.AttributeTypeCounter {
		t.Fatalf("counter type = %q, want COUNTER", got.AttributeDefinitions[0].Type)
	}

	err = c.Create(types.TableDescriptor{
		Name:      "BadCounterKey",
		KeySchema: types.KeySchema{PK: "count"},
		AttributeDefinitions: []types.AttributeDefinition{{
			Name: "count",
			Type: types.AttributeTypeCounter,
		}},
	})
	if !errors.Is(err, types.ErrInvalidAttributeDefinition) {
		t.Fatalf("counter key error = %v, want ErrInvalidAttributeDefinition", err)
	}

	err = c.Create(types.TableDescriptor{
		Name:      "BadCounterPath",
		KeySchema: types.KeySchema{PK: "id"},
		AttributeDefinitions: []types.AttributeDefinition{{
			Name: "metrics.count",
			Type: types.AttributeTypeCounter,
		}},
	})
	if !errors.Is(err, types.ErrInvalidAttributeDefinition) {
		t.Fatalf("counter path error = %v, want ErrInvalidAttributeDefinition", err)
	}
}

func TestUpdateTablePersistsAcrossReload(t *testing.T) {
	c := openCat(t)
	td := types.TableDescriptor{Name: "T", KeySchema: types.KeySchema{PK: "pk"}}
	_ = c.Create(td)
	td.TTLAttribute = "exp"
	_ = c.UpdateTable(td)
	if err := c.Reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	got, _ := c.Describe("T")
	if got.TTLAttribute != "exp" {
		t.Fatalf("TTLAttribute after reload = %q", got.TTLAttribute)
	}
}

func TestCreateTableNormalizesStreamSpecification(t *testing.T) {
	c := openCat(t)
	td := types.TableDescriptor{
		Name:      "Events",
		KeySchema: types.KeySchema{PK: "pk"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled: true,
		},
	}
	if err := c.Create(td); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := c.Describe("Events")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if got.StreamSpecification == nil || !got.StreamSpecification.StreamEnabled {
		t.Fatalf("stream specification not enabled: %+v", got.StreamSpecification)
	}
	if got.StreamSpecification.StreamViewType != types.StreamViewTypeNewAndOldImages {
		t.Fatalf("stream view = %q, want %q", got.StreamSpecification.StreamViewType, types.StreamViewTypeNewAndOldImages)
	}
	if got.StreamStatus != types.StreamStatusEnabled {
		t.Fatalf("stream status = %q, want %q", got.StreamStatus, types.StreamStatusEnabled)
	}
	if got.LatestStreamLabel == "" {
		t.Fatal("LatestStreamLabel is empty")
	}
	if !strings.Contains(got.LatestStreamArn, "table/Events/stream/"+got.LatestStreamLabel) {
		t.Fatalf("LatestStreamArn = %q, label = %q", got.LatestStreamArn, got.LatestStreamLabel)
	}
}

func TestCreateTablePersistsStreamDescriptor(t *testing.T) {
	c := openCat(t)
	td := types.TableDescriptor{
		Name:      "Events",
		KeySchema: types.KeySchema{PK: "pk", SK: "sk"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: types.StreamViewTypeNewImage,
		},
	}
	if err := c.Create(td); err != nil {
		t.Fatalf("create: %v", err)
	}
	table, err := c.Describe("Events")
	if err != nil {
		t.Fatalf("describe table: %v", err)
	}
	if table.LatestStreamArn == "" {
		t.Fatal("LatestStreamArn is empty")
	}
	desc, err := c.DescribeStream(table.LatestStreamArn)
	if err != nil {
		t.Fatalf("describe stream: %v", err)
	}
	if desc.StreamArn != table.LatestStreamArn || desc.StreamLabel != table.LatestStreamLabel {
		t.Fatalf("stream identity = (%q, %q), want (%q, %q)", desc.StreamArn, desc.StreamLabel, table.LatestStreamArn, table.LatestStreamLabel)
	}
	if desc.TableName != "Events" {
		t.Fatalf("TableName = %q, want Events", desc.TableName)
	}
	if desc.StreamStatus != types.StreamStatusEnabled {
		t.Fatalf("StreamStatus = %q, want %q", desc.StreamStatus, types.StreamStatusEnabled)
	}
	if desc.StreamViewType != types.StreamViewTypeNewImage {
		t.Fatalf("StreamViewType = %q, want %q", desc.StreamViewType, types.StreamViewTypeNewImage)
	}
	if desc.CreationRequestDateTime == 0 {
		t.Fatal("CreationRequestDateTime is zero")
	}
	if desc.KeySchema != td.KeySchema {
		t.Fatalf("KeySchema = %+v, want %+v", desc.KeySchema, td.KeySchema)
	}
	if len(desc.Shards) != 1 {
		t.Fatalf("shards = %d, want 1", len(desc.Shards))
	}
	shard := desc.Shards[0]
	if shard.ShardID != types.StreamShardIDSingle {
		t.Fatalf("ShardID = %q, want %q", shard.ShardID, types.StreamShardIDSingle)
	}
	if shard.SequenceNumberRange.StartingSequenceNumber != "1" {
		t.Fatalf("StartingSequenceNumber = %q, want 1", shard.SequenceNumberRange.StartingSequenceNumber)
	}
	if shard.SequenceNumberRange.EndingSequenceNumber != "" {
		t.Fatalf("EndingSequenceNumber = %q, want empty", shard.SequenceNumberRange.EndingSequenceNumber)
	}
	streams, err := c.ListStreams("Events")
	if err != nil {
		t.Fatalf("list streams: %v", err)
	}
	if len(streams) != 1 || streams[0].StreamArn != desc.StreamArn {
		t.Fatalf("streams = %+v, want one descriptor for %q", streams, desc.StreamArn)
	}
}

func TestStreamDescriptorStartingSequenceUsesNextChangeIndex(t *testing.T) {
	c, db := openCatWithDB(t)
	if err := db.PutItem("Audit", types.KeySchema{PK: "pk"}, types.Item{
		"pk": {T: types.AttrS, S: "existing"},
	}); err != nil {
		t.Fatalf("put existing change: %v", err)
	}
	if err := c.Create(types.TableDescriptor{
		Name:      "Events",
		KeySchema: types.KeySchema{PK: "pk"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled: true,
		},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	table, err := c.Describe("Events")
	if err != nil {
		t.Fatalf("describe table: %v", err)
	}
	desc, err := c.DescribeStream(table.LatestStreamArn)
	if err != nil {
		t.Fatalf("describe stream: %v", err)
	}
	if got := desc.Shards[0].SequenceNumberRange.StartingSequenceNumber; got != "2" {
		t.Fatalf("StartingSequenceNumber = %q, want next change index 2", got)
	}
}

func TestUpdateTableDisableAndReenableStreamsCreatesNewDescriptor(t *testing.T) {
	c := openCat(t)
	if err := c.Create(types.TableDescriptor{
		Name:      "Events",
		KeySchema: types.KeySchema{PK: "pk"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: types.StreamViewTypeKeysOnly,
		},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	enabled, err := c.Describe("Events")
	if err != nil {
		t.Fatalf("describe enabled: %v", err)
	}
	firstARN := enabled.LatestStreamArn
	firstLabel := enabled.LatestStreamLabel
	enabled.StreamSpecification = nil
	if err := c.UpdateTable(enabled); err != nil {
		t.Fatalf("disable stream: %v", err)
	}
	disabledTable, err := c.Describe("Events")
	if err != nil {
		t.Fatalf("describe disabled table: %v", err)
	}
	if disabledTable.StreamSpecification != nil || disabledTable.LatestStreamArn != "" || disabledTable.StreamStatus != "" {
		t.Fatalf("disabled table kept active stream metadata: %+v", disabledTable)
	}
	firstDesc, err := c.DescribeStream(firstARN)
	if err != nil {
		t.Fatalf("describe disabled stream: %v", err)
	}
	if firstDesc.StreamStatus != types.StreamStatusDisabled {
		t.Fatalf("disabled stream status = %q, want %q", firstDesc.StreamStatus, types.StreamStatusDisabled)
	}
	if firstDesc.Shards[0].SequenceNumberRange.EndingSequenceNumber == "" {
		t.Fatal("disabled stream missing EndingSequenceNumber")
	}

	disabledTable.StreamSpecification = &types.StreamSpecification{
		StreamEnabled:  true,
		StreamViewType: types.StreamViewTypeOldImage,
	}
	if err := c.UpdateTable(disabledTable); err != nil {
		t.Fatalf("re-enable stream: %v", err)
	}
	reenabled, err := c.Describe("Events")
	if err != nil {
		t.Fatalf("describe reenabled: %v", err)
	}
	if reenabled.LatestStreamArn == "" || reenabled.LatestStreamLabel == "" {
		t.Fatalf("reenabled stream missing latest metadata: %+v", reenabled)
	}
	if reenabled.LatestStreamArn == firstARN {
		t.Fatalf("re-enabled stream ARN reused %q", firstARN)
	}
	if reenabled.LatestStreamLabel == firstLabel {
		t.Fatalf("re-enabled stream label reused %q", firstLabel)
	}
	secondDesc, err := c.DescribeStream(reenabled.LatestStreamArn)
	if err != nil {
		t.Fatalf("describe reenabled stream: %v", err)
	}
	if secondDesc.StreamStatus != types.StreamStatusEnabled {
		t.Fatalf("second stream status = %q, want %q", secondDesc.StreamStatus, types.StreamStatusEnabled)
	}
	if secondDesc.StreamViewType != types.StreamViewTypeOldImage {
		t.Fatalf("second stream view = %q, want %q", secondDesc.StreamViewType, types.StreamViewTypeOldImage)
	}
	if secondDesc.Shards[0].SequenceNumberRange.EndingSequenceNumber != "" {
		t.Fatalf("active second stream EndingSequenceNumber = %q, want empty", secondDesc.Shards[0].SequenceNumberRange.EndingSequenceNumber)
	}
	streams, err := c.ListStreams("Events")
	if err != nil {
		t.Fatalf("list streams: %v", err)
	}
	if len(streams) != 2 {
		t.Fatalf("stream count = %d, want 2: %+v", len(streams), streams)
	}
	if streams[0].StreamArn != firstARN || streams[0].StreamStatus != types.StreamStatusDisabled {
		t.Fatalf("first listed stream = %+v, want disabled %q", streams[0], firstARN)
	}
	if streams[1].StreamArn != reenabled.LatestStreamArn || streams[1].StreamStatus != types.StreamStatusEnabled {
		t.Fatalf("second listed stream = %+v, want enabled %q", streams[1], reenabled.LatestStreamArn)
	}
}

func TestUpdateTableRejectsStreamViewTypeChangeWhileEnabled(t *testing.T) {
	c := openCat(t)
	if err := c.Create(types.TableDescriptor{
		Name:      "Events",
		KeySchema: types.KeySchema{PK: "pk"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: types.StreamViewTypeKeysOnly,
		},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	td, err := c.Describe("Events")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	firstARN := td.LatestStreamArn
	td.StreamSpecification.StreamViewType = types.StreamViewTypeNewImage
	err = c.UpdateTable(td)
	if err == nil || !strings.Contains(err.Error(), "cannot be changed while stream is enabled") {
		t.Fatalf("want streamViewType validation error, got %v", err)
	}
	got, err := c.Describe("Events")
	if err != nil {
		t.Fatalf("describe after failed update: %v", err)
	}
	if got.StreamSpecification.StreamViewType != types.StreamViewTypeKeysOnly {
		t.Fatalf("stream view changed to %q", got.StreamSpecification.StreamViewType)
	}
	if got.LatestStreamArn != firstARN {
		t.Fatalf("LatestStreamArn changed to %q, want %q", got.LatestStreamArn, firstARN)
	}
}

func TestCreateTableRejectsInvalidStreamViewType(t *testing.T) {
	c := openCat(t)
	err := c.Create(types.TableDescriptor{
		Name:      "Events",
		KeySchema: types.KeySchema{PK: "pk"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: "FULL_IMAGE",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "streamViewType") {
		t.Fatalf("want streamViewType error, got %v", err)
	}
}

func TestCreateTableClearsDisabledStreamSpecification(t *testing.T) {
	c := openCat(t)
	if err := c.Create(types.TableDescriptor{
		Name:      "Events",
		KeySchema: types.KeySchema{PK: "pk"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  false,
			StreamViewType: types.StreamViewTypeNewImage,
		},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := c.Describe("Events")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if got.StreamSpecification != nil || got.LatestStreamArn != "" || got.LatestStreamLabel != "" || got.StreamStatus != "" {
		t.Fatalf("disabled stream metadata not cleared: %+v", got)
	}
}
