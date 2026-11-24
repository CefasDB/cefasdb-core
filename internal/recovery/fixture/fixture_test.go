package fixture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	recoverymodel "github.com/CefasDb/cefasdb/internal/recovery/model"
	"github.com/CefasDb/cefasdb/internal/recovery/repository"
	"github.com/CefasDb/cefasdb/pkg/client"
	"github.com/CefasDb/cefasdb/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLoadManifestStrictAndCanonical(t *testing.T) {
	manifest, raw := testManifest(t)
	if manifest.FixtureID != FixtureID || manifest.SchemaVersion != 1 || manifest.Seed != 20261031 {
		t.Fatalf("unexpected manifest identity: %+v", manifest)
	}
	if len(manifest.Tables) != 2 || len(manifest.ExpectedQueries) != 6 {
		t.Fatalf("unexpected manifest coverage: tables=%d queries=%d", len(manifest.Tables), len(manifest.ExpectedQueries))
	}
	stored, err := storedManifestBytes(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, stored) {
		t.Fatal("saved manifest is not the canonical stored form")
	}
	if _, digest, err := canonicalManifestBytes(manifest); err != nil || digest != manifest.SHA256 {
		t.Fatalf("manifest self digest mismatch: digest=%q err=%v", digest, err)
	}

	cases := map[string][]byte{
		"invalid utf8":      append(append([]byte(nil), raw...), 0xff),
		"crlf":              bytes.ReplaceAll(raw, []byte{'\n'}, []byte{'\r', '\n'}),
		"duplicate field":   bytes.Replace(raw, []byte(`{"fixtureId":`), []byte(`{"fixtureId":"duplicate","fixtureId":`), 1),
		"unknown field":     bytes.Replace(raw, []byte(`{"fixtureId":`), []byte(`{"unknown":true,"fixtureId":`), 1),
		"non integer":       bytes.Replace(raw, []byte(`"schemaVersion":1`), []byte(`"schemaVersion":1.0`), 1),
		"digest mismatch":   bytes.Replace(raw, []byte(`sha256:`), []byte(`sha256:0`), 1),
		"trailing document": append(append([]byte(nil), raw...), []byte(`{}`)...),
		"oversized":         bytes.Repeat([]byte{' '}, maxManifestBytes+1),
	}
	for name, candidate := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeManifest(candidate); err == nil || !IsCode(err, CodeInvalidManifest) {
				t.Fatalf("decodeManifest() error = %v, want INVALID_MANIFEST", err)
			}
		})
	}

	unsorted := manifest
	unsorted.Tables = append([]FixtureTable(nil), manifest.Tables...)
	unsorted.Tables[0], unsorted.Tables[1] = unsorted.Tables[1], unsorted.Tables[0]
	unsorted.SHA256 = ""
	_, unsorted.SHA256, err = canonicalManifestBytes(unsorted)
	if err != nil {
		t.Fatal(err)
	}
	unsortedRaw, err := storedManifestBytes(unsorted)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeManifest(unsortedRaw); err == nil {
		t.Fatal("unsorted table ids were accepted")
	}
}

func FuzzDecodeManifest(f *testing.F) {
	_, raw := testManifest(f)
	f.Add(raw)
	f.Add([]byte(`{}`))
	f.Add([]byte{0xff})
	f.Fuzz(func(t *testing.T, candidate []byte) {
		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("decodeManifest panicked: %v", recovered)
			}
		}()
		manifest, err := decodeManifest(candidate)
		if err != nil {
			return
		}
		canonical, err := storedManifestBytes(manifest)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(candidate, canonical) {
			t.Fatal("accepted bytes did not round-trip to one canonical form")
		}
		again, err := decodeManifest(canonical)
		if err != nil || !reflect.DeepEqual(manifest, again) {
			t.Fatalf("canonical form did not round-trip: err=%v", err)
		}
	})
}

func TestSeedConflictsBeforeMutation(t *testing.T) {
	manifest, _ := testManifest(t)
	contract, ref := testContract(t, 2, 2)
	request := SeedRequest{Manifest: manifest, Seed: manifest.Seed, Contract: contract, ContractRef: ref}

	tests := []struct {
		name  string
		setup func(*fakeFixture)
	}{
		{
			name: "incompatible index",
			setup: func(fake *fakeFixture) {
				users, _ := manifestTable(manifest, "recovery_users_v1")
				descriptor := descriptorFor(users)
				descriptor.GSIs[0].KeySchema.SK = "email"
				fake.installTable(users, descriptor, expectedTTL(users), expectedStream(users))
			},
		},
		{
			name: "incompatible ttl",
			setup: func(fake *fakeFixture) {
				events, _ := manifestTable(manifest, "recovery_events_v1")
				fake.installTable(events, descriptorFor(events), client.TTLState{}, expectedStream(events))
			},
		},
		{
			name: "transitional stream",
			setup: func(fake *fakeFixture) {
				events, _ := manifestTable(manifest, "recovery_events_v1")
				stream := expectedStream(events)
				stream.Status = types.StreamStatusEnabling
				stream.Enabled = false
				fake.installTable(events, descriptorFor(events), expectedTTL(events), stream)
			},
		},
		{
			name: "different fixture row",
			setup: func(fake *fakeFixture) {
				events, _ := manifestTable(manifest, "recovery_events_v1")
				fake.installTable(events, descriptorFor(events), expectedTTL(events), expectedStream(events))
				row := generateRow(manifest, events.ID, 0, 64)
				row["payload"] = stringValue("different")
				fake.putDirect(events.Name, row)
			},
		},
		{
			name: "unexpected extra row",
			setup: func(fake *fakeFixture) {
				events, _ := manifestTable(manifest, "recovery_events_v1")
				fake.installTable(events, descriptorFor(events), expectedTTL(events), expectedStream(events))
				fake.putDirect(events.Name, generateRow(manifest, events.ID, events.BaseRows, 64))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := newFakeFixture(contract.Workload)
			test.setup(fake)
			before := fake.mutationSnapshot()
			_, err := seedWithPorts(context.Background(), fake.seedPorts(), request)
			if err == nil || !IsCode(err, CodeFixtureConflict) {
				t.Fatalf("seedWithPorts() error = %v, want FIXTURE_CONFLICT", err)
			}
			after := fake.mutationSnapshot()
			if before != after {
				t.Fatalf("mutation occurred before conflict: before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestSeedCollectBindDeterministicAndIdempotent(t *testing.T) {
	manifest, _ := testManifest(t)
	contract, ref := testContract(t, 2, 2)
	request := SeedRequest{Manifest: manifest, Seed: manifest.Seed, Contract: contract, ContractRef: ref}
	// This is deterministic unit coverage. The build-tagged integration gate exercises the public typed-client boundary.
	first := newFakeFixture(contract.Workload)
	second := newFakeFixture(contract.Workload)
	second.reverseReads = true

	firstResult, err := seedWithPorts(context.Background(), first.seedPorts(), request)
	if err != nil {
		t.Fatal(err)
	}
	secondResult, err := seedWithPorts(context.Background(), second.seedPorts(), request)
	if err != nil {
		t.Fatal(err)
	}
	if firstResult.RowsAcknowledged != 10000 || secondResult.RowsAcknowledged != 10000 {
		t.Fatalf("seed row counts = %d and %d, want 10000", firstResult.RowsAcknowledged, secondResult.RowsAcknowledged)
	}
	for _, size := range append(first.batchSizesCopy(), second.batchSizesCopy()...) {
		if size < 1 || size > maxBatchItems {
			t.Fatalf("batch size = %d, want 1..%d", size, maxBatchItems)
		}
	}
	for _, size := range append(first.batchWireSizesCopy(), second.batchWireSizesCopy()...) {
		if size < 1 || size > maxBatchWireBytes {
			t.Fatalf("encoded batch size = %d, want 1..%d", size, maxBatchWireBytes)
		}
	}
	if first.streamCalls.Load() != 0 || second.streamCalls.Load() != 0 {
		t.Fatalf("RF3 fixture called single-store CDC endpoints: first=%d second=%d",
			first.streamCalls.Load(), second.streamCalls.Load())
	}
	events, _ := manifestTable(manifest, "recovery_events_v1")
	if descriptorFor(events).StorageClass != types.StorageClassDisk {
		t.Fatal("fixture descriptor did not request the catalog default disk storage class")
	}
	emptyClass := descriptorFor(events)
	emptyClass.StorageClass = ""
	if normalizeDescriptor(emptyClass).StorageClass != types.StorageClassDisk {
		t.Fatal("empty storage class did not normalize to disk")
	}

	collectRequest := CollectRequest{
		Manifest: manifest, Contract: contract, ContractRef: ref, SourceEnvironmentID: "source-env-v1",
		RecoveryPoint: testRecoveryPoint("source-env-v1", []uint32{0}, 42),
	}
	firstSummary, err := collectSourceSummary(context.Background(), first.collectPorts(), collectRequest)
	if err != nil {
		t.Fatal(err)
	}
	secondSummary, err := collectSourceSummary(context.Background(), second.collectPorts(), collectRequest)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(firstSummary, secondSummary) {
		t.Fatalf("independent in-memory clients produced different summaries:\nfirst=%+v\nsecond=%+v", firstSummary, secondSummary)
	}
	if first.streamCalls.Load() == 0 || second.streamCalls.Load() == 0 {
		t.Fatalf("collection skipped typed stream evidence: first=%d second=%d",
			first.streamCalls.Load(), second.streamCalls.Load())
	}
	visibility := firstSummary.Tables[0].TTLVisibility
	if visibility == nil || visibility.Attribute != manifest.TTLExpectation.Attribute ||
		visibility.ClockSource != contract.Measurement.ClockSource ||
		visibility.ObservationEpochSeconds != manifest.EpochSeconds+manifest.TTLExpectation.ObservationOffsetSeconds ||
		visibility.ExpectedExpiredRows != manifest.TTLExpectation.ExpectedExpiredBaseRows ||
		visibility.ExpectedVisibleRows != manifest.TTLExpectation.ExpectedVisibleBaseRows ||
		!digestPattern.MatchString(visibility.SHA256) {
		t.Fatalf("unexpected TTL visibility oracle: %+v", visibility)
	}
	if firstSummary.Tables[1].TTLVisibility != nil {
		t.Fatalf("non-TTL table has a visibility oracle: %+v", firstSummary.Tables[1].TTLVisibility)
	}
	if _, err := json.Marshal(firstSummary); err == nil {
		t.Fatal("unbound SourceSummary was serializable")
	}
	assertQueryCounts(t, firstSummary)
	if firstSummary.RequestedLogicalBytes != contract.Workload.LogicalBytes ||
		firstSummary.LogicalBytes <= firstSummary.RequestedLogicalBytes {
		t.Fatalf("logical bytes were not measured: requested=%d measured=%d",
			firstSummary.RequestedLogicalBytes, firstSummary.LogicalBytes)
	}
	basePlan, err := planScale(manifest, 1)
	if err != nil {
		t.Fatal(err)
	}
	scaledPlan, err := planScale(manifest, basePlan.LogicalBytes+1)
	if err != nil {
		t.Fatal(err)
	}
	if scaledPlan.LogicalBytes < basePlan.LogicalBytes+1 ||
		(scaledPlan.EventRows == basePlan.EventRows && scaledPlan.PayloadBytes == basePlan.PayloadBytes) {
		t.Fatalf("scale plan did not satisfy requested bytes: base=%+v scaled=%+v", basePlan, scaledPlan)
	}

	beforeRerun, err := summaryFingerprint(firstSummary)
	if err != nil {
		t.Fatal(err)
	}
	rerun, err := seedWithPorts(context.Background(), first.seedPorts(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(rerun.CreatedTables) != 0 || rerun.RowsAcknowledged != 10000 {
		t.Fatalf("unexpected rerun result: %+v", rerun)
	}
	afterSummary, err := collectSourceSummary(context.Background(), first.collectPorts(), collectRequest)
	if err != nil {
		t.Fatal(err)
	}
	afterRerun, err := summaryFingerprint(afterSummary)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(beforeRerun, afterRerun) {
		t.Fatal("idempotent rerun changed the source summary")
	}

	set := testBackupSet(t, firstSummary, false)
	catalog, err := BuildRecoveryPointCatalog(firstSummary, set)
	if err != nil {
		t.Fatal(err)
	}
	catalogBytes := catalog.Bytes()
	catalogSum := sha256.Sum256(catalogBytes)
	if catalog.Reference().SHA256 != "sha256:"+hex.EncodeToString(catalogSum[:]) ||
		catalog.Reference().SizeBytes != uint64(len(catalogBytes)) {
		t.Fatalf("catalog reference does not bind returned bytes: %+v", catalog.Reference())
	}
	upload := catalog.Upload("attempt-v1")
	uploaded, err := io.ReadAll(upload.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(uploaded, catalogBytes) || upload.SHA256 != catalog.Reference().SHA256 ||
		upload.SizeBytes != catalog.Reference().SizeBytes || upload.MediaType != repository.MediaTypeCatalog {
		t.Fatal("catalog upload does not contain the bound catalog bytes")
	}
	evidenceA, err := BindSourceEvidence(firstSummary, set)
	if err != nil {
		t.Fatal(err)
	}
	reordered := testBackupSet(t, firstSummary, true)
	evidenceB, err := BindSourceEvidence(firstSummary, reordered)
	if err != nil {
		t.Fatal(err)
	}
	var bytesA, bytesB bytes.Buffer
	if err := WriteSourceEvidence(&bytesA, evidenceA); err != nil {
		t.Fatal(err)
	}
	if err := WriteSourceEvidence(&bytesB, evidenceB); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytesA.Bytes(), bytesB.Bytes()) {
		t.Fatal("shard order changed bound evidence bytes")
	}
	if unsafeEvidenceBytes(bytesA.Bytes()) {
		t.Fatal("generated evidence contains a forbidden disclosure marker")
	}
	assertGoldenEvidence(t, bytesA.Bytes())
}

func TestCollectSourceSummaryRequiresPointBoundStreamFence(t *testing.T) {
	manifest, _ := testManifest(t)
	contract, ref := testContract(t, 2, 2)
	fake := newFakeFixture(contract.Workload)
	if _, err := seedWithPorts(context.Background(), fake.seedPorts(), SeedRequest{
		Manifest: manifest, Seed: manifest.Seed, Contract: contract, ContractRef: ref,
	}); err != nil {
		t.Fatal(err)
	}
	events, _ := manifestTable(manifest, "recovery_events_v1")
	fake.mu.Lock()
	stream := fake.streams[events.Name]
	stream.Shards[0].LatestSequenceNumber = "201"
	fake.streams[events.Name] = stream
	fake.mu.Unlock()

	request := CollectRequest{
		Manifest: manifest, Contract: contract, ContractRef: ref, SourceEnvironmentID: "source-env-v1",
		RecoveryPoint: testRecoveryPoint("source-env-v1", []uint32{0}, 42),
	}
	if _, err := collectSourceSummary(context.Background(), fake.collectPorts(), request); err == nil ||
		!IsCode(err, CodeEvidenceBinding) {
		t.Fatalf("collectSourceSummary() error = %v, want EVIDENCE_BINDING", err)
	}

	multiContract, multiRef := testContractWithTopology(t, 2, 2, 2, 3)
	multi := newFakeFixture(multiContract.Workload)
	multiRequest := CollectRequest{
		Manifest: manifest, Contract: multiContract, ContractRef: multiRef, SourceEnvironmentID: "source-env-v1",
		RecoveryPoint: testRecoveryPoint("source-env-v1", []uint32{0, 1}, 42),
	}
	if _, err := collectSourceSummary(context.Background(), multi.collectPorts(), multiRequest); err == nil ||
		!IsCode(err, CodeUnsupportedTopology) {
		t.Fatalf("multi-shard collectSourceSummary() error = %v, want UNSUPPORTED_TOPOLOGY", err)
	}
	if multi.streamCalls.Load() != 0 {
		t.Fatalf("unsupported multi-shard collection called stream APIs %d times", multi.streamCalls.Load())
	}
}

func TestRunWorkloadReplayReturnsCommittedResultAndSeedAcceptsMarker(t *testing.T) {
	manifest, _ := testManifest(t)
	contract, ref := testContract(t, 2, 2)
	fake := newFakeFixture(contract.Workload)
	seedRequest := SeedRequest{
		Manifest: manifest, Seed: manifest.Seed, Contract: contract, ContractRef: ref,
	}
	if _, err := seedWithPorts(context.Background(), fake.seedPorts(), seedRequest); err != nil {
		t.Fatal(err)
	}
	request := WorkloadRequest{
		Manifest: manifest, Contract: contract, ContractRef: ref, Phase: "replay-v1",
		Duration: time.Second, Workers: 1, QueueDepth: 2, RPCTimeout: 100 * time.Millisecond,
	}
	start := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	first, err := runWorkload(context.Background(), fake, newFakeClock(start), request)
	if err != nil {
		t.Fatal(err)
	}
	afterFirst := fake.mutationSnapshot()
	second, err := runWorkload(context.Background(), fake, newFakeClock(start), request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("replayed workload result changed:\nfirst=%+v\nsecond=%+v", first, second)
	}
	if afterSecond := fake.mutationSnapshot(); afterSecond != afterFirst {
		t.Fatalf("replayed committed writes mutated rows: first=%+v second=%+v", afterFirst, afterSecond)
	}
	if _, err := seedWithPorts(context.Background(), fake.seedPorts(), seedRequest); err != nil {
		t.Fatalf("seed rejected valid workload markers: %v", err)
	}
}

func TestSeedReconcilesCommittedUnacknowledgedBatch(t *testing.T) {
	manifest, _ := testManifest(t)
	contract, _ := testContract(t, 2, 2)
	fake := newFakeFixture(contract.Workload)
	events, _ := manifestTable(manifest, "recovery_events_v1")
	fake.installTable(events, descriptorFor(events), expectedTTL(events), expectedStream(events))
	fake.commitThenFail.Store(true)

	attempted, acknowledged, err := seedTable(context.Background(), fake, manifest, events, 26, 64)
	if err != nil {
		t.Fatal(err)
	}
	if attempted != 26 || acknowledged != 26 {
		t.Fatalf("counts = attempted %d acknowledged %d, want 26/26", attempted, acknowledged)
	}
	mutations := fake.mutationSnapshot()
	if mutations.BatchCalls != 2 || mutations.RowWrites != 26 {
		t.Fatalf("ambiguous commit was blindly retried: %+v", mutations)
	}
}

func TestSeedReconcilesCanceledBatch(t *testing.T) {
	manifest, _ := testManifest(t)
	contract, _ := testContract(t, 2, 2)
	events, _ := manifestTable(manifest, "recovery_events_v1")

	tests := []struct {
		name             string
		commit           bool
		wantError        bool
		wantAcknowledged uint64
		wantRowWrites    uint64
	}{
		{name: "committed before cancellation", commit: true, wantAcknowledged: 1, wantRowWrites: 1},
		{name: "canceled before commit", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := newFakeFixture(contract.Workload)
			fake.installTable(events, descriptorFor(events), expectedTTL(events), expectedStream(events))
			if test.commit {
				fake.commitThenCancel.Store(true)
			} else {
				fake.cancelBeforeCommit.Store(true)
			}
			attempted, acknowledged, err := seedTable(context.Background(), fake, manifest, events, 1, 64)
			if test.wantError {
				if err == nil || !IsCode(err, CodeFixtureConflict) {
					t.Fatalf("seedTable() error = %v, want FIXTURE_CONFLICT", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if attempted != 1 || acknowledged != test.wantAcknowledged {
				t.Fatalf("counts = attempted %d acknowledged %d, want 1/%d", attempted, acknowledged, test.wantAcknowledged)
			}
			mutations := fake.mutationSnapshot()
			if mutations.BatchCalls != 1 || mutations.RowWrites != test.wantRowWrites {
				t.Fatalf("canceled mutation was retried or misclassified: %+v", mutations)
			}
		})
	}
}

func TestPlanScaleAndBatchWireLimits(t *testing.T) {
	manifest, _ := testManifest(t)
	const contractBytes = uint64(100) << 30
	plan, err := planScale(manifest, contractBytes)
	if err != nil {
		t.Fatal(err)
	}
	if plan.LogicalBytes < contractBytes || plan.PayloadBytes > maxFixturePayloadBytes {
		t.Fatalf("100 GiB scale plan violates bounds: %+v", plan)
	}
	contract, _ := testContract(t, 2, 2)
	fake := newFakeFixture(contract.Workload)
	events, _ := manifestTable(manifest, "recovery_events_v1")
	fake.installTable(events, descriptorFor(events), expectedTTL(events), expectedStream(events))
	attempted, acknowledged, err := seedTable(
		context.Background(), fake, manifest, events, 7, maxFixturePayloadBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	if attempted != 7 || acknowledged != 7 || len(fake.batchWireSizesCopy()) < 2 {
		t.Fatalf("byte-bound batching did not split the large rows: attempted=%d acknowledged=%d sizes=%v",
			attempted, acknowledged, fake.batchWireSizesCopy())
	}
	for _, size := range fake.batchWireSizesCopy() {
		if size > maxBatchWireBytes {
			t.Fatalf("encoded batch size = %d, max = %d", size, maxBatchWireBytes)
		}
	}
	oversized := []client.BatchWriteOp{{Put: generateRow(manifest, events.ID, 100, maxBatchWireBytes)}}
	if err := writeBatchReconciled(context.Background(), fake, events, oversized); err == nil || !IsCode(err, CodeFixtureConflict) {
		t.Fatalf("oversized single item error = %v, want FIXTURE_CONFLICT", err)
	}
}

func TestCanonicalItemOrderIndependence(t *testing.T) {
	left := types.Item{
		"pk": stringValue("one"),
		"map": {T: types.AttrM, M: map[string]types.AttributeValue{
			"b": numberValue("2"), "a": stringValue("one"),
		}},
		"ss":   {T: types.AttrSS, SS: []string{"z", "a"}},
		"ns":   {T: types.AttrNS, NS: []string{"10", "2"}},
		"bs":   {T: types.AttrBS, BS: [][]byte{{2}, {1}}},
		"list": {T: types.AttrL, L: []types.AttributeValue{stringValue("first"), stringValue("second")}},
	}
	right := types.Item{
		"list": {T: types.AttrL, L: []types.AttributeValue{stringValue("first"), stringValue("second")}},
		"bs":   {T: types.AttrBS, BS: [][]byte{{1}, {2}}},
		"ns":   {T: types.AttrNS, NS: []string{"2", "10"}},
		"ss":   {T: types.AttrSS, SS: []string{"a", "z"}},
		"map": {T: types.AttrM, M: map[string]types.AttributeValue{
			"a": stringValue("one"), "b": numberValue("2"),
		}},
		"pk": stringValue("one"),
	}
	leftBytes, err := canonicalItemBytes(left)
	if err != nil {
		t.Fatal(err)
	}
	rightBytes, err := canonicalItemBytes(right)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(leftBytes, rightBytes) {
		t.Fatal("attribute, map, or set order changed canonical bytes")
	}
	right["list"] = types.AttributeValue{
		T: types.AttrL, L: []types.AttributeValue{stringValue("second"), stringValue("first")},
	}
	changed, err := canonicalItemBytes(right)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(leftBytes, changed) {
		t.Fatal("list order was not preserved")
	}
}

func TestEvidenceContainsNoRows(t *testing.T) {
	saved, err := os.ReadFile(filepath.Join("testdata", "source-evidence.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	if unsafeEvidenceBytes(saved) {
		t.Fatal("saved source evidence contains row, payload, credential, token, or URL material")
	}
	var evidence SourceEvidence
	if err := json.Unmarshal(saved, &evidence); err != nil {
		t.Fatal(err)
	}
	var emitted bytes.Buffer
	if err := WriteSourceEvidence(&emitted, evidence); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(saved, emitted.Bytes()) {
		t.Fatal("saved evidence is not the validated canonical output")
	}
}

func TestBindSourceEvidenceRejectsMismatchAndDisclosure(t *testing.T) {
	manifest, _ := testManifest(t)
	contract, ref := testContract(t, 2, 2)
	fake := newFakeFixture(contract.Workload)
	if _, err := seedWithPorts(context.Background(), fake.seedPorts(), SeedRequest{
		Manifest: manifest, Seed: manifest.Seed, Contract: contract, ContractRef: ref,
	}); err != nil {
		t.Fatal(err)
	}
	summary, err := collectSourceSummary(context.Background(), fake.collectPorts(), CollectRequest{
		Manifest: manifest, Contract: contract, ContractRef: ref, SourceEnvironmentID: "source-env-v1",
		RecoveryPoint: testRecoveryPoint("source-env-v1", []uint32{0}, 42),
	})
	if err != nil {
		t.Fatal(err)
	}
	valid := testBackupSet(t, summary, false)
	tests := []struct {
		name   string
		mutate func(*repository.BackupSet)
	}{
		{"contract", func(set *repository.BackupSet) { set.ContractRef.Revision++ }},
		{"source", func(set *repository.BackupSet) { set.SourceEnvironmentID = "other-source" }},
		{"shards", func(set *repository.BackupSet) { set.ExpectedShardIDs = nil }},
		{"stream checkpoint", func(set *repository.BackupSet) { set.RecoveryPoint.Shards[0].ChangeIndex = 0 }},
		{"tables", func(set *repository.BackupSet) {
			for i := range set.Artifacts {
				set.Artifacts[i].RowStats = set.Artifacts[i].RowStats[:1]
			}
		}},
		{"capture content", func(set *repository.BackupSet) {
			set.CatalogObject.SHA256 = testDigest("different-capture")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			set := valid
			set.ExpectedShardIDs = append([]uint32(nil), valid.ExpectedShardIDs...)
			set.RecoveryPoint.Shards = append([]repository.ShardPoint(nil), valid.RecoveryPoint.Shards...)
			set.Artifacts = cloneArtifacts(valid.Artifacts)
			test.mutate(&set)
			if _, err := BindSourceEvidence(summary, set); err == nil || !IsCode(err, CodeEvidenceBinding) {
				t.Fatalf("BindSourceEvidence() error = %v, want EVIDENCE_BINDING", err)
			}
		})
	}

	summaryTests := []struct {
		name   string
		mutate func(*SourceSummary)
	}{
		{"query digest", func(candidate *SourceSummary) {
			candidate.Queries[0].SHA256 = testDigest("different-query")
		}},
		{"descriptor digest", func(candidate *SourceSummary) {
			candidate.Tables[0].DescriptorSHA256 = testDigest("different-descriptor")
		}},
		{"topology digest", func(candidate *SourceSummary) {
			candidate.TopologySHA256 = testDigest("different-topology")
		}},
		{"replication factor", func(candidate *SourceSummary) {
			candidate.ReplicationFactor++
		}},
	}
	for _, test := range summaryTests {
		t.Run("catalog "+test.name, func(t *testing.T) {
			candidate := summary
			candidate.ShardIDs = append([]uint32(nil), summary.ShardIDs...)
			candidate.Tables = append([]TableSummary(nil), summary.Tables...)
			candidate.Queries = append([]QuerySummary(nil), summary.Queries...)
			test.mutate(&candidate)
			if _, err := BindSourceEvidence(candidate, valid); err == nil ||
				!IsCode(err, CodeEvidenceBinding) {
				t.Fatalf("BindSourceEvidence() error = %v, want EVIDENCE_BINDING", err)
			}
		})
	}

	evidence, err := BindSourceEvidence(summary, valid)
	if err != nil {
		t.Fatal(err)
	}
	evidence.SourceEnvironmentID = "secret"
	_, evidence.SHA256, err = canonicalEvidenceBytes(evidence)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := WriteSourceEvidence(&output, evidence); err == nil || !IsCode(err, CodeEvidenceRedaction) {
		t.Fatalf("WriteSourceEvidence() error = %v, want EVIDENCE_REDACTION", err)
	}
	if output.Len() != 0 {
		t.Fatal("rejected evidence wrote partial bytes")
	}
	for _, candidate := range [][]byte{
		[]byte(`{"authorization":"value"}`),
		[]byte(`{"value":"Bearer abc.def"}`),
		[]byte(`{"value":"eyJabcde.abcdef.abcdef"}`),
		[]byte(`{"value":"x-amz-signature=abc"}`),
		[]byte(`{"payload":"row-copy"}`),
		[]byte(`{"value":"https://signed.example.test/ref"}`),
	} {
		if !unsafeEvidenceBytes(candidate) {
			t.Fatalf("forbidden marker was accepted: %s", candidate)
		}
	}
}

func TestRunWorkloadDeterministicRatesAndCancellation(t *testing.T) {
	manifest, _ := testManifest(t)
	contract, ref := testContract(t, 2, 2)
	request := WorkloadRequest{
		Manifest: manifest, Contract: contract, ContractRef: ref, Phase: "steady-v1",
		Duration: time.Second, RPCTimeout: 100 * time.Millisecond, QueueDepth: 2,
	}

	run := func(workers int) WorkloadReport {
		t.Helper()
		fake := newFakeFixture(contract.Workload)
		events, _ := manifestTable(manifest, "recovery_events_v1")
		fake.installTable(events, descriptorFor(events), expectedTTL(events), expectedStream(events))
		for ordinal := uint64(0); ordinal < 2; ordinal++ {
			fake.putDirect(events.Name, generateRow(manifest, events.ID, ordinal, 64))
		}
		clock := newFakeClock(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))
		candidate := request
		candidate.Workers = workers
		report, err := runWorkload(context.Background(), fake, clock, candidate)
		if err != nil {
			t.Fatal(err)
		}
		return report
	}
	one := run(1)
	many := run(4)
	if one.OperationIDsSHA256 != many.OperationIDsSHA256 {
		t.Fatal("worker schedule changed deterministic operation ids")
	}
	for _, report := range []WorkloadReport{one, many} {
		if report.AttemptedReads != 2 || report.AcknowledgedReads != 2 ||
			report.AttemptedWrites != 2 || report.AcknowledgedWrites != 2 {
			t.Fatalf("unexpected workload counts: %+v", report)
		}
		if report.Elapsed != time.Second || report.AchievedReadsPerSec != 2 || report.AchievedWritesPerSec != 2 {
			t.Fatalf("unexpected fake-clock rates: elapsed=%s reads=%v writes=%v", report.Elapsed, report.AchievedReadsPerSec, report.AchievedWritesPerSec)
		}
	}
	if operationID(manifest.SHA256, "steady-v1", 0) == operationID(manifest.SHA256, "other-v1", 0) {
		t.Fatal("phase was not bound into operation id")
	}

	samples := make([]time.Duration, 100)
	for i := range samples {
		samples[i] = time.Duration(i+1) * time.Millisecond
	}
	latency := latencyPercentiles(samples)
	if latency.P50 != 50*time.Millisecond || latency.P95 != 95*time.Millisecond ||
		latency.P99 != 99*time.Millisecond || latency.Max != 100*time.Millisecond {
		t.Fatalf("unexpected percentile boundaries: %+v", latency)
	}

	tracking := &failingItemPort{}
	cancelRequest := request
	cancelRequest.Workers = 8
	cancelRequest.QueueDepth = 1
	_, err := runWorkload(context.Background(), tracking, newFakeClock(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)), cancelRequest)
	if err == nil || !IsCode(err, CodeWorkloadFailed) {
		t.Fatalf("runWorkload() error = %v, want WORKLOAD_FAILED", err)
	}
	if tracking.active.Load() != 0 {
		t.Fatalf("worker remained active after cancellation: %d", tracking.active.Load())
	}

	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	tracking = &failingItemPort{}
	_, err = runWorkload(canceledContext, tracking, newFakeClock(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)), cancelRequest)
	if err == nil || !IsCode(err, CodeWorkloadFailed) {
		t.Fatalf("canceled runWorkload() error = %v, want WORKLOAD_FAILED", err)
	}
	if tracking.active.Load() != 0 {
		t.Fatalf("worker remained active after parent cancellation: %d", tracking.active.Load())
	}
}

func TestWorkloadWriteBindsOperationIdentity(t *testing.T) {
	manifest, _ := testManifest(t)
	contract, ref := testContract(t, 2, 2)
	request := WorkloadRequest{
		Manifest: manifest, Contract: contract, ContractRef: ref, Phase: "identity-v1",
		Duration: time.Second, Workers: 1, QueueDepth: 1, RPCTimeout: 100 * time.Millisecond,
	}
	_, normalized, err := prepareWorkload(request)
	if err != nil {
		t.Fatal(err)
	}
	table, _ := manifestTable(manifest, "recovery_events_v1")
	operation := operationID(manifest.SHA256, request.Phase, 0)
	job := workloadJob{kind: workloadWrite, ordinal: 0, operation: operation}

	tests := []struct {
		name       string
		setup      func(*fakeFixture)
		batchCalls uint64
	}{
		{
			name: "failed before commit is retried",
			setup: func(fake *fakeFixture) {
				fake.failBeforeCommit.Store(true)
			},
			batchCalls: 2,
		},
		{
			name: "committed response loss is reconciled",
			setup: func(fake *fakeFixture) {
				fake.commitThenFail.Store(true)
			},
			batchCalls: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := newFakeFixture(contract.Workload)
			fake.installTable(table, descriptorFor(table), expectedTTL(table), expectedStream(table))
			baseRow := generateRow(manifest, table.ID, 0, normalized.scale.PayloadBytes)
			fake.putDirect(table.Name, baseRow)
			test.setup(fake)

			result := executeWorkloadJob(
				context.Background(),
				fake,
				newFakeClock(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)),
				normalized,
				job,
			)
			if result.err != nil {
				t.Fatal(result.err)
			}
			key, err := keyForItem(table.KeySchema, baseRow)
			if err != nil {
				t.Fatal(err)
			}
			stored, err := fake.StrongGet(context.Background(), table.Name, key)
			if err != nil {
				t.Fatal(err)
			}
			marker, ok := stored[workloadOperationAttribute]
			if !ok || marker.T != types.AttrS || marker.S != operation {
				t.Fatalf("stored operation marker = %+v, want %q", marker, operation)
			}
			mutations := fake.mutationSnapshot()
			if mutations.BatchCalls != test.batchCalls || mutations.RowWrites != 1 {
				t.Fatalf("ambiguous write accounting = %+v, want calls=%d writes=1", mutations, test.batchCalls)
			}
		})
	}
}

func testManifest(tb testing.TB) (FixtureManifest, []byte) {
	tb.Helper()
	path := filepath.Join("..", "..", "..", "fixtures", "recovery", "v1", "manifest.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	manifest, err := LoadManifest(path)
	if err != nil {
		tb.Fatal(err)
	}
	return manifest, raw
}

func testContract(tb testing.TB, reads, writes uint64) (recoverymodel.RecoveryContract, recoverymodel.ContractRef) {
	tb.Helper()
	return testContractWithTopology(tb, reads, writes, 1, 3)
}

func testContractWithTopology(
	tb testing.TB,
	reads uint64,
	writes uint64,
	shards uint32,
	replicationFactor uint32,
) (recoverymodel.RecoveryContract, recoverymodel.ContractRef) {
	tb.Helper()
	digest := func(value string) string {
		sum := sha256.Sum256([]byte(value))
		return "sha256:" + hex.EncodeToString(sum[:])
	}
	commit := func(value string) string {
		sum := sha256.Sum256([]byte(value))
		return hex.EncodeToString(sum[:20])
	}
	oldestCommit, candidateCommit := commit("oldest"), commit("candidate")
	contract := recoverymodel.RecoveryContract{
		ContractID: "recovery-v1", Revision: 1,
		ApprovedAt: time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC),
		ApprovedBy: []string{"data-owner", "operations"},
		ScopeKind:  recoverymodel.ScopeKindEnvironment, ScopeID: "recovery-qualification-v1",
		CanonicalPath: recoverymodel.CanonicalPathClusterBackupSetV1,
		Objectives: recoverymodel.RecoveryObjectives{
			RPOSeconds: 3600, RTOSeconds: 3600, PreflightSeconds: 300,
		},
		Measurement: recoverymodel.MeasurementBoundaries{
			DeclarationEvent:   "recovery-declared-at",
			RecoveryPointEvent: "complete-cross-shard-recovery-point-at",
			ReadyEvent:         "ready-and-reconciled-at", ClockSource: "utc",
		},
		Workload: recoverymodel.Workload{
			LogicalBytes: 1, Shards: shards, ReplicationFactor: replicationFactor,
			WritesPerSecond: writes, ReadsPerSecond: reads,
		},
		RequiredChecks: []string{"REC-01", "REC-02", "REC-03", "REC-04", "REC-05"},
		Retention: recoverymodel.RetentionPolicy{
			MinimumAgeSeconds: 604800, MinimumQualifiedSets: 3, EvidenceSeconds: 7776000,
		},
		ManifestVersions: []uint32{1},
		SupportedVersions: recoverymodel.SupportedVersions{
			Oldest: recoverymodel.ReleaseIdentity{
				Version: "1.4.0", ImageDigest: digest("oldest-image"), SourceCommit: oldestCommit,
				ProvenanceURI: "https://example.test/commit/" + oldestCommit,
			},
			Candidate: recoverymodel.ReleaseIdentity{
				Version: "1.5.0", ImageDigest: digest("candidate-image"), SourceCommit: candidateCommit,
				ProvenanceURI: "https://example.test/commit/" + candidateCommit,
			},
			RecoveryBranch: recoverymodel.RecoveryBranchForward,
		},
		Lease:              recoverymodel.LeasePolicy{DurationSeconds: 1800, RenewBeforeSeconds: 300},
		RepositoryPolicyID: "repository-policy-v1",
		Runbook:            recoverymodel.RunbookIdentity{Revision: "runbook-v1", SHA256: digest("runbook")},
	}
	raw, err := json.Marshal(contract)
	if err != nil {
		tb.Fatal(err)
	}
	var root map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		tb.Fatal(err)
	}
	delete(root, "sha256")
	canonical, err := json.Marshal(root)
	if err != nil {
		tb.Fatal(err)
	}
	sum := sha256.Sum256(append(canonical, '\n'))
	contract.SHA256 = "sha256:" + hex.EncodeToString(sum[:])
	if err := contract.Validate(); err != nil {
		tb.Fatalf("test contract is invalid: %v", err)
	}
	return contract, recoverymodel.ContractRef{
		ID: contract.ContractID, Revision: contract.Revision, SHA256: contract.SHA256,
	}
}

type mutationCounts struct {
	Creates    uint64
	TTLUpdates uint64
	BatchCalls uint64
	RowWrites  uint64
}

type fakeFixture struct {
	mu                 sync.RWMutex
	workload           recoverymodel.Workload
	descriptors        map[string]types.TableDescriptor
	ttls               map[string]client.TTLState
	streams            map[string]observedStream
	rows               map[string]map[string]types.Item
	mutations          mutationCounts
	batchSizes         []int
	batchWireSizes     []int
	reverseReads       bool
	streamCalls        atomic.Uint64
	commitThenFail     atomic.Bool
	commitThenCancel   atomic.Bool
	cancelBeforeCommit atomic.Bool
	failBeforeCommit   atomic.Bool
}

func newFakeFixture(workload recoverymodel.Workload) *fakeFixture {
	return &fakeFixture{
		workload: workload, descriptors: map[string]types.TableDescriptor{},
		ttls: map[string]client.TTLState{}, streams: map[string]observedStream{},
		rows: map[string]map[string]types.Item{},
	}
}

func (fake *fakeFixture) seedPorts() seedPorts {
	return seedPorts{schema: fake, ttl: fake, items: fake, topology: fake}
}

func (fake *fakeFixture) collectPorts() collectPorts {
	return collectPorts{
		schema: fake, ttl: fake, topology: fake, source: fake,
		streams: streamEvidencePorts{catalog: fake, records: fake},
	}
}

func (fake *fakeFixture) DescribeTable(_ context.Context, table string) (types.TableDescriptor, error) {
	fake.mu.RLock()
	defer fake.mu.RUnlock()
	descriptor, ok := fake.descriptors[table]
	if !ok {
		return types.TableDescriptor{}, types.ErrTableNotFound
	}
	return descriptorForCopy(descriptor), nil
}

func (fake *fakeFixture) CreateTable(_ context.Context, descriptor types.TableDescriptor) (types.TableDescriptor, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if _, exists := fake.descriptors[descriptor.Name]; exists {
		return types.TableDescriptor{}, status.Error(codes.AlreadyExists, "table exists")
	}
	if descriptor.StorageClass == "" {
		descriptor.StorageClass = types.StorageClassDisk
	}
	stream := observedStream{}
	if descriptor.StreamSpecification != nil && descriptor.StreamSpecification.StreamEnabled {
		descriptor.StreamStatus = types.StreamStatusEnabled
		stream = fakeStreamCheckpoints(observedStream{
			Status: types.StreamStatusEnabled, Enabled: true,
			ViewType: descriptor.StreamSpecification.StreamViewType,
		})
	}
	fake.descriptors[descriptor.Name] = descriptorForCopy(descriptor)
	fake.ttls[descriptor.Name] = client.TTLState{}
	fake.streams[descriptor.Name] = stream
	fake.rows[descriptor.Name] = map[string]types.Item{}
	fake.mutations.Creates++
	return descriptorForCopy(descriptor), nil
}

func (fake *fakeFixture) DescribeTTL(_ context.Context, table string) (client.TTLState, error) {
	fake.mu.RLock()
	defer fake.mu.RUnlock()
	state, ok := fake.ttls[table]
	if !ok {
		return client.TTLState{}, types.ErrTableNotFound
	}
	return state, nil
}

func (fake *fakeFixture) UpdateTTL(_ context.Context, table, attribute string, enabled bool) (client.TTLState, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if _, ok := fake.descriptors[table]; !ok {
		return client.TTLState{}, types.ErrTableNotFound
	}
	state := client.TTLState{Enabled: enabled, AttributeName: attribute}
	if !enabled {
		state.AttributeName = ""
	}
	fake.ttls[table] = state
	fake.mutations.TTLUpdates++
	return state, nil
}

func (fake *fakeFixture) StreamState(_ context.Context, table string) (observedStream, error) {
	fake.streamCalls.Add(1)
	fake.mu.RLock()
	defer fake.mu.RUnlock()
	stream, ok := fake.streams[table]
	if !ok {
		return observedStream{}, types.ErrTableNotFound
	}
	return normalizeStream(stream), nil
}

func (fake *fakeFixture) ListStreams(
	_ context.Context,
	opts client.ListStreamsOptions,
) (client.ListStreamsResult, error) {
	fake.streamCalls.Add(1)
	fake.mu.RLock()
	defer fake.mu.RUnlock()
	stream, ok := fake.streams[opts.TableName]
	if !ok {
		return client.ListStreamsResult{}, types.ErrTableNotFound
	}
	if !stream.Enabled {
		return client.ListStreamsResult{}, nil
	}
	return client.ListStreamsResult{Streams: []client.StreamSummary{{
		StreamArn: fakeStreamARN(opts.TableName), StreamLabel: "fixture-v1", TableName: opts.TableName,
	}}}, nil
}

func (fake *fakeFixture) DescribeStream(
	_ context.Context,
	streamARN string,
	_ client.DescribeStreamOptions,
) (client.StreamDescription, error) {
	fake.streamCalls.Add(1)
	table := strings.TrimPrefix(streamARN, "arn:cefasdb:fixture:")
	if table == streamARN {
		return client.StreamDescription{}, types.ErrStreamNotFound
	}
	fake.mu.RLock()
	defer fake.mu.RUnlock()
	stream, ok := fake.streams[table]
	if !ok || !stream.Enabled {
		return client.StreamDescription{}, types.ErrStreamNotFound
	}
	description := client.StreamDescription{
		StreamArn: streamARN, StreamLabel: "fixture-v1", StreamStatus: stream.Status,
		StreamViewType: stream.ViewType, TableName: table, KeySchema: fake.descriptors[table].KeySchema,
	}
	for _, shard := range stream.Shards {
		description.Shards = append(description.Shards, types.StreamShardDescriptor{
			ShardID: shard.ShardID,
			SequenceNumberRange: types.StreamSequenceNumberRange{
				StartingSequenceNumber: shard.StartingSequenceNumber,
				EndingSequenceNumber:   shard.EndingSequenceNumber,
			},
			LatestSequenceNumber: shard.LatestSequenceNumber,
		})
	}
	return description, nil
}

func (fake *fakeFixture) GetShardIterator(
	_ context.Context,
	opts client.GetShardIteratorOptions,
) (string, error) {
	fake.streamCalls.Add(1)
	if opts.ShardIteratorType != "AFTER_SEQUENCE_NUMBER" || opts.SequenceNumber == "" {
		return "", types.ErrStreamIteratorInvalid
	}
	return opts.StreamArn + "|" + opts.SequenceNumber, nil
}

func (fake *fakeFixture) GetRecords(
	_ context.Context,
	iterator string,
	_ client.GetRecordsOptions,
) (client.GetRecordsResult, error) {
	fake.streamCalls.Add(1)
	separator := strings.LastIndexByte(iterator, '|')
	if separator < 0 {
		return client.GetRecordsResult{}, types.ErrStreamIteratorInvalid
	}
	streamARN := iterator[:separator]
	table := strings.TrimPrefix(streamARN, "arn:cefasdb:fixture:")
	fake.mu.RLock()
	defer fake.mu.RUnlock()
	stream, ok := fake.streams[table]
	if !ok || len(stream.Shards) != 1 {
		return client.GetRecordsResult{}, types.ErrStreamNotFound
	}
	return client.GetRecordsResult{
		NextShardIterator:         iterator,
		LastScannedSequenceNumber: stream.Shards[0].LatestSequenceNumber,
	}, nil
}

func fakeStreamARN(table string) string {
	return "arn:cefasdb:fixture:" + table
}

func (fake *fakeFixture) ClusterStatus(context.Context) (client.ClusterStatus, error) {
	statusValue := client.ClusterStatus{
		ShardCount: int(fake.workload.Shards), PlacementVersion: 42,
	}
	for shard := uint32(0); shard < fake.workload.Shards; shard++ {
		placement := client.ShardPlacement{ID: shard, Epoch: 42}
		for replica := uint32(0); replica < fake.workload.ReplicationFactor; replica++ {
			placement.Voters = append(placement.Voters, "node-"+strconv.FormatUint(uint64(replica), 10))
		}
		statusValue.Shards = append(statusValue.Shards, placement)
	}
	return statusValue, nil
}

func (fake *fakeFixture) WriteBatch(_ context.Context, table string, operations []client.BatchWriteOp) error {
	wireBytes, err := batchWriteRequestSize(table, operations)
	if err != nil {
		return err
	}
	fake.mu.Lock()
	descriptor, ok := fake.descriptors[table]
	if !ok {
		fake.mu.Unlock()
		return types.ErrTableNotFound
	}
	if len(operations) > maxBatchItems || wireBytes > maxBatchWireBytes {
		fake.mu.Unlock()
		return errors.New("batch too large")
	}
	fake.mutations.BatchCalls++
	fake.batchSizes = append(fake.batchSizes, len(operations))
	fake.batchWireSizes = append(fake.batchWireSizes, wireBytes)
	if fake.cancelBeforeCommit.CompareAndSwap(true, false) {
		fake.mu.Unlock()
		return context.Canceled
	}
	if fake.failBeforeCommit.CompareAndSwap(true, false) {
		fake.mu.Unlock()
		return status.Error(codes.Unavailable, "request failed before commit")
	}
	for _, operation := range operations {
		key, err := keyForItem(descriptor.KeySchema, operation.Put)
		if err != nil {
			fake.mu.Unlock()
			return err
		}
		encoded, err := canonicalItemBytes(key)
		if err != nil {
			fake.mu.Unlock()
			return err
		}
		fake.rows[table][string(encoded)] = cloneItem(operation.Put)
		fake.mutations.RowWrites++
	}
	ambiguous := fake.commitThenFail.CompareAndSwap(true, false)
	canceled := fake.commitThenCancel.CompareAndSwap(true, false)
	fake.mu.Unlock()
	if canceled {
		return context.Canceled
	}
	if ambiguous {
		return status.Error(codes.Unavailable, "commit response lost")
	}
	return nil
}

func (fake *fakeFixture) StrongGet(_ context.Context, table string, key types.Item) (types.Item, error) {
	fake.mu.RLock()
	defer fake.mu.RUnlock()
	if _, ok := fake.descriptors[table]; !ok {
		return nil, types.ErrTableNotFound
	}
	encoded, err := canonicalItemBytes(key)
	if err != nil {
		return nil, err
	}
	return cloneItem(fake.rows[table][string(encoded)]), nil
}

func (fake *fakeFixture) ScanItems(_ context.Context, table string, visit func(types.Item) error) error {
	items, err := fake.snapshot(table)
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := visit(item); err != nil {
			return err
		}
	}
	return nil
}

func (fake *fakeFixture) QueryItems(_ context.Context, query QueryExpectation, visit func(types.Item) error) error {
	items, err := fake.snapshot(query.Table)
	if err != nil {
		return err
	}
	fake.mu.RLock()
	descriptor := fake.descriptors[query.Table]
	fake.mu.RUnlock()
	pk, sk, ok := queryKeys(descriptor, query.Index)
	if !ok {
		return errors.New("unknown index")
	}
	for _, item := range items {
		partitionBytes, partitionErr := canonicalItemBytes(types.Item{"value": item[pk]})
		if partitionErr != nil {
			return partitionErr
		}
		expectedBytes, expectedErr := canonicalItemBytes(types.Item{"value": fixtureValue(query.Partition)})
		if expectedErr != nil {
			return expectedErr
		}
		if !bytes.Equal(partitionBytes, expectedBytes) {
			continue
		}
		if query.SortLow != nil && compareAttribute(item[sk], fixtureValue(*query.SortLow)) < 0 {
			continue
		}
		if query.SortHigh != nil && compareAttribute(item[sk], fixtureValue(*query.SortHigh)) > 0 {
			continue
		}
		if err := visit(item); err != nil {
			return err
		}
	}
	return nil
}

func (fake *fakeFixture) snapshot(table string) ([]types.Item, error) {
	fake.mu.RLock()
	defer fake.mu.RUnlock()
	rows, ok := fake.rows[table]
	if !ok {
		return nil, types.ErrTableNotFound
	}
	keys := make([]string, 0, len(rows))
	for key := range rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if fake.reverseReads {
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	}
	items := make([]types.Item, 0, len(keys))
	for _, key := range keys {
		items = append(items, cloneItem(rows[key]))
	}
	return items, nil
}

func queryKeys(descriptor types.TableDescriptor, index string) (string, string, bool) {
	if index == "" {
		return descriptor.KeySchema.PK, descriptor.KeySchema.SK, true
	}
	for _, candidate := range descriptor.GSIs {
		if candidate.Name == index {
			return candidate.KeySchema.PK, candidate.KeySchema.SK, true
		}
	}
	for _, candidate := range descriptor.LSIs {
		if candidate.Name == index {
			return descriptor.KeySchema.PK, candidate.SK, true
		}
	}
	return "", "", false
}

func compareAttribute(left, right types.AttributeValue) int {
	if left.T == types.AttrN && right.T == types.AttrN {
		var leftNumber, rightNumber big.Int
		leftNumber.SetString(left.N, 10)
		rightNumber.SetString(right.N, 10)
		return leftNumber.Cmp(&rightNumber)
	}
	return strings.Compare(left.S, right.S)
}

func (fake *fakeFixture) installTable(
	table FixtureTable,
	descriptor types.TableDescriptor,
	ttl client.TTLState,
	stream observedStream,
) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if descriptor.StorageClass == "" {
		descriptor.StorageClass = types.StorageClassDisk
	}
	if stream.Enabled || stream.Status != "" {
		descriptor.StreamSpecification = &types.StreamSpecification{
			StreamEnabled:  stream.Enabled,
			StreamViewType: stream.ViewType,
		}
		descriptor.StreamStatus = stream.Status
	}
	fake.descriptors[table.Name] = descriptorForCopy(descriptor)
	fake.ttls[table.Name] = ttl
	fake.streams[table.Name] = fakeStreamCheckpoints(stream)
	fake.rows[table.Name] = map[string]types.Item{}
}

func fakeStreamCheckpoints(stream observedStream) observedStream {
	if !stream.Enabled || len(stream.Shards) != 0 {
		return stream
	}
	if stream.Status == "" {
		stream.Status = types.StreamStatusEnabled
	}
	stream.Shards = []observedStreamShard{{
		ShardID: "stream-shard-0", StartingSequenceNumber: "1", LatestSequenceNumber: "200",
	}}
	return stream
}

func (fake *fakeFixture) putDirect(table string, item types.Item) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	descriptor := fake.descriptors[table]
	key, _ := keyForItem(descriptor.KeySchema, item)
	encoded, _ := canonicalItemBytes(key)
	fake.rows[table][string(encoded)] = cloneItem(item)
}

func (fake *fakeFixture) mutationSnapshot() mutationCounts {
	fake.mu.RLock()
	defer fake.mu.RUnlock()
	return fake.mutations
}

func (fake *fakeFixture) batchSizesCopy() []int {
	fake.mu.RLock()
	defer fake.mu.RUnlock()
	return append([]int(nil), fake.batchSizes...)
}

func (fake *fakeFixture) batchWireSizesCopy() []int {
	fake.mu.RLock()
	defer fake.mu.RUnlock()
	return append([]int(nil), fake.batchWireSizes...)
}

func descriptorForCopy(descriptor types.TableDescriptor) types.TableDescriptor {
	out := descriptor
	out.AttributeDefinitions = append([]types.AttributeDefinition(nil), descriptor.AttributeDefinitions...)
	out.GSIs = append([]types.GSIDescriptor(nil), descriptor.GSIs...)
	out.LSIs = append([]types.LSIDescriptor(nil), descriptor.LSIs...)
	out.SpatialIndexes = append([]types.SpatialIndexDescriptor(nil), descriptor.SpatialIndexes...)
	for i := range out.SpatialIndexes {
		out.SpatialIndexes[i].Attributes = append([]string(nil), descriptor.SpatialIndexes[i].Attributes...)
		out.SpatialIndexes[i].Ranges = append([]types.NumRange(nil), descriptor.SpatialIndexes[i].Ranges...)
	}
	out.TextIndexes = append([]types.TextIndexDescriptor(nil), descriptor.TextIndexes...)
	out.MaterializedViews = append([]string(nil), descriptor.MaterializedViews...)
	out.GlobalIndexes = append([]string(nil), descriptor.GlobalIndexes...)
	if descriptor.StreamSpecification != nil {
		stream := *descriptor.StreamSpecification
		out.StreamSpecification = &stream
	}
	return out
}

func cloneItem(item types.Item) types.Item {
	if item == nil {
		return nil
	}
	out := make(types.Item, len(item))
	for key, value := range item {
		out[key] = cloneAttribute(value)
	}
	return out
}

func cloneAttribute(value types.AttributeValue) types.AttributeValue {
	out := value
	out.B = append([]byte(nil), value.B...)
	out.SS = append([]string(nil), value.SS...)
	out.NS = append([]string(nil), value.NS...)
	if value.BS != nil {
		out.BS = make([][]byte, len(value.BS))
		for i := range value.BS {
			out.BS[i] = append([]byte(nil), value.BS[i]...)
		}
	}
	if value.L != nil {
		out.L = make([]types.AttributeValue, len(value.L))
		for i := range value.L {
			out.L[i] = cloneAttribute(value.L[i])
		}
	}
	if value.M != nil {
		out.M = make(map[string]types.AttributeValue, len(value.M))
		for key, nested := range value.M {
			out.M[key] = cloneAttribute(nested)
		}
	}
	out.Vec = append([]float64(nil), value.Vec...)
	return out
}

func assertQueryCounts(t *testing.T, summary SourceSummary) {
	t.Helper()
	want := map[string]uint64{
		"events.gsi.type-v1": 2250, "events.lsi.expiry-window-v1": 9,
		"events.primary.user-v1": 9, "users.gsi.region-v1": 250,
		"users.lsi.email-range-v1": 10, "users.primary.account-v1": 10,
	}
	for _, query := range summary.Queries {
		if query.Count != want[query.ID] || !digestPattern.MatchString(query.SHA256) {
			t.Fatalf("query %s = count %d digest %q", query.ID, query.Count, query.SHA256)
		}
		delete(want, query.ID)
	}
	if len(want) != 0 {
		t.Fatalf("missing query summaries: %v", want)
	}
	for _, table := range summary.Tables {
		switch table.Table {
		case "recovery_events_v1":
			if table.PhysicalRows != 9000 {
				t.Fatalf("event rows = %d, want 9000", table.PhysicalRows)
			}
		case "recovery_users_v1":
			if table.PhysicalRows != 1000 {
				t.Fatalf("user rows = %d, want 1000", table.PhysicalRows)
			}
		default:
			t.Fatalf("unexpected table %q", table.Table)
		}
		if !digestPattern.MatchString(table.CanonicalItemSHA256) || len(table.CanonicalItemFNV1A64) != 16 {
			t.Fatalf("invalid table digests: %+v", table)
		}
	}
}

func summaryFingerprint(summary SourceSummary) ([]byte, error) {
	type serializable SourceSummary
	return json.Marshal(serializable(summary))
}

func testRecoveryPoint(
	sourceEnvironmentID string,
	shardIDs []uint32,
	placementEpoch uint64,
) repository.RecoveryPoint {
	point := repository.RecoveryPoint{
		BarrierID: "barrier-v1", SourceEnvironmentID: sourceEnvironmentID,
		PlacementEpoch:  placementEpoch,
		AdmittedAt:      time.Date(2026, 11, 23, 11, 0, 0, 0, time.UTC),
		RecoveryPointAt: time.Date(2026, 11, 23, 11, 1, 0, 0, time.UTC),
	}
	for _, shard := range shardIDs {
		point.Shards = append(point.Shards, repository.ShardPoint{
			ShardID: shard, PlacementEpoch: placementEpoch,
			ReplicaID:        "replica-" + strconv.FormatUint(uint64(shard), 10),
			RaftAppliedIndex: uint64(shard) + 100, ChangeIndex: uint64(shard) + 200,
		})
	}
	return point
}

func testBackupSet(t *testing.T, summary SourceSummary, reverse bool) repository.BackupSet {
	t.Helper()
	shards := append([]uint32(nil), summary.ShardIDs...)
	if reverse {
		sort.Slice(shards, func(i, j int) bool { return shards[i] > shards[j] })
	}
	point := testRecoveryPoint(summary.SourceEnvironmentID, shards, summary.PlacementEpoch)
	set := repository.BackupSet{
		SchemaVersion: 1, ScopeID: summary.ScopeID, BackupID: "backup-v1",
		ContractRef: summary.ContractRef, SourceEnvironmentID: summary.SourceEnvironmentID,
		ExpectedShardIDs: append([]uint32(nil), shards...),
		ManifestSHA256:   testDigest("backup-set-v1"),
	}
	for shardIndex, shard := range shards {
		replica := "replica-" + strconv.FormatUint(uint64(shard), 10)
		stats := make([]repository.TableStat, 0, len(summary.Tables))
		for _, table := range summary.Tables {
			rows := table.PhysicalRows / uint64(len(shards))
			if uint64(shardIndex) < table.PhysicalRows%uint64(len(shards)) {
				rows++
			}
			rawDigest := strings.TrimPrefix(testDigest("raw-pebble:"+table.Table+":"+strconv.Itoa(shardIndex)), "sha256:")[:16]
			stats = append(stats, repository.TableStat{
				Table: table.Table, Rows: rows, FNV1A64: rawDigest,
			})
		}
		if reverse {
			sort.Slice(stats, func(i, j int) bool { return stats[i].Table > stats[j].Table })
		}
		set.Artifacts = append(set.Artifacts, repository.ShardArtifact{
			ShardID: shard, PlacementEpoch: summary.PlacementEpoch, ReplicaID: replica, RowStats: stats,
		})
	}
	set.RecoveryPoint = point
	catalog, err := BuildRecoveryPointCatalog(summary, set)
	if err != nil {
		t.Fatal(err)
	}
	set.CatalogObject = catalog.Reference()
	return set
}

func cloneArtifacts(in []repository.ShardArtifact) []repository.ShardArtifact {
	out := append([]repository.ShardArtifact(nil), in...)
	for i := range out {
		out[i].RowStats = append([]repository.TableStat(nil), in[i].RowStats...)
	}
	return out
}

func testDigest(label string) string {
	sum := sha256.Sum256([]byte(label))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func assertGoldenEvidence(t *testing.T, generated []byte) {
	t.Helper()
	path := filepath.Join("testdata", "source-evidence.golden.json")
	if os.Getenv("UPDATE_RECOVERY_GOLDEN") == "1" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, generated, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(saved, generated) {
		t.Fatalf("source evidence golden mismatch\nsaved: %s\ngenerated: %s", saved, generated)
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(now time.Time) *fakeClock { return &fakeClock{now: now} }

func (clock *fakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeClock) NewTimer(duration time.Duration) workloadTimer {
	clock.mu.Lock()
	if duration > 0 {
		clock.now = clock.now.Add(duration)
	}
	now := clock.now
	clock.mu.Unlock()
	channel := make(chan time.Time, 1)
	channel <- now
	return fakeTimer{channel: channel}
}

type fakeTimer struct{ channel <-chan time.Time }

func (timer fakeTimer) C() <-chan time.Time { return timer.channel }
func (fakeTimer) Stop() bool                { return true }

type failingItemPort struct{ active atomic.Int64 }

func (port *failingItemPort) WriteBatch(context.Context, string, []client.BatchWriteOp) error {
	port.active.Add(1)
	defer port.active.Add(-1)
	return status.Error(codes.Unavailable, "ambiguous")
}

func (port *failingItemPort) StrongGet(context.Context, string, types.Item) (types.Item, error) {
	port.active.Add(1)
	defer port.active.Add(-1)
	return nil, status.Error(codes.Unavailable, "unavailable")
}
