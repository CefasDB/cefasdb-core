package ddb

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"testing"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/api"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
)

type streamCLIFixture struct {
	addr string
}

func newStreamCLIFixture(t *testing.T) streamCLIFixture {
	t.Helper()
	db, err := storage.Open(storage.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("storage open: %v", err)
	}
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gsrv := grpc.NewServer()
	cefaspb.RegisterCefasServer(gsrv, api.NewGRPCServer(db, cat, nil))
	go func() { _ = gsrv.Serve(ln) }()
	t.Cleanup(func() {
		gsrv.GracefulStop()
		_ = ln.Close()
		_ = db.Close()
	})
	return streamCLIFixture{addr: ln.Addr().String()}
}

func runStreamCLI(t *testing.T, fx streamCLIFixture, args ...string) []byte {
	t.Helper()
	oldFlags := runtime.Flags
	defer func() { runtime.Flags = oldFlags }()
	runtime.Flags.Endpoint = fx.addr
	runtime.Flags.Insecure = true
	runtime.Flags.Output = "json"

	root := &cobra.Command{
		Use:           "cefas",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	Register(root)
	var out bytes.Buffer
	var errOut bytes.Buffer
	root.SetContext(context.Background())
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(args)
	if err := root.Execute(); err != nil {
		t.Fatalf("cefas %v: %v\nstderr:\n%s", args, err, errOut.String())
	}
	return out.Bytes()
}

func TestCLIStreamsEndToEndSmoke(t *testing.T) {
	fx := newStreamCLIFixture(t)

	runStreamCLI(t, fx,
		"create-table",
		"--table-name", "Events",
		"--attribute-definitions", "AttributeName=pk,AttributeType=S",
		"--key-schema", "AttributeName=pk,KeyType=HASH",
		"--stream-specification", "StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES",
	)
	runStreamCLI(t, fx,
		"put-item",
		"--table-name", "Events",
		"--item", `{"pk":{"S":"event-1"},"status":{"S":"new"}}`,
	)
	runStreamCLI(t, fx,
		"put-item",
		"--table-name", "Events",
		"--item", `{"pk":{"S":"event-1"},"status":{"S":"updated"}}`,
	)
	runStreamCLI(t, fx,
		"delete-item",
		"--table-name", "Events",
		"--key", `{"pk":{"S":"event-1"}}`,
	)

	var listed listStreamsOutput
	if err := json.Unmarshal(runStreamCLI(t, fx, "list-streams", "--table-name", "Events", "--limit", "10"), &listed); err != nil {
		t.Fatalf("decode list-streams: %v", err)
	}
	if len(listed.Streams) != 1 || listed.Streams[0].TableName != "Events" || listed.Streams[0].StreamArn == "" {
		t.Fatalf("list-streams output = %+v", listed)
	}

	var described describeStreamOutput
	if err := json.Unmarshal(runStreamCLI(t, fx,
		"describe-stream",
		"--stream-arn", listed.Streams[0].StreamArn,
		"--limit", "1",
	), &described); err != nil {
		t.Fatalf("decode describe-stream: %v", err)
	}
	desc := described.StreamDescription
	if desc.StreamArn != listed.Streams[0].StreamArn ||
		desc.StreamViewType != "NEW_AND_OLD_IMAGES" ||
		len(desc.Shards) != 1 ||
		desc.Shards[0].ShardId == "" {
		t.Fatalf("describe-stream output = %+v", described)
	}

	var iteratorResp struct {
		ShardIterator string `json:"ShardIterator"`
	}
	if err := json.Unmarshal(runStreamCLI(t, fx,
		"get-shard-iterator",
		"--stream-arn", desc.StreamArn,
		"--shard-id", desc.Shards[0].ShardId,
		"--shard-iterator-type", "TRIM_HORIZON",
	), &iteratorResp); err != nil {
		t.Fatalf("decode get-shard-iterator: %v", err)
	}
	if iteratorResp.ShardIterator == "" {
		t.Fatal("get-shard-iterator returned empty token")
	}

	var records getRecordsOutput
	if err := json.Unmarshal(runStreamCLI(t, fx,
		"get-records",
		"--shard-iterator", iteratorResp.ShardIterator,
		"--limit", "10",
	), &records); err != nil {
		t.Fatalf("decode get-records: %v", err)
	}
	if len(records.Records) != 3 || records.NextShardIterator == "" {
		t.Fatalf("get-records output = %+v", records)
	}
	if records.Records[0].EventName != "INSERT" ||
		records.Records[1].EventName != "MODIFY" ||
		records.Records[2].EventName != "REMOVE" {
		t.Fatalf("record event names = %+v", records.Records)
	}
	if records.Records[1].DynamoDB.NewImage["status"].S == nil ||
		*records.Records[1].DynamoDB.NewImage["status"].S != "updated" ||
		records.Records[1].DynamoDB.OldImage["status"].S == nil ||
		*records.Records[1].DynamoDB.OldImage["status"].S != "new" {
		t.Fatalf("modify images = %+v", records.Records[1].DynamoDB)
	}
}
