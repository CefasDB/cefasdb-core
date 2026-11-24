//go:build integration

package fixture

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/pkg/client"
)

// TestFreshRF3TypedClientEvidence exercises the public typed-client boundary
// against two externally provisioned fresh RF3 clusters. The integration gate
// must remain closed until multi-shard stream checkpoints can bind a source
// summary to the recovery point.
func TestFreshRF3TypedClientEvidence(t *testing.T) {
	rawEndpoints := strings.TrimSpace(os.Getenv("CEFAS_RECOVERY_RF3_ENDPOINTS"))
	endpoints := strings.Split(rawEndpoints, ",")
	if rawEndpoints == "" || len(endpoints) != 2 {
		t.Fatal("CEFAS_RECOVERY_RF3_ENDPOINTS must contain exactly two comma-separated fresh cluster endpoints")
	}
	manifest, _ := testManifest(t)
	contract, ref := testContractWithTopology(t, 2, 2, 2, 3)

	for index, rawEndpoint := range endpoints {
		endpoint := strings.TrimSpace(rawEndpoint)
		if endpoint == "" {
			t.Fatalf("endpoint %d is empty", index)
		}
		t.Run("cluster-"+string(rune('1'+index)), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cli, err := client.Dial(ctx, endpoint, client.WithPlaintext())
			if err != nil {
				t.Fatalf("dial typed client for cluster %d failed", index+1)
			}
			defer func() {
				if closeErr := cli.Close(); closeErr != nil {
					t.Errorf("close typed client for cluster %d failed", index+1)
				}
			}()

			_, err = CollectSourceSummary(ctx, cli, CollectRequest{
				Manifest: manifest, Contract: contract, ContractRef: ref,
				SourceEnvironmentID: "source-env-v1",
				RecoveryPoint:       testRecoveryPoint("source-env-v1", []uint32{0, 1}, 42),
			})
			if err == nil || !IsCode(err, CodeUnsupportedTopology) {
				t.Fatalf("cluster %d did not fail with UNSUPPORTED_TOPOLOGY", index+1)
			}
		})
	}
}
