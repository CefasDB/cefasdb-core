// Package runner contains the workload-phase orchestration for cefas-loadtest.
//
// The cmd/cefas-loadtest binary owns flag parsing, signal handling and gRPC
// dialing; it delegates the actual write / read / mixed phase loops here so
// main.go stays small (Issue #314, parent epic #307).
package runner

import "time"

// Config carries the subset of cefas-loadtest flags the phase runners need.
//
// It mirrors the flag-parsing `config` struct in main.go intentionally: the
// runner package never imports main, and main keeps its parseFlags result
// private. Callers populate a Config via plain field assignment.
type Config struct {
	Table               string
	Items               int64
	StartID             int64
	Reads               int64
	WriteDuration       time.Duration
	ReadDuration        time.Duration
	MixedDuration       time.Duration
	BatchSize           int
	Workers             int
	ReadWorkers         int
	WriteRate           int64
	ReadRate            int64
	Users               int64
	PayloadBytes        int
	PayloadMode         string
	RPCTimeout          time.Duration
	Progress            time.Duration
	LatencySampleRate   int64
	JSONOutput          string
	Label               string
	StrongRead          bool
	Keyspace            int64
	RouteAwareReads     bool
	RouteAwareReadNodes int
}
