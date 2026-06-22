// Package server holds bootstrap helpers shared by cmd/cefasdb.
// It carries no runtime state — every export is a pure function over
// the parsed flag values and the loaded *config.Config.
package server

import (
	"strings"
	"time"

	"github.com/CefasDb/cefasdb/internal/config"
)

// OverlayFlags pushes flag values into the cfg struct. Only non-zero
// flag values overwrite — the YAML/env layer wins when the operator
// did not touch the flag. This is the cheap way to keep precedence
// "flag > env > yaml > default" without per-flag tracking of "user
// supplied this" bits.
func OverlayFlags(
	cfg *config.Config,
	dataDir, httpAddr string, fsync bool,
	raftBind, raftID, raftPath, raftStorePath string, raftBootstrap bool, raftPeers, raftHTTPPeers, raftGRPCPeers string,
	raftHeartbeatTimeout, raftElectionTimeout, raftLeaderLeaseTimeout, raftCommitTimeout, raftApplyTimeout time.Duration,
	raftSnapshotEntries uint64, raftLogCompression string,
	raftLogCompressionMinBytes int, raftLogCompressionMinSavingsRatio float64, raftLogCompressionSkipCooldown time.Duration,
	storageProfile, raftStorageProfile string,
	storageBlockCache int64, storageMemTableSize uint64, storageMemTableStopWrites int,
	storageMaxCompactions, storageL0Concurrency, storageL0Threshold int,
	storageL0FileThreshold, storageL0Stop, storageBytesPerSync, storageWALBytesPerSync int,
	storageLanes string, storageLaneReadWorkers, storageLaneWriteWorkers int,
	storageLaneReadQueue, storageLaneWriteQueue int,
	backpressureEnabled, backpressureReject bool,
	backpressureWarnL0, backpressureCriticalL0 int64,
	backpressureWarnDebt, backpressureCriticalDebt uint64,
	backpressureWarnReadAmp, backpressureCriticalReadAmp int,
	backpressureWarnDelay, backpressureCriticalDelay time.Duration,
	streamRetention time.Duration, streamRetentionMaxBytes int64,
	storageChangeLogMode string,
	identityJwks, identityIssuer, identityAudience string, identityClockSkew time.Duration,
	shardsN, replicationFactor int, muxAddr string,
	grpcAddr string, grpcRefl bool, tlsCert, tlsKey, mtlsCA string,
	metricsOff bool, tracingURL string, tracingInsecure bool,
	rebalancerEnabled bool, rebalancerMode string, rebalancerInterval, rebalancerMinInterval time.Duration,
	rebalancerMaxConcurrent, rebalancerMaxHotspots, rebalancerMinVoters int,
	rebalancerApplyTimeout time.Duration, rebalancerManualDir string,
	backupSchedulerEnabled, backupSchedulerDisabled, backupSchedulerDryRun bool,
	backupSchedulerInterval time.Duration, backupSchedulerNameTemplate, backupSchedulerTables string,
	backupSchedulerRetentionKeepLatest int, backupSchedulerRetentionMaxAge time.Duration, backupSchedulerRetentionDryRun bool,
) {
	if dataDir != "" && dataDir != "./cefas-data" {
		cfg.Data = dataDir
	} else if cfg.Data == "" {
		cfg.Data = dataDir
	}
	if httpAddr != "" && httpAddr != ":8080" {
		cfg.HTTP.Addr = httpAddr
	} else if cfg.HTTP.Addr == "" {
		cfg.HTTP.Addr = httpAddr
	}
	if fsync {
		cfg.Storage.FsyncOnCommit = true
	}
	if raftBind != "" {
		cfg.Raft.Bind = raftBind
	}
	if raftPath != "" {
		cfg.Raft.Path = raftPath
	}
	if raftStorePath != "" {
		cfg.Raft.StorePath = raftStorePath
	}
	if raftID != "" {
		cfg.Cluster.SelfID = raftID
	}
	if raftBootstrap {
		cfg.Cluster.Bootstrap = true
	}
	if raftPeers != "" {
		peers, _ := config.ParsePeers(raftPeers)
		cfg.Cluster.Peers = peers
	}
	if raftHTTPPeers != "" {
		hp, _ := config.ParsePeers(raftHTTPPeers)
		cfg.Cluster.HTTPPeers = hp
	}
	if raftGRPCPeers != "" {
		gp, _ := config.ParsePeers(raftGRPCPeers)
		cfg.Cluster.GRPCPeers = gp
	}
	if raftHeartbeatTimeout > 0 {
		cfg.Raft.HeartbeatTimeout = raftHeartbeatTimeout
	}
	if raftElectionTimeout > 0 {
		cfg.Raft.ElectionTimeout = raftElectionTimeout
	}
	if raftLeaderLeaseTimeout > 0 {
		cfg.Raft.LeaderLeaseTimeout = raftLeaderLeaseTimeout
	}
	if raftCommitTimeout > 0 {
		cfg.Raft.CommitTimeout = raftCommitTimeout
	}
	if raftApplyTimeout > 0 {
		cfg.Raft.ApplyTimeout = raftApplyTimeout
	}
	if raftSnapshotEntries > 0 {
		cfg.Raft.SnapshotEntries = raftSnapshotEntries
	}
	if raftLogCompression != "" {
		cfg.Raft.LogCompression = raftLogCompression
	}
	if raftLogCompressionMinBytes > 0 {
		cfg.Raft.LogCompressionMinBytes = raftLogCompressionMinBytes
	}
	if raftLogCompressionMinSavingsRatio >= 0 {
		cfg.Raft.LogCompressionMinSavingsRatio = raftLogCompressionMinSavingsRatio
	}
	if raftLogCompressionSkipCooldown >= 0 {
		cfg.Raft.LogCompressionSkipCooldown = raftLogCompressionSkipCooldown
	}
	if storageProfile != "" {
		cfg.Storage.Profile = storageProfile
	}
	if raftStorageProfile != "" {
		cfg.Storage.RaftProfile = raftStorageProfile
	}
	if storageBlockCache > 0 {
		cfg.Storage.BlockCacheSizeBytes = storageBlockCache
	}
	if storageMemTableSize > 0 {
		cfg.Storage.MemTableSizeBytes = storageMemTableSize
	}
	if storageMemTableStopWrites > 0 {
		cfg.Storage.MemTableStopWritesThreshold = storageMemTableStopWrites
	}
	if storageMaxCompactions > 0 {
		cfg.Storage.MaxConcurrentCompactions = storageMaxCompactions
	}
	if storageL0Concurrency > 0 {
		cfg.Storage.L0CompactionConcurrency = storageL0Concurrency
	}
	if storageL0Threshold > 0 {
		cfg.Storage.L0CompactionThreshold = storageL0Threshold
	}
	if storageL0FileThreshold > 0 {
		cfg.Storage.L0CompactionFileThreshold = storageL0FileThreshold
	}
	if storageL0Stop > 0 {
		cfg.Storage.L0StopWritesThreshold = storageL0Stop
	}
	if storageBytesPerSync > 0 {
		cfg.Storage.BytesPerSync = storageBytesPerSync
	}
	if storageWALBytesPerSync > 0 {
		cfg.Storage.WALBytesPerSync = storageWALBytesPerSync
	}
	if storageLanes != "" {
		cfg.Storage.Lanes = storageLanes
	}
	if storageLaneReadWorkers > 0 {
		cfg.Storage.LaneReadWorkers = storageLaneReadWorkers
	}
	if storageLaneWriteWorkers > 0 {
		cfg.Storage.LaneWriteWorkers = storageLaneWriteWorkers
	}
	if storageLaneReadQueue > 0 {
		cfg.Storage.LaneReadQueue = storageLaneReadQueue
	}
	if storageLaneWriteQueue > 0 {
		cfg.Storage.LaneWriteQueue = storageLaneWriteQueue
	}
	if backpressureEnabled {
		cfg.Storage.BackpressureEnabled = true
	}
	if backpressureReject {
		cfg.Storage.BackpressureRejectCritical = true
	}
	if backpressureWarnL0 > 0 {
		cfg.Storage.BackpressureWarningL0Files = backpressureWarnL0
	}
	if backpressureCriticalL0 > 0 {
		cfg.Storage.BackpressureCriticalL0Files = backpressureCriticalL0
	}
	if backpressureWarnDebt > 0 {
		cfg.Storage.BackpressureWarningDebt = backpressureWarnDebt
	}
	if backpressureCriticalDebt > 0 {
		cfg.Storage.BackpressureCriticalDebt = backpressureCriticalDebt
	}
	if backpressureWarnReadAmp > 0 {
		cfg.Storage.BackpressureWarningReadAmp = backpressureWarnReadAmp
	}
	if backpressureCriticalReadAmp > 0 {
		cfg.Storage.BackpressureCriticalReadAmp = backpressureCriticalReadAmp
	}
	if backpressureWarnDelay > 0 {
		cfg.Storage.BackpressureWarningDelay = backpressureWarnDelay
	}
	if backpressureCriticalDelay > 0 {
		cfg.Storage.BackpressureCriticalDelay = backpressureCriticalDelay
	}
	if streamRetention > 0 {
		cfg.Storage.StreamRetention = streamRetention
	}
	if streamRetentionMaxBytes > 0 {
		cfg.Storage.StreamRetentionMaxBytes = streamRetentionMaxBytes
	}
	if storageChangeLogMode != "" {
		cfg.Storage.ChangeLogMode = storageChangeLogMode
	}
	if identityJwks != "" {
		cfg.Identity.JwksURL = identityJwks
	}
	if identityIssuer != "" {
		cfg.Identity.Issuer = identityIssuer
	}
	if identityAudience != "" {
		cfg.Identity.Audience = identityAudience
	}
	if identityClockSkew != 30*time.Second {
		cfg.Identity.ClockSkew = identityClockSkew
	}
	if shardsN > 0 {
		cfg.Cluster.Shards = shardsN
	}
	if replicationFactor > 0 {
		cfg.Cluster.ReplicationFactor = replicationFactor
	}
	if muxAddr != "" {
		cfg.Cluster.MuxAddr = muxAddr
	}
	if grpcAddr != "" {
		cfg.GRPC.Addr = grpcAddr
	}
	if grpcRefl {
		cfg.GRPC.Reflection = true
	}
	if tlsCert != "" {
		cfg.GRPC.TLSCertPath = tlsCert
	}
	if tlsKey != "" {
		cfg.GRPC.TLSKeyPath = tlsKey
	}
	if mtlsCA != "" {
		cfg.GRPC.MTLSCAPath = mtlsCA
	}
	if metricsOff {
		cfg.Metrics.Enabled = false
	}
	if tracingURL != "" {
		cfg.Tracing.Endpoint = tracingURL
	}
	if !tracingInsecure {
		cfg.Tracing.Insecure = false
	}
	if rebalancerEnabled {
		cfg.Rebalancer.Enabled = true
	}
	if rebalancerMode != "" {
		cfg.Rebalancer.Mode = rebalancerMode
	}
	if rebalancerInterval > 0 {
		cfg.Rebalancer.Interval = rebalancerInterval
	}
	if rebalancerMinInterval > 0 {
		cfg.Rebalancer.MinInterval = rebalancerMinInterval
	}
	if rebalancerMaxConcurrent > 0 {
		cfg.Rebalancer.MaxConcurrentOperations = rebalancerMaxConcurrent
	}
	if rebalancerMaxHotspots > 0 {
		cfg.Rebalancer.MaxHotspots = rebalancerMaxHotspots
	}
	if rebalancerMinVoters > 0 {
		cfg.Rebalancer.MinVoters = rebalancerMinVoters
	}
	if rebalancerApplyTimeout > 0 {
		cfg.Rebalancer.ApplyTimeout = rebalancerApplyTimeout
	}
	if rebalancerManualDir != "" {
		cfg.Rebalancer.ManualPlanDir = rebalancerManualDir
	}
	if backupSchedulerEnabled {
		cfg.BackupScheduler.Enabled = true
	}
	if backupSchedulerDisabled {
		cfg.BackupScheduler.Enabled = false
	}
	if backupSchedulerDryRun {
		cfg.BackupScheduler.DryRun = true
	}
	if backupSchedulerInterval > 0 {
		cfg.BackupScheduler.Interval = backupSchedulerInterval
	}
	if backupSchedulerNameTemplate != "" {
		cfg.BackupScheduler.NameTemplate = backupSchedulerNameTemplate
	}
	if backupSchedulerTables != "" {
		cfg.BackupScheduler.Tables = SplitCSVFlag(backupSchedulerTables)
	}
	if backupSchedulerRetentionKeepLatest >= 0 {
		cfg.BackupScheduler.Retention.KeepLatest = backupSchedulerRetentionKeepLatest
		cfg.BackupScheduler.Retention.KeepLatestSet = true
	}
	if backupSchedulerRetentionMaxAge > 0 {
		cfg.BackupScheduler.Retention.MaxAge = backupSchedulerRetentionMaxAge
		cfg.BackupScheduler.Retention.MaxAgeSet = true
	}
	if backupSchedulerRetentionDryRun {
		cfg.BackupScheduler.Retention.DryRun = true
	}
}

// SplitCSVFlag splits a comma-separated CLI flag value into a trimmed
// slice. Blank entries are dropped, so "a, ,b" yields ["a","b"].
func SplitCSVFlag(in string) []string {
	if strings.TrimSpace(in) == "" {
		return nil
	}
	parts := strings.Split(in, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
