// Package ops hosts the Epic 7 CLI surface: create/describe/rebuild
// index, explain, top-k, cohort create/estimate, geo audience,
// dedup, freqcap, aggregate. Each command lives in its own file;
// Register wires them all onto the supplied root.
package ops

import "github.com/spf13/cobra"

// Register installs every Epic 7 subcommand onto root. The four
// grouped verbs (cohort, geo, dedup, freqcap) each get a parent
// command at root + their own sub-verbs.
func Register(root *cobra.Command) {
	registerCreateIndex(root)
	registerDescribeIndex(root)
	registerRebuildIndex(root)
	registerExplain(root)
	registerTopK(root)
	registerCohort(root)
	registerGeo(root)
	registerDedup(root)
	registerFreqCap(root)
	registerAggregate(root)
}
