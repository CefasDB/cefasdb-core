// Package ddb hosts the DynamoDB-style subcommands. The Register
// function wires every implemented command onto the supplied root.
package ddb

import "github.com/spf13/cobra"

// Register installs every implemented DDB-style subcommand.
// PRs 3 + 4 add table-management, item, query, batch and PartiQL
// commands here.
func Register(root *cobra.Command) {
	registerListTables(root)
	registerCreateTable(root)
	registerDeleteTable(root)
	registerDescribeTable(root)
	registerGetItem(root)
	registerPutItem(root)
	registerDeleteItem(root)
	registerQuery(root)
	registerBatchGetItem(root)
	registerBatchWriteItem(root)
	registerExecuteStatement(root)
	registerUpdateTimeToLive(root)
	registerDescribeTimeToLive(root)
	registerScan(root)
	registerUpdateItem(root)
	registerCreateBackup(root)
	registerListBackups(root)
	registerDeleteBackup(root)
	registerApplyBackupRetention(root)
	registerRestoreTableFromBackup(root)
	registerCompactTable(root)
	registerTransactWriteItems(root)
	registerTransactGetItems(root)
}
