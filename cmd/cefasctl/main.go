// cefasctl is the command-line interface for the cefas database.
// Surface mirrors AWS DynamoDB CLI so scripts written against
// `aws dynamodb` can be ported by replacing the command name.
package main

import "github.com/osvaldoandrade/cefas/cmd/cefasctl/cmd"

func main() { cmd.Execute() }
