package cmd

import (
	"context"
	"io"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
)

func runCommand(ctx context.Context, session *runtime.Session, args []string, in io.Reader, out, errOut io.Writer) error {
	if len(args) == 0 {
		return nil
	}
	cmdSession := runtime.NewSession(session.Options())
	root := rootWithSession(cmdSession, rootModeCommand)
	root.SetContext(runtime.WithSession(ctx, cmdSession))
	root.SetArgs(args)
	if in != nil {
		root.SetIn(in)
	}
	if out != nil {
		root.SetOut(out)
	}
	if errOut != nil {
		root.SetErr(errOut)
	}
	return root.Execute()
}
