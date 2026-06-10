package sql

// Compile parses + plans `src` in one shot. Use Compile when the
// caller has no intermediate inspection to do; Parse + Plan are the
// finer-grained entry points (tests use both).
func Compile(src string, cat Catalog) (Plan, error) {
	stmt, err := Parse(src)
	if err != nil {
		return nil, err
	}
	return PlanStmt(stmt, cat)
}

// Run is the full pipeline: parse → plan → execute. The convenience
// wrapper exposed by the HTTP + gRPC handlers.
func Run(src string, cat Catalog, mut CatalogMutator, store Storage) (*Result, error) {
	plan, err := Compile(src, cat)
	if err != nil {
		return nil, err
	}
	ex := &Executor{Storage: store, Catalog: mut}
	return ex.Execute(plan)
}
