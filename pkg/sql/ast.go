package sql

// Stmt is implemented by every top-level statement returned from
// Parse. Each concrete type drives a distinct planner branch.
type Stmt interface {
	stmt()
}

// SelectStmt is a SELECT query. The planner inspects WHERE to pick
// the access path (primary / GSI / spatial), then runs ORDER BY /
// LIMIT in the executor.
type SelectStmt struct {
	Table     string
	Columns   []string // nil → SELECT *
	IndexName string   // "" → primary; else USE INDEX (idx)
	Where     Expr     // nil → unconditional
	OrderBy   string
	OrderDesc bool
	Limit     int
}

// InsertStmt is INSERT INTO <table> (cols) VALUES (vals).
type InsertStmt struct {
	Table   string
	Columns []string
	Values  []Expr
}

// UpdateStmt is UPDATE <table> SET col = val, ... WHERE <pred>.
type UpdateStmt struct {
	Table       string
	Assignments []Assignment
	Where       Expr
}

// Assignment is "col = expr" inside UPDATE SET.
type Assignment struct {
	Column string
	Value  Expr
}

// DeleteStmt is DELETE FROM <table> WHERE <pred>.
type DeleteStmt struct {
	Table string
	Where Expr
}

// CreateTableStmt is the minimal table-creation form. Indexes and
// projections are managed through the descriptor APIs — keeping the
// SQL surface narrow here avoids a parser blow-up on DDL we already
// have a structured way to handle.
type CreateTableStmt struct {
	Table string
	PK    string
	SK    string // "" if no sort key
}

// DropTableStmt is DROP TABLE <name>.
type DropTableStmt struct {
	Table string
}

func (*SelectStmt) stmt()      {}
func (*InsertStmt) stmt()      {}
func (*UpdateStmt) stmt()      {}
func (*DeleteStmt) stmt()      {}
func (*CreateTableStmt) stmt() {}
func (*DropTableStmt) stmt()   {}

// Expr is the predicate / value-expression node interface.
type Expr interface {
	expr()
}

// ColumnRef references an attribute by name.
type ColumnRef struct {
	Name string
}

// Literal is a constant value with its SQL kind.
type Literal struct {
	// Kind is one of the LitKind constants. Value carries the canonical
	// string form (numbers stay as text to preserve arbitrary precision).
	Kind  LitKind
	Value string
	Bool  bool
}

// LitKind enumerates literal value kinds. Strings and numbers are the
// common cases; booleans and NULL round out the SQL standard.
type LitKind uint8

const (
	LitString LitKind = iota + 1
	LitNumber
	LitBool
	LitNull
)

// BinaryExpr models AND / OR plus the six comparison operators.
type BinaryExpr struct {
	Op    BinOp
	Left  Expr
	Right Expr
}

// BinOp identifies the operator on a BinaryExpr.
type BinOp uint8

const (
	BinAnd BinOp = iota + 1
	BinOr
	BinEq
	BinNeq
	BinLt
	BinLte
	BinGt
	BinGte
)

// NotExpr is NOT <inner>.
type NotExpr struct{ Inner Expr }

// BetweenExpr captures col BETWEEN lo AND hi.
type BetweenExpr struct {
	Value Expr
	Lo    Expr
	Hi    Expr
}

// FuncCall represents a function invocation. Used today for the
// spatial helpers ST_Within / ST_DWithin / BBox / Point, but the
// shape is generic enough to host other functions later.
type FuncCall struct {
	Name string
	Args []Expr
}

func (*ColumnRef) expr()   {}
func (*Literal) expr()     {}
func (*BinaryExpr) expr()  {}
func (*NotExpr) expr()     {}
func (*BetweenExpr) expr() {}
func (*FuncCall) expr()    {}
