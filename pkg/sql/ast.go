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

// InsertStmt is INSERT INTO <table> (cols) VALUES (vals) [IF expr].
type InsertStmt struct {
	Table   string
	Columns []string
	Values  []Expr
	// If, when non-nil, is the optional ConditionExpression
	// evaluated against the prior item before write.
	If Expr
}

// UpdateStmt is UPDATE <table> SET <action> [, ...] WHERE <pred>
// [IF <cond>].
//
// Each action is an Assignment carrying the kind (SET/REMOVE/ADD/
// DELETE) and the operand. The executor reads the prior row, applies
// each action in order, and writes the merged result back through
// storage.PutItemWith so GSI + LSI + spatial + TTL maintenance all
// stay atomic.
type UpdateStmt struct {
	Table       string
	Assignments []Assignment
	Where       Expr
	If          Expr
}

// AssignKind picks the SET action grammar branch.
type AssignKind uint8

const (
	AssignSet    AssignKind = iota + 1 // col = expr
	AssignRemove                       // REMOVE col [, ...]
	AssignAdd                          // ADD col value
	AssignDelete                       // DELETE col value (set remove)
)

// Assignment is one entry in UPDATE SET. Kind selects which fields
// matter:
//   - Set    → Column, Value
//   - Remove → Column (one Assignment per column listed in REMOVE)
//   - Add    → Column, Value (numeric increment or set add)
//   - Delete → Column, Value (set element to remove)
type Assignment struct {
	Kind   AssignKind
	Column string
	Value  Expr
}

// DeleteStmt is DELETE FROM <table> WHERE <pred> [IF <cond>].
type DeleteStmt struct {
	Table string
	Where Expr
	If    Expr
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
// spatial helpers ST_Within / ST_DWithin / BBox / Point, and the
// scalar WHERE functions begins_with / contains / attribute_exists /
// attribute_not_exists / attribute_type / size. Also lives in UPDATE
// SET expressions as list_append / list_prepend.
type FuncCall struct {
	Name string
	Args []Expr
}

// ArithKind selects the binary arithmetic operator on ArithExpr.
type ArithKind uint8

const (
	ArithAdd ArithKind = iota + 1
	ArithSub
)

// ArithExpr models `col + value` and `col - value` inside UPDATE SET.
// Kept narrow to numeric attributes — the executor refuses to apply
// arithmetic to non-numeric prior values.
type ArithExpr struct {
	Op    ArithKind
	Left  Expr
	Right Expr
}

func (*ArithExpr) expr() {}

func (*ColumnRef) expr()   {}
func (*Literal) expr()     {}
func (*BinaryExpr) expr()  {}
func (*NotExpr) expr()     {}
func (*BetweenExpr) expr() {}
func (*FuncCall) expr()    {}
