package sql

import (
	"fmt"
	"strconv"
	"strings"
)

// Parse turns SQL source into a single Stmt. Multi-statement scripts
// are out of scope — pass them one at a time.
func Parse(src string) (Stmt, error) {
	toks, err := Tokenize(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	// Trailing semicolon is optional.
	if t := p.peek(); t.Kind == tSemicolon {
		p.consume()
	}
	if t := p.peek(); t.Kind != tEOF {
		return nil, fmt.Errorf("unexpected token %q at position %d", t.Lit, t.Pos)
	}
	return stmt, nil
}

type parser struct {
	toks []Token
	pos  int
}

func (p *parser) peek() Token {
	if p.pos >= len(p.toks) {
		return Token{Kind: tEOF}
	}
	return p.toks[p.pos]
}

// peekAt returns the token offset positions ahead of the current
// cursor without advancing. Used by parseStatement to disambiguate
// CREATE TABLE from CREATE MATERIALIZED VIEW.
func (p *parser) peekAt(offset int) Token {
	if p.pos+offset >= len(p.toks) {
		return Token{Kind: tEOF}
	}
	return p.toks[p.pos+offset]
}

func (p *parser) consume() Token {
	t := p.peek()
	p.pos++
	return t
}

func (p *parser) expect(k TokenKind, want string) (Token, error) {
	t := p.peek()
	if t.Kind != k {
		return Token{}, fmt.Errorf("expected %s, got %q at %d", want, t.Lit, t.Pos)
	}
	p.pos++
	return t, nil
}

func (p *parser) accept(kinds ...TokenKind) bool {
	for _, k := range kinds {
		if p.peek().Kind == k {
			return true
		}
	}
	return false
}

func (p *parser) parseStatement() (Stmt, error) {
	switch p.peek().Kind {
	case tSelect:
		return p.parseSelect()
	case tInsert:
		return p.parseInsert()
	case tUpdate:
		return p.parseUpdate()
	case tDelete:
		return p.parseDelete()
	case tCreate:
		if p.peekAt(1).Kind == tMaterialized {
			return p.parseCreateMaterializedView()
		}
		if p.peekAt(1).Kind == tService {
			return p.parseCreateServiceLevel()
		}
		return p.parseCreate()
	case tDrop:
		if p.peekAt(1).Kind == tMaterialized {
			return p.parseDropMaterializedView()
		}
		if p.peekAt(1).Kind == tService {
			return p.parseDropServiceLevel()
		}
		return p.parseDrop()
	case tAlter:
		if p.peekAt(1).Kind == tService {
			return p.parseAlterServiceLevel()
		}
	case tList:
		if p.peekAt(1).Kind == tService {
			return p.parseListServiceLevels()
		}
	}
	return nil, fmt.Errorf("unsupported statement starting with %q", p.peek().Lit)
}

// ---------- SELECT ----------

func (p *parser) parseSelect() (*SelectStmt, error) {
	if _, err := p.expect(tSelect, "SELECT"); err != nil {
		return nil, err
	}
	stmt := &SelectStmt{}

	switch p.peek().Kind {
	case tStar:
		p.consume()
	case tCount:
		p.consume()
		if _, err := p.expect(tLParen, "("); err != nil {
			return nil, err
		}
		switch p.peek().Kind {
		case tStar, tIdent:
			p.consume()
		default:
			return nil, fmt.Errorf("expected * or column inside COUNT()")
		}
		if _, err := p.expect(tRParen, ")"); err != nil {
			return nil, err
		}
		stmt.Count = true
	default:
		for {
			id, err := p.expect(tIdent, "column name")
			if err != nil {
				return nil, err
			}
			stmt.Columns = append(stmt.Columns, id.Lit)
			if p.peek().Kind == tComma {
				p.consume()
				continue
			}
			break
		}
	}
	if _, err := p.expect(tFrom, "FROM"); err != nil {
		return nil, err
	}
	tn, err := p.expect(tIdent, "table name")
	if err != nil {
		return nil, err
	}
	stmt.Table = tn.Lit

	// Optional USE INDEX (<idx>).
	if p.peek().Kind == tUse {
		p.consume()
		if _, err := p.expect(tIndex, "INDEX"); err != nil {
			return nil, err
		}
		if _, err := p.expect(tLParen, "("); err != nil {
			return nil, err
		}
		idx, err := p.expect(tIdent, "index name")
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tRParen, ")"); err != nil {
			return nil, err
		}
		stmt.IndexName = idx.Lit
	}

	if p.peek().Kind == tAllow {
		p.consume()
		if _, err := p.expect(tScan, "SCAN"); err != nil {
			return nil, err
		}
		stmt.AllowScan = true
	}

	if p.peek().Kind == tWhere {
		p.consume()
		w, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = w
	}

	if p.peek().Kind == tOrder {
		p.consume()
		if _, err := p.expect(tBy, "BY"); err != nil {
			return nil, err
		}
		id, err := p.expect(tIdent, "column name")
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = id.Lit
		if p.peek().Kind == tAnn {
			p.consume()
			if _, err := p.expect(tOf, "OF"); err != nil {
				return nil, err
			}
			target, err := p.parseVectorLiteral()
			if err != nil {
				return nil, err
			}
			stmt.OrderANN = true
			stmt.ANNTarget = target.Values
		} else {
			switch p.peek().Kind {
			case tAsc:
				p.consume()
			case tDesc:
				p.consume()
				stmt.OrderDesc = true
			}
		}
	}

	if p.peek().Kind == tLimit {
		p.consume()
		n, err := p.expect(tNumber, "limit value")
		if err != nil {
			return nil, err
		}
		var lim int
		_, err = fmt.Sscanf(n.Lit, "%d", &lim)
		if err != nil {
			return nil, fmt.Errorf("bad LIMIT %q: %w", n.Lit, err)
		}
		stmt.Limit = lim
	}
	if p.peek().Kind == tDiversify {
		div, err := p.parseDiversifyTail()
		if err != nil {
			return nil, err
		}
		stmt.Diversify = div
	}
	return stmt, nil
}

func (p *parser) parseDiversifyTail() (*DiversifyClause, error) {
	p.consume() // DIVERSIFY
	if _, err := p.expect(tBy, "BY"); err != nil {
		return nil, err
	}
	method, err := p.expect(tIdent, "diversify method")
	if err != nil {
		return nil, err
	}
	div := &DiversifyClause{Method: method.Lit, Lambda: 0.5}
	if p.peek().Kind == tLParen {
		p.consume()
		if p.peek().Kind != tRParen {
			name, err := p.expect(tIdent, "diversify option")
			if err != nil {
				return nil, err
			}
			if !strings.EqualFold(name.Lit, "lambda") {
				return nil, fmt.Errorf("unsupported DIVERSIFY option %q", name.Lit)
			}
			if _, err := p.expect(tEq, "="); err != nil {
				return nil, err
			}
			val, err := p.expect(tNumber, "lambda value")
			if err != nil {
				return nil, err
			}
			f, err := strconv.ParseFloat(val.Lit, 64)
			if err != nil {
				return nil, fmt.Errorf("bad lambda %q: %w", val.Lit, err)
			}
			div.Lambda = f
		}
		if _, err := p.expect(tRParen, ")"); err != nil {
			return nil, err
		}
	}
	if _, err := p.expect(tTo, "TO"); err != nil {
		return nil, err
	}
	n, err := p.expect(tNumber, "target slate size")
	if err != nil {
		return nil, err
	}
	target, err := strconv.Atoi(n.Lit)
	if err != nil || target <= 0 {
		return nil, fmt.Errorf("bad DIVERSIFY target size %q", n.Lit)
	}
	div.TargetSize = target
	return div, nil
}

// ---------- INSERT ----------

func (p *parser) parseInsert() (*InsertStmt, error) {
	p.consume() // INSERT
	if _, err := p.expect(tInto, "INTO"); err != nil {
		return nil, err
	}
	tn, err := p.expect(tIdent, "table name")
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tLParen, "("); err != nil {
		return nil, err
	}
	stmt := &InsertStmt{Table: tn.Lit}
	for {
		id, err := p.expect(tIdent, "column name")
		if err != nil {
			return nil, err
		}
		stmt.Columns = append(stmt.Columns, id.Lit)
		if p.peek().Kind == tComma {
			p.consume()
			continue
		}
		break
	}
	if _, err := p.expect(tRParen, ")"); err != nil {
		return nil, err
	}
	if _, err := p.expect(tValues, "VALUES"); err != nil {
		return nil, err
	}
	if _, err := p.expect(tLParen, "("); err != nil {
		return nil, err
	}
	for {
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		stmt.Values = append(stmt.Values, v)
		if p.peek().Kind == tComma {
			p.consume()
			continue
		}
		break
	}
	if _, err := p.expect(tRParen, ")"); err != nil {
		return nil, err
	}
	if len(stmt.Values) != len(stmt.Columns) {
		return nil, fmt.Errorf("INSERT column/value count mismatch: %d cols vs %d values", len(stmt.Columns), len(stmt.Values))
	}
	if cond, err := p.parseIfTail(); err != nil {
		return nil, err
	} else if cond != nil {
		stmt.If = cond
	}
	if mode, err := p.parseReturningTail(); err != nil {
		return nil, err
	} else {
		stmt.Returning = mode
	}
	return stmt, nil
}

// parseReturningTail accepts an optional `RETURNING (*|NEW|OLD)`
// suffix on DML statements. Returns ReturningNone when absent.
func (p *parser) parseReturningTail() (ReturningMode, error) {
	if p.peek().Kind != tReturning {
		return ReturningNone, nil
	}
	p.consume()
	switch p.peek().Kind {
	case tStar:
		p.consume()
		return ReturningAll, nil
	case tNew:
		p.consume()
		return ReturningNew, nil
	case tOld:
		p.consume()
		return ReturningOld, nil
	}
	return ReturningNone, fmt.Errorf("expected *, NEW or OLD after RETURNING")
}

// parseIfTail accepts an optional `IF <expr>` suffix on DML
// statements. Returns nil, nil when the keyword is absent so the
// caller can pass nil through to the storage condition.
func (p *parser) parseIfTail() (Expr, error) {
	if p.peek().Kind != tIf {
		return nil, nil
	}
	p.consume()
	// Special case: "IF NOT EXISTS" shortcut for
	// attribute_not_exists(<pk>). The planner refines which column
	// it means when it has the table descriptor in hand.
	if p.peek().Kind == tNot {
		p.consume()
		if _, err := p.expect(tExists, "EXISTS"); err != nil {
			return nil, err
		}
		return &FuncCall{Name: "ATTRIBUTE_NOT_EXISTS", Args: []Expr{&ColumnRef{Name: "*"}}}, nil
	}
	if p.peek().Kind == tExists {
		p.consume()
		return &FuncCall{Name: "ATTRIBUTE_EXISTS", Args: []Expr{&ColumnRef{Name: "*"}}}, nil
	}
	return p.parseExpr()
}

// ---------- UPDATE ----------

func (p *parser) parseUpdate() (*UpdateStmt, error) {
	p.consume() // UPDATE
	tn, err := p.expect(tIdent, "table name")
	if err != nil {
		return nil, err
	}
	stmt := &UpdateStmt{Table: tn.Lit}
	for {
		switch p.peek().Kind {
		case tRemove:
			p.consume()
			for {
				col, err := p.expect(tIdent, "column name after REMOVE")
				if err != nil {
					return nil, err
				}
				stmt.Assignments = append(stmt.Assignments, Assignment{Kind: AssignRemove, Column: col.Lit})
				if p.peek().Kind == tComma {
					p.consume()
					continue
				}
				break
			}
		case tAdd:
			p.consume()
			col, err := p.expect(tIdent, "column name after ADD")
			if err != nil {
				return nil, err
			}
			v, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			stmt.Assignments = append(stmt.Assignments, Assignment{Kind: AssignAdd, Column: col.Lit, Value: v})
		case tDelete:
			p.consume()
			col, err := p.expect(tIdent, "column name after DELETE")
			if err != nil {
				return nil, err
			}
			v, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			stmt.Assignments = append(stmt.Assignments, Assignment{Kind: AssignDelete, Column: col.Lit, Value: v})
		default:
			// SET <col> = <expr>. SET keyword is optional — DynamoDB
			// drops it after the first action and we follow suit.
			if p.peek().Kind == tSet {
				p.consume()
			}
			col, err := p.expect(tIdent, "column name")
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(tEq, "="); err != nil {
				return nil, err
			}
			v, err := p.parseAssignValue(col.Lit)
			if err != nil {
				return nil, err
			}
			stmt.Assignments = append(stmt.Assignments, Assignment{Kind: AssignSet, Column: col.Lit, Value: v})
		}
		if p.peek().Kind == tComma {
			p.consume()
			continue
		}
		break
	}
	if p.peek().Kind != tWhere {
		return nil, fmt.Errorf("UPDATE requires WHERE")
	}
	p.consume()
	w, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	stmt.Where = w
	if cond, err := p.parseIfTail(); err != nil {
		return nil, err
	} else if cond != nil {
		stmt.If = cond
	}
	if mode, err := p.parseReturningTail(); err != nil {
		return nil, err
	} else {
		stmt.Returning = mode
	}
	return stmt, nil
}

// parseAssignValue reads the right-hand side of a SET assignment.
// Beyond plain literals + function calls (inherited from
// parseValue), it accepts `col + N` / `col - N` arithmetic so the
// caller can write `SET score = score + 1` straight off DynamoDB
// PartiQL. The first token has already been consumed by the parent
// statement parser; we only emit the additional grammar here.
func (p *parser) parseAssignValue(targetCol string) (Expr, error) {
	v, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	// `col + value` arithmetic only kicks in when the left operand is
	// a bare column reference. Anything else stays a plain Set value.
	if _, isCol := v.(*ColumnRef); !isCol {
		return v, nil
	}
	switch p.peek().Kind {
	case tPlus:
		p.consume()
		rhs, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		return &ArithExpr{Op: ArithAdd, Left: v, Right: rhs}, nil
	case tMinus:
		p.consume()
		rhs, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		return &ArithExpr{Op: ArithSub, Left: v, Right: rhs}, nil
	}
	_ = targetCol
	return v, nil
}

// ---------- DELETE ----------

func (p *parser) parseDelete() (*DeleteStmt, error) {
	p.consume() // DELETE
	if _, err := p.expect(tFrom, "FROM"); err != nil {
		return nil, err
	}
	tn, err := p.expect(tIdent, "table name")
	if err != nil {
		return nil, err
	}
	if p.peek().Kind != tWhere {
		return nil, fmt.Errorf("DELETE requires WHERE")
	}
	p.consume()
	w, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	stmt := &DeleteStmt{Table: tn.Lit, Where: w}
	if cond, err := p.parseIfTail(); err != nil {
		return nil, err
	} else if cond != nil {
		stmt.If = cond
	}
	if mode, err := p.parseReturningTail(); err != nil {
		return nil, err
	} else {
		stmt.Returning = mode
	}
	return stmt, nil
}

// ---------- CREATE TABLE ----------

func (p *parser) parseCreate() (*CreateTableStmt, error) {
	p.consume() // CREATE
	if _, err := p.expect(tTable, "TABLE"); err != nil {
		return nil, err
	}
	tn, err := p.expect(tIdent, "table name")
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tLParen, "("); err != nil {
		return nil, err
	}
	if _, err := p.expect(tPrimary, "PRIMARY"); err != nil {
		return nil, err
	}
	if _, err := p.expect(tKey, "KEY"); err != nil {
		return nil, err
	}
	if _, err := p.expect(tLParen, "("); err != nil {
		return nil, err
	}
	pk, err := p.expect(tIdent, "PK column")
	if err != nil {
		return nil, err
	}
	stmt := &CreateTableStmt{Table: tn.Lit, PK: pk.Lit}
	if p.peek().Kind == tComma {
		p.consume()
		sk, err := p.expect(tIdent, "SK column")
		if err != nil {
			return nil, err
		}
		stmt.SK = sk.Lit
	}
	if _, err := p.expect(tRParen, ")"); err != nil {
		return nil, err
	}
	for p.peek().Kind == tComma {
		p.consume()
		col, err := p.expect(tIdent, "column name")
		if err != nil {
			return nil, err
		}
		def, err := p.parseColumnDefinition(col.Lit)
		if err != nil {
			return nil, err
		}
		stmt.AttributeDefinitions = append(stmt.AttributeDefinitions, def)
	}
	if _, err := p.expect(tRParen, ")"); err != nil {
		return nil, err
	}
	if p.peek().Kind == tWith {
		p.consume()
		if _, err := p.expect(tStorage, "STORAGE"); err != nil {
			return nil, err
		}
		if _, err := p.expect(tEq, "="); err != nil {
			return nil, err
		}
		switch p.peek().Kind {
		case tString, tIdent:
			stmt.StorageClass = p.consume().Lit
		default:
			return nil, fmt.Errorf("expected storage class after WITH STORAGE =")
		}
	}
	return stmt, nil
}

func (p *parser) parseColumnDefinition(name string) (CreateAttributeDefinition, error) {
	typ, err := p.expect(tIdent, "attribute type")
	if err != nil {
		return CreateAttributeDefinition{}, err
	}
	def := CreateAttributeDefinition{Name: name, Type: strings.ToUpper(typ.Lit)}
	if strings.EqualFold(def.Type, "V") {
		if _, err := p.expect(tLt, "<"); err != nil {
			return CreateAttributeDefinition{}, err
		}
		n, err := p.expect(tNumber, "vector dimension")
		if err != nil {
			return CreateAttributeDefinition{}, err
		}
		dim, err := strconv.Atoi(n.Lit)
		if err != nil || dim <= 0 {
			return CreateAttributeDefinition{}, fmt.Errorf("bad vector dimension %q", n.Lit)
		}
		def.VectorDimensions = dim
		if _, err := p.expect(tGt, ">"); err != nil {
			return CreateAttributeDefinition{}, err
		}
	}
	return def, nil
}

// ---------- DROP TABLE ----------

func (p *parser) parseDrop() (*DropTableStmt, error) {
	p.consume() // DROP
	if _, err := p.expect(tTable, "TABLE"); err != nil {
		return nil, err
	}
	tn, err := p.expect(tIdent, "table name")
	if err != nil {
		return nil, err
	}
	return &DropTableStmt{Table: tn.Lit}, nil
}

// ---------- expressions ----------

func (p *parser) parseExpr() (Expr, error) { return p.parseOr() }

func (p *parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.peek().Kind == tOr {
		p.consume()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: BinOr, Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.peek().Kind == tAnd {
		p.consume()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: BinAnd, Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parseNot() (Expr, error) {
	if p.peek().Kind == tNot {
		p.consume()
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &NotExpr{Inner: inner}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (Expr, error) {
	if p.peek().Kind == tLParen {
		p.consume()
		inner, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tRParen, ")"); err != nil {
			return nil, err
		}
		return inner, nil
	}
	// Try comparison / BETWEEN / function-only predicate.
	left, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	switch p.peek().Kind {
	case tBetween:
		p.consume()
		lo, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tAnd, "AND"); err != nil {
			return nil, err
		}
		hi, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &BetweenExpr{Value: left, Lo: lo, Hi: hi}, nil
	case tEq, tNeq, tLt, tLte, tGt, tGte:
		op := binOpFromKind(p.consume().Kind)
		right, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Op: op, Left: left, Right: right}, nil
	case tIs:
		p.consume()
		neg := false
		if p.peek().Kind == tNot {
			p.consume()
			neg = true
		}
		if _, err := p.expect(tNull, "NULL"); err != nil {
			return nil, err
		}
		var bin Expr = &BinaryExpr{
			Op:    BinEq,
			Left:  left,
			Right: &Literal{Kind: LitNull},
		}
		if neg {
			bin = &NotExpr{Inner: bin}
		}
		return bin, nil
	}
	// A bare function call (e.g. ST_Within(...)) used as a predicate.
	// FuncCall already satisfies Expr; the planner / evaluator treats
	// a FuncCall in boolean position as "true when the function holds
	// for the row".
	if fn, ok := left.(*FuncCall); ok {
		return fn, nil
	}
	return nil, fmt.Errorf("unexpected token after operand: %q at %d", p.peek().Lit, p.peek().Pos)
}

func binOpFromKind(k TokenKind) BinOp {
	switch k {
	case tEq:
		return BinEq
	case tNeq:
		return BinNeq
	case tLt:
		return BinLt
	case tLte:
		return BinLte
	case tGt:
		return BinGt
	case tGte:
		return BinGte
	}
	return 0
}

// parseOperand reads a column ref, a literal, or a function call.
func (p *parser) parseOperand() (Expr, error) {
	t := p.peek()
	switch t.Kind {
	case tIdent:
		p.consume()
		if p.peek().Kind == tLParen {
			return p.parseFuncCallTail(t.Lit)
		}
		return &ColumnRef{Name: t.Lit}, nil
	}
	return p.parseValue()
}

func (p *parser) parseFuncCallTail(name string) (Expr, error) {
	if _, err := p.expect(tLParen, "("); err != nil {
		return nil, err
	}
	fn := &FuncCall{Name: strings.ToUpper(name)}
	if p.peek().Kind == tRParen {
		p.consume()
		return fn, nil
	}
	for {
		arg, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		fn.Args = append(fn.Args, arg)
		if p.peek().Kind == tComma {
			p.consume()
			continue
		}
		break
	}
	if _, err := p.expect(tRParen, ")"); err != nil {
		return nil, err
	}
	return fn, nil
}

// parseValue reads a literal (string/number/bool/null) or a function
// call. Used in INSERT VALUES, UPDATE SET, and the right-hand side of
// comparisons.
func (p *parser) parseValue() (Expr, error) {
	t := p.peek()
	switch t.Kind {
	case tLBracket:
		return p.parseVectorLiteral()
	case tString:
		p.consume()
		return &Literal{Kind: LitString, Value: t.Lit}, nil
	case tNumber:
		p.consume()
		return &Literal{Kind: LitNumber, Value: t.Lit}, nil
	case tTrue:
		p.consume()
		return &Literal{Kind: LitBool, Bool: true}, nil
	case tFalse:
		p.consume()
		return &Literal{Kind: LitBool, Bool: false}, nil
	case tNull:
		p.consume()
		return &Literal{Kind: LitNull}, nil
	case tIdent:
		// Could be a function call or a bare ident used as a value
		// (rare, but legal for "col = other_col" — not supported in v1
		// but the parser shape stays open).
		p.consume()
		if p.peek().Kind == tLParen {
			return p.parseFuncCallTail(t.Lit)
		}
		return &ColumnRef{Name: t.Lit}, nil
	}
	return nil, fmt.Errorf("expected value at %d, got %q", t.Pos, t.Lit)
}

func (p *parser) parseVectorLiteral() (*VectorLiteral, error) {
	if _, err := p.expect(tLBracket, "["); err != nil {
		return nil, err
	}
	var out []float64
	if p.peek().Kind == tRBracket {
		p.consume()
		return &VectorLiteral{Values: out}, nil
	}
	for {
		tok, err := p.expect(tNumber, "vector number")
		if err != nil {
			return nil, err
		}
		f, err := strconv.ParseFloat(tok.Lit, 64)
		if err != nil {
			return nil, fmt.Errorf("bad vector number %q: %w", tok.Lit, err)
		}
		out = append(out, f)
		if p.peek().Kind == tComma {
			p.consume()
			continue
		}
		break
	}
	if _, err := p.expect(tRBracket, "]"); err != nil {
		return nil, err
	}
	return &VectorLiteral{Values: out}, nil
}

func (p *parser) parseCreateMaterializedView() (*CreateMaterializedViewStmt, error) {
	p.consume() // CREATE
	if _, err := p.expect(tMaterialized, "MATERIALIZED"); err != nil {
		return nil, err
	}
	if _, err := p.expect(tView, "VIEW"); err != nil {
		return nil, err
	}
	name, err := p.expect(tIdent, "view name")
	if err != nil {
		return nil, err
	}
	stmt := &CreateMaterializedViewStmt{Name: name.Lit}

	if _, err := p.expect(tAs, "AS"); err != nil {
		return nil, err
	}
	if _, err := p.expect(tSelect, "SELECT"); err != nil {
		return nil, err
	}
	if p.peek().Kind == tStar {
		p.consume()
	} else {
		for {
			col, err := p.expect(tIdent, "projected attribute")
			if err != nil {
				return nil, err
			}
			stmt.Projected = append(stmt.Projected, col.Lit)
			if p.peek().Kind != tComma {
				break
			}
			p.consume()
		}
	}
	if _, err := p.expect(tFrom, "FROM"); err != nil {
		return nil, err
	}
	base, err := p.expect(tIdent, "base table name")
	if err != nil {
		return nil, err
	}
	stmt.BaseTable = base.Lit

	if _, err := p.expect(tPrimary, "PRIMARY"); err != nil {
		return nil, err
	}
	if _, err := p.expect(tKey, "KEY"); err != nil {
		return nil, err
	}
	if _, err := p.expect(tLParen, "("); err != nil {
		return nil, err
	}
	pk, err := p.expect(tIdent, "PK column")
	if err != nil {
		return nil, err
	}
	stmt.PK = pk.Lit
	if p.peek().Kind == tComma {
		p.consume()
		sk, err := p.expect(tIdent, "SK column")
		if err != nil {
			return nil, err
		}
		stmt.SK = sk.Lit
	}
	if _, err := p.expect(tRParen, ")"); err != nil {
		return nil, err
	}

	if p.peek().Kind == tRefresh {
		p.consume()
		spec, err := p.parseRefreshClause()
		if err != nil {
			return nil, err
		}
		stmt.Refresh = spec
	} else {
		stmt.Refresh = MVRefreshSpec{Mode: "eager"}
	}
	return stmt, nil
}

func (p *parser) parseRefreshClause() (MVRefreshSpec, error) {
	switch p.peek().Kind {
	case tEager:
		p.consume()
		return MVRefreshSpec{Mode: "eager"}, nil
	case tEvery:
		p.consume()
		n, err := p.expect(tNumber, "refresh interval")
		if err != nil {
			return MVRefreshSpec{}, err
		}
		val, err := strconv.ParseInt(n.Lit, 10, 64)
		if err != nil || val <= 0 {
			return MVRefreshSpec{}, fmt.Errorf("REFRESH EVERY: bad interval %q", n.Lit)
		}
		secs, err := p.parseRefreshUnit(val)
		if err != nil {
			return MVRefreshSpec{}, err
		}
		return MVRefreshSpec{Mode: "scheduled", IntervalSeconds: secs}, nil
	case tOnDemand:
		p.consume()
		if _, err := p.expect(tDemand, "DEMAND"); err != nil {
			return MVRefreshSpec{}, err
		}
		return MVRefreshSpec{Mode: "on_demand"}, nil
	default:
		return MVRefreshSpec{}, fmt.Errorf("expected EAGER, EVERY or ON DEMAND after REFRESH, got %q", p.peek().Lit)
	}
}

func (p *parser) parseRefreshUnit(val int64) (int64, error) {
	switch p.peek().Kind {
	case tSecond, tSeconds:
		p.consume()
		return val, nil
	case tMinute, tMinutes:
		p.consume()
		return val * 60, nil
	case tHour, tHours:
		p.consume()
		return val * 3600, nil
	case tDay, tDays:
		p.consume()
		return val * 86400, nil
	default:
		return 0, fmt.Errorf("expected SECONDS / MINUTES / HOURS / DAYS, got %q", p.peek().Lit)
	}
}

func (p *parser) parseDropMaterializedView() (*DropMaterializedViewStmt, error) {
	p.consume() // DROP
	if _, err := p.expect(tMaterialized, "MATERIALIZED"); err != nil {
		return nil, err
	}
	if _, err := p.expect(tView, "VIEW"); err != nil {
		return nil, err
	}
	name, err := p.expect(tIdent, "view name")
	if err != nil {
		return nil, err
	}
	return &DropMaterializedViewStmt{Name: name.Lit}, nil
}

// parseCreateServiceLevel parses
//
//	CREATE SERVICE LEVEL <name>
//	  [WITH SHARES=N [, MAX_IN_FLIGHT=N] [, MAX_ROWS_PER_SEC=N] [, MAX_BYTES_PER_SEC=N]]
func (p *parser) parseCreateServiceLevel() (*CreateServiceLevelStmt, error) {
	p.consume() // CREATE
	if _, err := p.expect(tService, "SERVICE"); err != nil {
		return nil, err
	}
	if _, err := p.expect(tLevel, "LEVEL"); err != nil {
		return nil, err
	}
	name, err := p.expect(tIdent, "service level name")
	if err != nil {
		return nil, err
	}
	spec, err := p.parseOptionalServiceLevelWith()
	if err != nil {
		return nil, err
	}
	return &CreateServiceLevelStmt{Name: name.Lit, Spec: spec}, nil
}

// parseAlterServiceLevel parses ALTER SERVICE LEVEL <name> WITH ...
func (p *parser) parseAlterServiceLevel() (*AlterServiceLevelStmt, error) {
	p.consume() // ALTER
	if _, err := p.expect(tService, "SERVICE"); err != nil {
		return nil, err
	}
	if _, err := p.expect(tLevel, "LEVEL"); err != nil {
		return nil, err
	}
	name, err := p.expect(tIdent, "service level name")
	if err != nil {
		return nil, err
	}
	spec, err := p.parseOptionalServiceLevelWith()
	if err != nil {
		return nil, err
	}
	return &AlterServiceLevelStmt{Name: name.Lit, Spec: spec}, nil
}

// parseDropServiceLevel parses DROP SERVICE LEVEL <name>.
func (p *parser) parseDropServiceLevel() (*DropServiceLevelStmt, error) {
	p.consume() // DROP
	if _, err := p.expect(tService, "SERVICE"); err != nil {
		return nil, err
	}
	if _, err := p.expect(tLevel, "LEVEL"); err != nil {
		return nil, err
	}
	name, err := p.expect(tIdent, "service level name")
	if err != nil {
		return nil, err
	}
	return &DropServiceLevelStmt{Name: name.Lit}, nil
}

// parseListServiceLevels parses LIST SERVICE LEVELS.
func (p *parser) parseListServiceLevels() (*ListServiceLevelsStmt, error) {
	p.consume() // LIST
	if _, err := p.expect(tService, "SERVICE"); err != nil {
		return nil, err
	}
	// Accept LEVEL or LEVELS — keyword lookup table only has LEVEL,
	// so the lexer reports both as tLevel since the case-insensitive
	// match handles plurals at the application layer.
	if _, err := p.expect(tLevel, "LEVEL"); err != nil {
		return nil, err
	}
	return &ListServiceLevelsStmt{}, nil
}

// parseOptionalServiceLevelWith parses the optional
// WITH key=value [, key=value]+ clause. Recognised keys are
// SHARES, MAX_IN_FLIGHT, MAX_ROWS_PER_SEC, MAX_BYTES_PER_SEC.
func (p *parser) parseOptionalServiceLevelWith() (ServiceLevelSpec, error) {
	var spec ServiceLevelSpec
	if p.peek().Kind != tWith {
		return spec, nil
	}
	p.consume() // WITH
	for {
		key, err := p.expect(tIdent, "service level option")
		if err != nil {
			// SHARES is a reserved keyword (tShares); accept it.
			if p.peek().Kind == tShares {
				key = p.consume()
			} else {
				return spec, err
			}
		}
		if _, err := p.expect(tEq, "="); err != nil {
			return spec, err
		}
		val, err := p.expect(tNumber, "integer value")
		if err != nil {
			return spec, err
		}
		v, err := strconv.ParseInt(val.Lit, 10, 64)
		if err != nil {
			return spec, fmt.Errorf("service level option %q: %w", key.Lit, err)
		}
		switch strings.ToUpper(key.Lit) {
		case "SHARES":
			spec.Shares = int(v)
		case "MAX_IN_FLIGHT":
			spec.MaxInFlight = int(v)
		case "MAX_ROWS_PER_SEC":
			spec.MaxRowsPerSec = v
		case "MAX_BYTES_PER_SEC":
			spec.MaxBytesPerSec = v
		default:
			return spec, fmt.Errorf("unknown service level option %q", key.Lit)
		}
		if p.peek().Kind != tComma {
			break
		}
		p.consume()
	}
	return spec, nil
}
