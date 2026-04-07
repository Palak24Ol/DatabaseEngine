package sql

import (
	"fmt"
	"strconv"
	"strings"
)

type Parser struct {
	tokens []Token
	pos    int
}

func NewParser(input string) *Parser {
	return &Parser{tokens: NewLexer(input).Tokenize()}
}

func (p *Parser) Parse() (Statement, error) {
	switch p.peek().Type {
	case TOKEN_SELECT: return p.parseSelect()
	case TOKEN_INSERT: return p.parseInsert()
	case TOKEN_DELETE: return p.parseDelete()
	case TOKEN_CREATE: return p.parseCreate()
	case TOKEN_UPDATE: return p.parseUpdate()
	case TOKEN_USE:    return p.parseUse()
	case TOKEN_SHOW:   return p.parseShow()
	case TOKEN_EXPLAIN: return p.parseExplain()
	case TOKEN_DROP:   return p.parseDrop()
	default:
		return nil, fmt.Errorf("unknown statement: '%s'", p.peek().Literal)
	}
}

// ── SELECT ────────────────────────────────────────────────────

func (p *Parser) parseSelect() (*SelectStatement, error) {
	p.consume()
	exprs, err := p.parseSelectExprs()
	if err != nil { return nil, err }
	if err := p.expect(TOKEN_FROM); err != nil { return nil, err }
	tableTok, err := p.expectIdent()
	if err != nil { return nil, err }

	stmt := &SelectStatement{Exprs: exprs, TableName: tableTok.Literal}

	if p.peek().Type == TOKEN_JOIN {
		p.consume()
		joinTbl, err := p.expectIdent()
		if err != nil { return nil, err }
		if err := p.expect(TOKEN_ON); err != nil { return nil, err }
		lt, lc, err := p.parseQualifiedCol()
		if err != nil { return nil, err }
		if err := p.expect(TOKEN_EQUALS); err != nil { return nil, err }
		rt, rc, err := p.parseQualifiedCol()
		if err != nil { return nil, err }
		stmt.Join = &JoinClause{TableName: joinTbl.Literal, LeftTable: lt, LeftCol: lc, RightTable: rt, RightCol: rc}
	}

	if p.peek().Type == TOKEN_WHERE {
		p.consume()
		where, err := p.parseWhere()
		if err != nil { return nil, err }
		stmt.Where = where
	}

	if p.peek().Type == TOKEN_ORDER {
		p.consume()
		if err := p.expect(TOKEN_BY); err != nil { return nil, err }
		colTok, err := p.expectIdent()
		if err != nil { return nil, err }
		desc := false
		if p.peek().Type == TOKEN_DESC { p.consume(); desc = true } else if p.peek().Type == TOKEN_ASC { p.consume() }
		stmt.OrderBy = &OrderByClause{Column: colTok.Literal, Desc: desc}
	}

	if p.peek().Type == TOKEN_LIMIT {
		p.consume()
		n, err := strconv.Atoi(p.consume().Literal)
		if err != nil { return nil, fmt.Errorf("LIMIT expects a number") }
		stmt.Limit = n
	}

	return stmt, nil
}

func (p *Parser) parseSelectExprs() ([]SelectExpr, error) {
	if p.peek().Type == TOKEN_STAR { p.consume(); return []SelectExpr{{Star: true}}, nil }
	var exprs []SelectExpr
	for {
		expr, err := p.parseOneExpr()
		if err != nil { return nil, err }
		exprs = append(exprs, expr)
		if p.peek().Type != TOKEN_COMMA { break }
		p.consume()
	}
	return exprs, nil
}

func (p *Parser) parseOneExpr() (SelectExpr, error) {
	tok := p.consume()
	upper := strings.ToUpper(tok.Literal)
	if isAggFunc(upper) && p.peek().Type == TOKEN_LPAREN {
		p.consume()
		expr := SelectExpr{AggFunc: upper}
		if p.peek().Type == TOKEN_STAR { p.consume(); expr.AggStar = true } else { expr.AggArg = p.consume().Literal }
		if err := p.expect(TOKEN_RPAREN); err != nil { return SelectExpr{}, err }
		return expr, nil
	}
	if p.peek().Type == TOKEN_DOT {
		p.consume()
		colTok, err := p.expectIdent()
		if err != nil { return SelectExpr{}, err }
		return SelectExpr{Table: tok.Literal, Column: colTok.Literal}, nil
	}
	return SelectExpr{Column: tok.Literal}, nil
}

func isAggFunc(s string) bool {
	switch s { case "COUNT","SUM","AVG","MAX","MIN": return true }
	return false
}

func (p *Parser) parseQualifiedCol() (string, string, error) {
	t, err := p.expectIdent()
	if err != nil { return "","",err }
	if err := p.expect(TOKEN_DOT); err != nil { return "","",err }
	c, err := p.expectIdent()
	if err != nil { return "","",err }
	return t.Literal, c.Literal, nil
}

// ── INSERT ────────────────────────────────────────────────────

func (p *Parser) parseInsert() (*InsertStatement, error) {
	p.consume()
	if err := p.expect(TOKEN_INTO); err != nil { return nil, err }
	tbl, err := p.expectIdent()
	if err != nil { return nil, err }
	if err := p.expect(TOKEN_VALUES); err != nil { return nil, err }
	if err := p.expect(TOKEN_LPAREN); err != nil { return nil, err }
	var vals []string
	for p.peek().Type != TOKEN_RPAREN && p.peek().Type != TOKEN_EOF {
		tok := p.consume()
		vals = append(vals, tok.Literal)
		if p.peek().Type == TOKEN_COMMA { p.consume() }
	}
	if err := p.expect(TOKEN_RPAREN); err != nil { return nil, err }
	return &InsertStatement{TableName: tbl.Literal, Values: vals}, nil
}

// ── UPDATE ────────────────────────────────────────────────────

func (p *Parser) parseUpdate() (*UpdateStatement, error) {
	p.consume()
	tbl, err := p.expectIdent()
	if err != nil { return nil, err }
	if err := p.expect(TOKEN_SET); err != nil { return nil, err }
	var sets []SetClause
	for {
		col, err := p.expectIdent()
		if err != nil { return nil, err }
		if err := p.expect(TOKEN_EQUALS); err != nil { return nil, err }
		val := p.consume()
		sets = append(sets, SetClause{Column: col.Literal, Value: val.Literal})
		if p.peek().Type != TOKEN_COMMA { break }
		p.consume()
	}
	stmt := &UpdateStatement{TableName: tbl.Literal, Sets: sets}
	if p.peek().Type == TOKEN_WHERE {
		p.consume()
		where, err := p.parseWhere()
		if err != nil { return nil, err }
		stmt.Where = where
	}
	return stmt, nil
}

// ── DELETE ────────────────────────────────────────────────────

func (p *Parser) parseDelete() (*DeleteStatement, error) {
	p.consume()
	if err := p.expect(TOKEN_FROM); err != nil { return nil, err }
	tbl, err := p.expectIdent()
	if err != nil { return nil, err }
	stmt := &DeleteStatement{TableName: tbl.Literal}
	if p.peek().Type == TOKEN_WHERE {
		p.consume()
		where, err := p.parseWhere()
		if err != nil { return nil, err }
		stmt.Where = where
	}
	return stmt, nil
}

// ── CREATE ────────────────────────────────────────────────────

func (p *Parser) parseCreate() (Statement, error) {
	p.consume()

	if p.peek().Type == TOKEN_DATABASE {
		p.consume()
		name, err := p.expectIdent()
		if err != nil { return nil, err }
		return &CreateDBStatement{DBName: name.Literal}, nil
	}

	if p.peek().Type == TOKEN_INDEX || (p.peek().Type == TOKEN_UNIQUE && p.peekAt(1).Type == TOKEN_INDEX) {
		return p.parseCreateIndex()
	}

	if err := p.expect(TOKEN_TABLE); err != nil { return nil, err }
	tbl, err := p.expectIdent()
	if err != nil { return nil, err }
	if err := p.expect(TOKEN_LPAREN); err != nil { return nil, err }

	var cols []ColumnDef
	for p.peek().Type != TOKEN_RPAREN && p.peek().Type != TOKEN_EOF {
		// handle FOREIGN KEY inline syntax: skip for now
		if p.peek().Type == TOKEN_FOREIGN {
			for p.peek().Type != TOKEN_COMMA && p.peek().Type != TOKEN_RPAREN && p.peek().Type != TOKEN_EOF {
				p.consume()
			}
			if p.peek().Type == TOKEN_COMMA { p.consume() }
			continue
		}

		name, err := p.expectIdent()
		if err != nil { return nil, err }
		typeT := p.consume()
		col := ColumnDef{Name: name.Literal, DataType: typeT.Literal}

		if p.peek().Type == TOKEN_PRIMARY { p.consume(); p.consume(); col.PrimaryKey = true }
		if p.peek().Type == TOKEN_UNIQUE  { p.consume(); col.Unique = true }

		// REFERENCES table(col)
		if p.peek().Type == TOKEN_REFERENCES {
			p.consume()
			refTbl, _ := p.expectIdent()
			p.expect(TOKEN_LPAREN)
			refCol, _ := p.expectIdent()
			p.expect(TOKEN_RPAREN)
			col.ForeignKey = &ForeignKeyDef{RefTable: refTbl.Literal, RefColumn: refCol.Literal}
		}

		cols = append(cols, col)
		if p.peek().Type == TOKEN_COMMA { p.consume() }
	}
	if err := p.expect(TOKEN_RPAREN); err != nil { return nil, err }
	return &CreateStatement{TableName: tbl.Literal, Columns: cols}, nil
}

func (p *Parser) parseCreateIndex() (*CreateIndexStatement, error) {
	unique := false
	if p.peek().Type == TOKEN_UNIQUE { p.consume(); unique = true }
	p.consume() // INDEX
	name, err := p.expectIdent()
	if err != nil { return nil, err }
	if err := p.expect(TOKEN_ON); err != nil { return nil, err }
	tbl, err := p.expectIdent()
	if err != nil { return nil, err }
	if err := p.expect(TOKEN_LPAREN); err != nil { return nil, err }
	col, err := p.expectIdent()
	if err != nil { return nil, err }
	if err := p.expect(TOKEN_RPAREN); err != nil { return nil, err }
	return &CreateIndexStatement{IndexName: name.Literal, TableName: tbl.Literal, Column: col.Literal, Unique: unique}, nil
}

// ── DROP ──────────────────────────────────────────────────────

func (p *Parser) parseDrop() (Statement, error) {
	p.consume()
	if p.peek().Type == TOKEN_INDEX {
		p.consume()
		name, err := p.expectIdent()
		if err != nil { return nil, err }
		return &DropIndexStatement{IndexName: name.Literal}, nil
	}
	return nil, fmt.Errorf("DROP: only INDEX is supported")
}

// ── EXPLAIN ───────────────────────────────────────────────────

func (p *Parser) parseExplain() (*ExplainStatement, error) {
	p.consume()
	inner, err := p.Parse()
	if err != nil { return nil, err }
	return &ExplainStatement{Inner: inner}, nil
}

// ── USE / SHOW ────────────────────────────────────────────────

func (p *Parser) parseUse() (*UseDBStatement, error) {
	p.consume()
	name, err := p.expectIdent()
	if err != nil { return nil, err }
	return &UseDBStatement{DBName: name.Literal}, nil
}

func (p *Parser) parseShow() (*ShowDBStatement, error) {
	p.consume(); p.consume()
	return &ShowDBStatement{}, nil
}

// ── WHERE ─────────────────────────────────────────────────────

func (p *Parser) parseWhere() (*WhereClause, error) {
	left, err := p.parseCondition()
	if err != nil { return nil, err }
	for p.peek().Type == TOKEN_AND || p.peek().Type == TOKEN_OR {
		logic := p.consume().Literal
		right, err := p.parseCondition()
		if err != nil { return nil, err }
		left = &WhereClause{IsCompound: true, Left: left, Right: right, Logic: logic}
	}
	return left, nil
}

func (p *Parser) parseCondition() (*WhereClause, error) {
	col, err := p.expectIdent()
	if err != nil { return nil, err }
	colName := col.Literal
	if p.peek().Type == TOKEN_DOT {
		p.consume()
		right, err := p.expectIdent()
		if err != nil { return nil, err }
		colName = col.Literal + "." + right.Literal
	}
	op := p.consume()
	val := p.consume()
	return &WhereClause{Column: colName, Operator: op.Literal, Value: val.Literal}, nil
}

func (p *Parser) parseColumns() ([]string, error) {
	if p.peek().Type == TOKEN_STAR { p.consume(); return []string{"*"}, nil }
	var cols []string
	for {
		tok, err := p.expectIdent()
		if err != nil { return nil, err }
		cols = append(cols, tok.Literal)
		if p.peek().Type != TOKEN_COMMA { break }
		p.consume()
	}
	return cols, nil
}

// ── Helpers ───────────────────────────────────────────────────

func (p *Parser) peek() Token {
	if p.pos >= len(p.tokens) { return Token{TOKEN_EOF, ""} }
	return p.tokens[p.pos]
}

func (p *Parser) peekAt(offset int) Token {
	i := p.pos + offset
	if i >= len(p.tokens) { return Token{TOKEN_EOF, ""} }
	return p.tokens[i]
}

func (p *Parser) consume() Token {
	tok := p.peek(); p.pos++; return tok
}

func (p *Parser) expect(t TokenType) error {
	tok := p.consume()
	if tok.Type != t { return fmt.Errorf("expected token %d but got '%s'", t, tok.Literal) }
	return nil
}

// expectIdent accepts keywords as column names too
func (p *Parser) expectIdent() (Token, error) {
	tok := p.consume()
	if tok.Type == TOKEN_IDENT { return tok, nil }
	// treat keywords as identifiers in column name position
	if tok.Type != TOKEN_EOF && tok.Type != TOKEN_ILLEGAL &&
		tok.Type != TOKEN_COMMA && tok.Type != TOKEN_LPAREN &&
		tok.Type != TOKEN_RPAREN && tok.Type != TOKEN_EQUALS &&
		tok.Type != TOKEN_STAR && tok.Type != TOKEN_DOT {
		return Token{Type: TOKEN_IDENT, Literal: tok.Literal}, nil
	}
	return Token{}, fmt.Errorf("expected identifier but got '%s'", tok.Literal)
}