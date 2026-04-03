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
	case TOKEN_SELECT:
		return p.parseSelect()
	case TOKEN_INSERT:
		return p.parseInsert()
	case TOKEN_DELETE:
		return p.parseDelete()
	case TOKEN_CREATE:
		return p.parseCreate()
	case TOKEN_UPDATE:
		return p.parseUpdate()
	case TOKEN_USE:
		return p.parseUse()
	case TOKEN_SHOW:
		return p.parseShow()
	default:
		return nil, fmt.Errorf("unknown statement: '%s'", p.peek().Literal)
	}
}

// ── SELECT ────────────────────────────────────────────────────

func (p *Parser) parseSelect() (*SelectStatement, error) {
	p.consume()

	exprs, err := p.parseSelectExprs()
	if err != nil {
		return nil, err
	}

	if err := p.expect(TOKEN_FROM); err != nil {
		return nil, err
	}

	tableTok, err := p.expectIdent()
	if err != nil {
		return nil, err
	}

	stmt := &SelectStatement{Exprs: exprs, TableName: tableTok.Literal}

	// optional JOIN
	if p.peek().Type == TOKEN_JOIN {
		p.consume()
		joinTable, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		if err := p.expect(TOKEN_ON); err != nil {
			return nil, err
		}
		// parse: leftTable.leftCol = rightTable.rightCol
		leftTable, leftCol, err := p.parseQualifiedCol()
		if err != nil {
			return nil, err
		}
		if err := p.expect(TOKEN_EQUALS); err != nil {
			return nil, err
		}
		rightTable, rightCol, err := p.parseQualifiedCol()
		if err != nil {
			return nil, err
		}
		stmt.Join = &JoinClause{
			TableName:  joinTable.Literal,
			LeftTable:  leftTable,
			LeftCol:    leftCol,
			RightTable: rightTable,
			RightCol:   rightCol,
		}
	}

	// optional WHERE
	if p.peek().Type == TOKEN_WHERE {
		p.consume()
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// optional ORDER BY
	if p.peek().Type == TOKEN_ORDER {
		p.consume()
		if err := p.expect(TOKEN_BY); err != nil {
			return nil, err
		}
		colTok, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		desc := false
		if p.peek().Type == TOKEN_DESC {
			p.consume(); desc = true
		} else if p.peek().Type == TOKEN_ASC {
			p.consume()
		}
		stmt.OrderBy = &OrderByClause{Column: colTok.Literal, Desc: desc}
	}

	// optional LIMIT
	if p.peek().Type == TOKEN_LIMIT {
		p.consume()
		numTok := p.consume()
		n, err := strconv.Atoi(numTok.Literal)
		if err != nil {
			return nil, fmt.Errorf("LIMIT expects a number")
		}
		stmt.Limit = n
	}

	return stmt, nil
}

// parseSelectExprs parses: *, col, table.col, COUNT(*), SUM(col), ...
func (p *Parser) parseSelectExprs() ([]SelectExpr, error) {
	if p.peek().Type == TOKEN_STAR {
		p.consume()
		return []SelectExpr{{Star: true}}, nil
	}

	var exprs []SelectExpr
	for {
		expr, err := p.parseOneExpr()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
		if p.peek().Type != TOKEN_COMMA {
			break
		}
		p.consume()
	}
	return exprs, nil
}

func (p *Parser) parseOneExpr() (SelectExpr, error) {
	tok := p.consume()

	// check for aggregate functions: COUNT, SUM, AVG, MAX, MIN
	upper := strings.ToUpper(tok.Literal)
	if isAggFunc(upper) && p.peek().Type == TOKEN_LPAREN {
		p.consume() // eat (
		expr := SelectExpr{AggFunc: upper}
		if p.peek().Type == TOKEN_STAR {
			p.consume()
			expr.AggStar = true
		} else {
			argTok := p.consume()
			expr.AggArg = argTok.Literal
		}
		if err := p.expect(TOKEN_RPAREN); err != nil {
			return SelectExpr{}, err
		}
		return expr, nil
	}

	// check for table.col
	if p.peek().Type == TOKEN_DOT {
		p.consume() // eat .
		colTok, err := p.expectIdent()
		if err != nil {
			return SelectExpr{}, err
		}
		return SelectExpr{Table: tok.Literal, Column: colTok.Literal}, nil
	}

	// plain column
	return SelectExpr{Column: tok.Literal}, nil
}

func isAggFunc(s string) bool {
	switch s {
	case "COUNT", "SUM", "AVG", "MAX", "MIN":
		return true
	}
	return false
}

// parseQualifiedCol parses table.col
func (p *Parser) parseQualifiedCol() (table, col string, err error) {
	tableTok, err := p.expectIdent()
	if err != nil {
		return "", "", err
	}
	if err := p.expect(TOKEN_DOT); err != nil {
		return "", "", err
	}
	colTok, err := p.expectIdent()
	if err != nil {
		return "", "", err
	}
	return tableTok.Literal, colTok.Literal, nil
}

// ── INSERT ────────────────────────────────────────────────────

func (p *Parser) parseInsert() (*InsertStatement, error) {
	p.consume()
	if err := p.expect(TOKEN_INTO); err != nil {
		return nil, err
	}
	tableTok, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expect(TOKEN_VALUES); err != nil {
		return nil, err
	}
	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}
	var values []string
	for p.peek().Type != TOKEN_RPAREN && p.peek().Type != TOKEN_EOF {
		tok := p.consume()
		values = append(values, tok.Literal)
		if p.peek().Type == TOKEN_COMMA {
			p.consume()
		}
	}
	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}
	return &InsertStatement{TableName: tableTok.Literal, Values: values}, nil
}

// ── UPDATE ────────────────────────────────────────────────────

func (p *Parser) parseUpdate() (*UpdateStatement, error) {
	p.consume()
	tableTok, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expect(TOKEN_SET); err != nil {
		return nil, err
	}
	var sets []SetClause
	for {
		colTok, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		if err := p.expect(TOKEN_EQUALS); err != nil {
			return nil, err
		}
		valTok := p.consume()
		sets = append(sets, SetClause{Column: colTok.Literal, Value: valTok.Literal})
		if p.peek().Type != TOKEN_COMMA {
			break
		}
		p.consume()
	}
	stmt := &UpdateStatement{TableName: tableTok.Literal, Sets: sets}
	if p.peek().Type == TOKEN_WHERE {
		p.consume()
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}
	return stmt, nil
}

// ── DELETE ────────────────────────────────────────────────────

func (p *Parser) parseDelete() (*DeleteStatement, error) {
	p.consume()
	if err := p.expect(TOKEN_FROM); err != nil {
		return nil, err
	}
	tableTok, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	stmt := &DeleteStatement{TableName: tableTok.Literal}
	if p.peek().Type == TOKEN_WHERE {
		p.consume()
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}
	return stmt, nil
}

// ── CREATE TABLE / DATABASE ───────────────────────────────────

func (p *Parser) parseCreate() (Statement, error) {
	p.consume() // eat CREATE

	if p.peek().Type == TOKEN_DATABASE {
		p.consume()
		nameTok, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return &CreateDBStatement{DBName: nameTok.Literal}, nil
	}

	if err := p.expect(TOKEN_TABLE); err != nil {
		return nil, err
	}
	tableTok, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}
	var cols []ColumnDef
	for p.peek().Type != TOKEN_RPAREN && p.peek().Type != TOKEN_EOF {
		nameTok, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		typeTok := p.consume()
		col := ColumnDef{Name: nameTok.Literal, DataType: typeTok.Literal}

		// check for PRIMARY KEY or UNIQUE
		if p.peek().Type == TOKEN_PRIMARY {
			p.consume()
			p.consume() // eat KEY
			col.PrimaryKey = true
		} else if p.peek().Type == TOKEN_UNIQUE {
			p.consume()
			col.Unique = true
		}

		cols = append(cols, col)
		if p.peek().Type == TOKEN_COMMA {
			p.consume()
		}
	}
	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}
	return &CreateStatement{TableName: tableTok.Literal, Columns: cols}, nil
}

// ── USE DATABASE ──────────────────────────────────────────────

func (p *Parser) parseUse() (*UseDBStatement, error) {
	p.consume()
	nameTok, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	return &UseDBStatement{DBName: nameTok.Literal}, nil
}

// ── SHOW DATABASES ────────────────────────────────────────────

func (p *Parser) parseShow() (*ShowDBStatement, error) {
	p.consume()
	p.consume() // eat DATABASES
	return &ShowDBStatement{}, nil
}

// ── WHERE with AND/OR ─────────────────────────────────────────

func (p *Parser) parseWhere() (*WhereClause, error) {
	left, err := p.parseCondition()
	if err != nil {
		return nil, err
	}
	for p.peek().Type == TOKEN_AND || p.peek().Type == TOKEN_OR {
		logic := p.consume().Literal
		right, err := p.parseCondition()
		if err != nil {
			return nil, err
		}
		left = &WhereClause{IsCompound: true, Left: left, Right: right, Logic: logic}
	}
	return left, nil
}

func (p *Parser) parseCondition() (*WhereClause, error) {
	// could be col or table.col
	colTok, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	colName := colTok.Literal
	if p.peek().Type == TOKEN_DOT {
		p.consume()
		rightTok, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		colName = colTok.Literal + "." + rightTok.Literal
	}
	opTok := p.consume()
	valTok := p.consume()
	return &WhereClause{Column: colName, Operator: opTok.Literal, Value: valTok.Literal}, nil
}

// ── Helpers ───────────────────────────────────────────────────

func (p *Parser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{TOKEN_EOF, ""}
	}
	return p.tokens[p.pos]
}

func (p *Parser) consume() Token {
	tok := p.peek()
	p.pos++
	return tok
}

func (p *Parser) expect(t TokenType) error {
	tok := p.consume()
	if tok.Type != t {
		return fmt.Errorf("expected token type %d but got '%s'", t, tok.Literal)
	}
	return nil
}

func (p *Parser) expectIdent() (Token, error) {
	tok := p.consume()
	if tok.Type != TOKEN_IDENT {
		return Token{}, fmt.Errorf("expected identifier but got '%s'", tok.Literal)
	}
	return tok, nil
}