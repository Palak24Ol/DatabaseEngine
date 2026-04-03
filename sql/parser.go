package sql

import (
	"fmt"
	"strconv"
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
	default:
		return nil, fmt.Errorf("unknown statement: '%s'", p.peek().Literal)
	}
}

// ── SELECT ────────────────────────────────────────────────────

func (p *Parser) parseSelect() (*SelectStatement, error) {
	p.consume() // eat SELECT
	cols, err := p.parseColumns()
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

	stmt := &SelectStatement{Columns: cols, TableName: tableTok.Literal, Limit: 0}

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
			p.consume()
			desc = true
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
	p.consume() // eat UPDATE
	tableTok, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expect(TOKEN_SET); err != nil {
		return nil, err
	}

	// parse SET col = val, col = val ...
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

	// optional WHERE
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

// ── CREATE ────────────────────────────────────────────────────

func (p *Parser) parseCreate() (*CreateStatement, error) {
	p.consume()
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
		cols = append(cols, ColumnDef{Name: nameTok.Literal, DataType: typeTok.Literal})
		if p.peek().Type == TOKEN_COMMA {
			p.consume()
		}
	}
	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}
	return &CreateStatement{TableName: tableTok.Literal, Columns: cols}, nil
}

// ── WHERE with AND / OR ───────────────────────────────────────

func (p *Parser) parseWhere() (*WhereClause, error) {
	left, err := p.parseCondition()
	if err != nil {
		return nil, err
	}

	// check for AND / OR
	for p.peek().Type == TOKEN_AND || p.peek().Type == TOKEN_OR {
		logic := p.consume().Literal // "AND" or "OR"
		right, err := p.parseCondition()
		if err != nil {
			return nil, err
		}
		left = &WhereClause{
			IsCompound: true,
			Left:       left,
			Right:      right,
			Logic:      logic,
		}
	}

	return left, nil
}

func (p *Parser) parseCondition() (*WhereClause, error) {
	colTok, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	opTok := p.consume()
	valTok := p.consume()
	return &WhereClause{
		Column:   colTok.Literal,
		Operator: opTok.Literal,
		Value:    valTok.Literal,
	}, nil
}

// ── Column list ───────────────────────────────────────────────

func (p *Parser) parseColumns() ([]string, error) {
	if p.peek().Type == TOKEN_STAR {
		p.consume()
		return []string{"*"}, nil
	}
	var cols []string
	for {
		tok, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		cols = append(cols, tok.Literal)
		if p.peek().Type != TOKEN_COMMA {
			break
		}
		p.consume()
	}
	return cols, nil
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
		return fmt.Errorf("expected token %d but got '%s'", t, tok.Literal)
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