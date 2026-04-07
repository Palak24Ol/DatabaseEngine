package sql

import (
	"strings"
	"unicode"
)

type TokenType int

const (
	TOKEN_SELECT TokenType = iota
	TOKEN_INSERT
	TOKEN_DELETE
	TOKEN_CREATE
	TOKEN_UPDATE
	TOKEN_SET
	TOKEN_FROM
	TOKEN_INTO
	TOKEN_WHERE
	TOKEN_VALUES
	TOKEN_TABLE
	TOKEN_INT
	TOKEN_TEXT
	TOKEN_ORDER
	TOKEN_BY
	TOKEN_ASC
	TOKEN_DESC
	TOKEN_LIMIT
	TOKEN_AND
	TOKEN_OR
	TOKEN_JOIN
	TOKEN_ON
	TOKEN_PRIMARY
	TOKEN_KEY
	TOKEN_UNIQUE
	TOKEN_DATABASE
	TOKEN_USE
	TOKEN_SHOW
	TOKEN_DATABASES
	TOKEN_EXPLAIN
	TOKEN_INDEX
	TOKEN_DROP
	TOKEN_REFERENCES
	TOKEN_FOREIGN
	TOKEN_NOT
	TOKEN_NULL

	TOKEN_STAR
	TOKEN_COMMA
	TOKEN_LPAREN
	TOKEN_RPAREN
	TOKEN_EQUALS
	TOKEN_GT
	TOKEN_LT
	TOKEN_NOT_EQUALS
	TOKEN_GTE
	TOKEN_LTE
	TOKEN_DOT

	TOKEN_IDENT
	TOKEN_STRING
	TOKEN_NUMBER

	TOKEN_EOF
	TOKEN_ILLEGAL
)

type Token struct {
	Type    TokenType
	Literal string
}

var keywords = map[string]TokenType{
	"SELECT":     TOKEN_SELECT,
	"INSERT":     TOKEN_INSERT,
	"DELETE":     TOKEN_DELETE,
	"CREATE":     TOKEN_CREATE,
	"UPDATE":     TOKEN_UPDATE,
	"SET":        TOKEN_SET,
	"FROM":       TOKEN_FROM,
	"INTO":       TOKEN_INTO,
	"WHERE":      TOKEN_WHERE,
	"VALUES":     TOKEN_VALUES,
	"TABLE":      TOKEN_TABLE,
	"INT":        TOKEN_INT,
	"TEXT":       TOKEN_TEXT,
	"ORDER":      TOKEN_ORDER,
	"BY":         TOKEN_BY,
	"ASC":        TOKEN_ASC,
	"DESC":       TOKEN_DESC,
	"LIMIT":      TOKEN_LIMIT,
	"AND":        TOKEN_AND,
	"OR":         TOKEN_OR,
	"JOIN":       TOKEN_JOIN,
	"ON":         TOKEN_ON,
	"PRIMARY":    TOKEN_PRIMARY,
	"KEY":        TOKEN_KEY,
	"UNIQUE":     TOKEN_UNIQUE,
	"DATABASE":   TOKEN_DATABASE,
	"USE":        TOKEN_USE,
	"SHOW":       TOKEN_SHOW,
	"DATABASES":  TOKEN_DATABASES,
	"EXPLAIN":    TOKEN_EXPLAIN,
	"INDEX":      TOKEN_INDEX,
	"DROP":       TOKEN_DROP,
	"REFERENCES": TOKEN_REFERENCES,
	"FOREIGN":    TOKEN_FOREIGN,
	"NOT":        TOKEN_NOT,
	"NULL":       TOKEN_NULL,
}

type Lexer struct {
	input []rune
	pos   int
}

func NewLexer(input string) *Lexer {
	return &Lexer{input: []rune(strings.TrimSpace(input))}
}

func (l *Lexer) Tokenize() []Token {
	var tokens []Token
	for {
		tok := l.nextToken()
		tokens = append(tokens, tok)
		if tok.Type == TOKEN_EOF || tok.Type == TOKEN_ILLEGAL {
			break
		}
	}
	return tokens
}

func (l *Lexer) nextToken() Token {
	l.skipWhitespace()
	if l.pos >= len(l.input) {
		return Token{TOKEN_EOF, ""}
	}
	ch := l.input[l.pos]
	switch ch {
	case '*': l.pos++; return Token{TOKEN_STAR, "*"}
	case ',': l.pos++; return Token{TOKEN_COMMA, ","}
	case '(': l.pos++; return Token{TOKEN_LPAREN, "("}
	case ')': l.pos++; return Token{TOKEN_RPAREN, ")"}
	case '.': l.pos++; return Token{TOKEN_DOT, "."}
	case '=': l.pos++; return Token{TOKEN_EQUALS, "="}
	case '>':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
			l.pos += 2; return Token{TOKEN_GTE, ">="}
		}
		l.pos++; return Token{TOKEN_GT, ">"}
	case '<':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
			l.pos += 2; return Token{TOKEN_LTE, "<="}
		}
		l.pos++; return Token{TOKEN_LT, "<"}
	case '!':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
			l.pos += 2; return Token{TOKEN_NOT_EQUALS, "!="}
		}
	case '\'': return l.readString()
	}
	if unicode.IsDigit(ch) || (ch == '-' && l.pos+1 < len(l.input) && unicode.IsDigit(l.input[l.pos+1])) {
		return l.readNumber()
	}
	if unicode.IsLetter(ch) || ch == '_' {
		return l.readIdent()
	}
	l.pos++
	return Token{TOKEN_ILLEGAL, string(ch)}
}

func (l *Lexer) readIdent() Token {
	start := l.pos
	for l.pos < len(l.input) && (unicode.IsLetter(l.input[l.pos]) || l.input[l.pos] == '_' || unicode.IsDigit(l.input[l.pos])) {
		l.pos++
	}
	word := strings.ToUpper(string(l.input[start:l.pos]))
	if tt, ok := keywords[word]; ok {
		return Token{tt, word}
	}
	return Token{TOKEN_IDENT, string(l.input[start:l.pos])}
}

func (l *Lexer) readNumber() Token {
	start := l.pos
	if l.input[l.pos] == '-' { l.pos++ }
	for l.pos < len(l.input) && unicode.IsDigit(l.input[l.pos]) { l.pos++ }
	return Token{TOKEN_NUMBER, string(l.input[start:l.pos])}
}

func (l *Lexer) readString() Token {
	l.pos++
	start := l.pos
	for l.pos < len(l.input) && l.input[l.pos] != '\'' { l.pos++ }
	val := string(l.input[start:l.pos])
	l.pos++
	return Token{TOKEN_STRING, val}
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(l.input[l.pos]) { l.pos++ }
}