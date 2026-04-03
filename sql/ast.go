package sql

type StatementType int

const (
	SELECT_STMT StatementType = iota
	INSERT_STMT
	DELETE_STMT
	CREATE_STMT
	UPDATE_STMT
)

type Statement interface {
	stmtType() StatementType
}

// ── SELECT ────────────────────────────────────────────────────

type SelectStatement struct {
	Columns   []string
	TableName string
	Where     *WhereClause
	OrderBy   *OrderByClause // NEW
	Limit     int            // NEW — 0 means no limit
}

func (s *SelectStatement) stmtType() StatementType { return SELECT_STMT }

// ── INSERT ────────────────────────────────────────────────────

type InsertStatement struct {
	TableName string
	Values    []string
}

func (s *InsertStatement) stmtType() StatementType { return INSERT_STMT }

// ── UPDATE ────────────────────────────────────────────────────
// UPDATE table SET col1 = val1, col2 = val2 WHERE col = val

type UpdateStatement struct {
	TableName string
	Sets      []SetClause
	Where     *WhereClause
}

type SetClause struct {
	Column string
	Value  string
}

func (s *UpdateStatement) stmtType() StatementType { return UPDATE_STMT }

// ── DELETE ────────────────────────────────────────────────────

type DeleteStatement struct {
	TableName string
	Where     *WhereClause
}

func (s *DeleteStatement) stmtType() StatementType { return DELETE_STMT }

// ── CREATE ────────────────────────────────────────────────────

type CreateStatement struct {
	TableName string
	Columns   []ColumnDef
}

type ColumnDef struct {
	Name     string
	DataType string
}

func (s *CreateStatement) stmtType() StatementType { return CREATE_STMT }

// ── WHERE — now supports AND / OR ─────────────────────────────

type WhereClause struct {
	// single condition
	Column   string
	Operator string
	Value    string

	// compound conditions (AND / OR)
	Left     *WhereClause
	Right    *WhereClause
	Logic    string // "AND" or "OR"
	IsCompound bool
}

// ── ORDER BY ──────────────────────────────────────────────────

type OrderByClause struct {
	Column string
	Desc   bool // false = ASC, true = DESC
}