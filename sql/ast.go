package sql

type StatementType int

const (
	SELECT_STMT StatementType = iota
	INSERT_STMT
	DELETE_STMT
	CREATE_STMT
	UPDATE_STMT
	CREATE_DB_STMT
	USE_DB_STMT
	SHOW_DB_STMT
)

type Statement interface{ stmtType() StatementType }

// ── SelectExpr — one item in SELECT list ─────────────────────
type SelectExpr struct {
	Star    bool   // bare *
	Table   string // for table.col
	Column  string // column name
	AggFunc string // COUNT SUM AVG MAX MIN
	AggArg  string // argument inside agg
	AggStar bool   // COUNT(*)
}

func (e SelectExpr) DisplayName() string {
	if e.Star {
		return "*"
	}
	if e.AggFunc != "" {
		if e.AggStar {
			return e.AggFunc + "(*)"
		}
		return e.AggFunc + "(" + e.AggArg + ")"
	}
	if e.Table != "" {
		return e.Table + "." + e.Column
	}
	return e.Column
}

// ── SELECT ────────────────────────────────────────────────────
type SelectStatement struct {
	Exprs     []SelectExpr
	TableName string
	Join      *JoinClause
	Where     *WhereClause
	OrderBy   *OrderByClause
	Limit     int
}

func (s *SelectStatement) stmtType() StatementType { return SELECT_STMT }

// ── JOIN ──────────────────────────────────────────────────────
type JoinClause struct {
	TableName  string
	LeftTable  string
	LeftCol    string
	RightTable string
	RightCol   string
}

// ── INSERT ────────────────────────────────────────────────────
type InsertStatement struct {
	TableName string
	Values    []string
}

func (s *InsertStatement) stmtType() StatementType { return INSERT_STMT }

// ── UPDATE ────────────────────────────────────────────────────
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

// ── CREATE TABLE ──────────────────────────────────────────────
type CreateStatement struct {
	TableName string
	Columns   []ColumnDef
}

type ColumnDef struct {
	Name       string
	DataType   string
	PrimaryKey bool
	Unique     bool
}

func (s *CreateStatement) stmtType() StatementType { return CREATE_STMT }

// ── CREATE DATABASE ───────────────────────────────────────────
type CreateDBStatement struct{ DBName string }

func (s *CreateDBStatement) stmtType() StatementType { return CREATE_DB_STMT }

// ── USE DATABASE ──────────────────────────────────────────────
type UseDBStatement struct{ DBName string }

func (s *UseDBStatement) stmtType() StatementType { return USE_DB_STMT }

// ── SHOW DATABASES ────────────────────────────────────────────
type ShowDBStatement struct{}

func (s *ShowDBStatement) stmtType() StatementType { return SHOW_DB_STMT }

// ── WHERE (AND/OR) ────────────────────────────────────────────
type WhereClause struct {
	Column     string
	Operator   string
	Value      string
	Left       *WhereClause
	Right      *WhereClause
	Logic      string
	IsCompound bool
}

// ── ORDER BY ──────────────────────────────────────────────────
type OrderByClause struct {
	Column string
	Desc   bool
}
