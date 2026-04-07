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
	EXPLAIN_STMT
	CREATE_INDEX_STMT
	DROP_INDEX_STMT
)

type Statement interface{ stmtType() StatementType }

type SelectExpr struct {
	Star    bool
	Table   string
	Column  string
	AggFunc string
	AggArg  string
	AggStar bool
}

func (e SelectExpr) DisplayName() string {
	if e.Star { return "*" }
	if e.AggFunc != "" {
		if e.AggStar { return e.AggFunc + "(*)" }
		return e.AggFunc + "(" + e.AggArg + ")"
	}
	if e.Table != "" { return e.Table + "." + e.Column }
	return e.Column
}

type SelectStatement struct {
	Exprs     []SelectExpr
	TableName string
	Join      *JoinClause
	Where     *WhereClause
	OrderBy   *OrderByClause
	Limit     int
}
func (s *SelectStatement) stmtType() StatementType { return SELECT_STMT }

type JoinClause struct {
	TableName  string
	LeftTable  string
	LeftCol    string
	RightTable string
	RightCol   string
}

type InsertStatement struct {
	TableName string
	Values    []string
}
func (s *InsertStatement) stmtType() StatementType { return INSERT_STMT }

type UpdateStatement struct {
	TableName string
	Sets      []SetClause
	Where     *WhereClause
}
type SetClause struct{ Column, Value string }
func (s *UpdateStatement) stmtType() StatementType { return UPDATE_STMT }

type DeleteStatement struct {
	TableName string
	Where     *WhereClause
}
func (s *DeleteStatement) stmtType() StatementType { return DELETE_STMT }

type CreateStatement struct {
	TableName string
	Columns   []ColumnDef
}
type ColumnDef struct {
	Name        string
	DataType    string
	PrimaryKey  bool
	Unique      bool
	ForeignKey  *ForeignKeyDef
}
type ForeignKeyDef struct {
	RefTable  string
	RefColumn string
}
func (s *CreateStatement) stmtType() StatementType { return CREATE_STMT }

type CreateDBStatement struct{ DBName string }
func (s *CreateDBStatement) stmtType() StatementType { return CREATE_DB_STMT }

type UseDBStatement struct{ DBName string }
func (s *UseDBStatement) stmtType() StatementType { return USE_DB_STMT }

type ShowDBStatement struct{}
func (s *ShowDBStatement) stmtType() StatementType { return SHOW_DB_STMT }

// EXPLAIN wraps another statement
type ExplainStatement struct{ Inner Statement }
func (s *ExplainStatement) stmtType() StatementType { return EXPLAIN_STMT }

// CREATE INDEX
type CreateIndexStatement struct {
	IndexName string
	TableName string
	Column    string
	Unique    bool
}
func (s *CreateIndexStatement) stmtType() StatementType { return CREATE_INDEX_STMT }

// DROP INDEX
type DropIndexStatement struct{ IndexName string }
func (s *DropIndexStatement) stmtType() StatementType { return DROP_INDEX_STMT }

type WhereClause struct {
	Column     string
	Operator   string
	Value      string
	Left       *WhereClause
	Right      *WhereClause
	Logic      string
	IsCompound bool
}

type OrderByClause struct {
	Column string
	Desc   bool
}