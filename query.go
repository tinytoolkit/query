package query

import (
	"bytes"
	"strconv"
	"sync"
)

type (
	// Query is a struct representing a query statement
	Query struct {
		query []byte
		args  []any
	}

	// Column is a struct representing a column in a CREATE TABLE statement
	Column struct {
		Name string
		Type string
	}

	// ColumnOptions is a struct representing the options for a column in a CREATE TABLE statement
	ColumnOptions struct {
		DefaultValue       string
		PrimaryKey         bool
		NotNull            bool
		Unique             bool
		Identity           bool
		IdentityGeneration string
		Check              string
	}

	// Field is a struct to hold the name and value of a field
	Field struct {
		Name  string
		Value any
	}
)

// Begin is a function to start building a BEGIN query statement
func Begin() *Query {
	q := getQuery()
	q.query = append(q.query, "BEGIN; "...)
	return q
}

// Commit builds the query string for a COMMIT statement
func (q *Query) Commit() *Query {
	q.query = append(q.query, "COMMIT;"...)
	return q
}

// CreateSchema is a function that returns a CREATE SCHEMA query
func CreateSchema(name string, owner string) string {
	return getQuery().CreateSchema(name, owner).String()
}

// CreateSchema builds the query string for a CREATE SCHEMA statement
func (q *Query) CreateSchema(name string, owner string) *Query {
	q.query = append(q.query, "CREATE SCHEMA "...)
	q.query = append(q.query, name...)
	if owner != "" {
		q.query = append(q.query, " AUTHORIZATION "...)
		q.query = append(q.query, owner...)
	}
	q.query = append(q.query, "; "...)
	return q
}

// AlterSchemaName is a function that returns a RENAME SCHEMA query
func AlterSchemaName(name string, newName string) string {
	return getQuery().AlterSchemaName(name, newName).String()
}

// AlterSchemaName builds the query string for an ALTER SCHEMA ... RENAME TO statement
func (q *Query) AlterSchemaName(name string, newName string) *Query {
	q.query = append(q.query, "ALTER SCHEMA "...)
	q.query = append(q.query, name...)
	q.query = append(q.query, " RENAME TO "...)
	q.query = append(q.query, newName...)
	q.query = append(q.query, "; "...)
	return q
}

// AlterSchemaOwner is a function that returns an ALTER SCHEMA OWNER TO query
func AlterSchemaOwner(name string, owner string) string {
	return getQuery().AlterSchemaOwner(name, owner).String()
}

// AlterSchemaOwner builds the query string for an ALTER SCHEMA ... OWNER TO statement
func (q *Query) AlterSchemaOwner(name string, owner string) *Query {
	q.query = append(q.query, "ALTER SCHEMA "...)
	q.query = append(q.query, name...)
	q.query = append(q.query, " OWNER TO "...)
	q.query = append(q.query, owner...)
	q.query = append(q.query, "; "...)
	return q
}

// DropSchema is a function that returns a DROP SCHEMA query
func DropSchema(name string, cascade bool) string {
	return getQuery().DropSchema(name, cascade).String()
}

// DropSchema builds the query string for a DROP SCHEMA statement
func (q *Query) DropSchema(name string, cascade bool) *Query {
	q.query = append(q.query, "DROP SCHEMA "...)
	q.query = append(q.query, name...)
	if cascade {
		q.query = append(q.query, " CASCADE"...)
	} else {
		q.query = append(q.query, " RESTRICT"...)
	}
	q.query = append(q.query, "; "...)
	return q
}

// CreateTable is a function that returns a CREATE TABLE query
func CreateTable(table string, columns []*Column) string {
	return getQuery().CreateTable(table, columns).String()
}

// CreateTable is a function to start building a CREATE TABLE query statement
func (q *Query) CreateTable(table string, columns []*Column) *Query {
	q.query = append(q.query, "CREATE TABLE "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ("...)
	for i, column := range columns {
		q.query = append(q.query, column.Name...)
		q.query = append(q.query, " "...)
		q.query = append(q.query, column.Type...)
		if i < len(columns)-1 {
			q.query = append(q.query, ", "...)
		}
	}
	q.query = append(q.query, "); "...)
	return q
}

// CommentOnTable is a function that returns a COMMENT ON TABLE query
func CommentOnTable(table string, comment string) string {
	return getQuery().CommentOnTable(table, comment).String()
}

// CommentOnTable builds the query string for a COMMENT ON TABLE statement
func (q *Query) CommentOnTable(table string, comment string) *Query {
	q.query = append(q.query, "COMMENT ON TABLE "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " IS "...)
	q.query = append(q.query, "'"...)
	q.query = append(q.query, comment...)
	q.query = append(q.query, "'; "...)
	return q
}

// AlterTableName is a function that returns a RENAME TABLE query
func AlterTableName(oldName string, newName string) string {
	return getQuery().AlterTableName(oldName, newName).String()
}

// AlterTableName builds the query string for a RENAME TABLE statement
func (q *Query) AlterTableName(oldName string, newName string) *Query {
	q.query = append(q.query, "ALTER TABLE "...)
	q.query = append(q.query, oldName...)
	q.query = append(q.query, " RENAME TO "...)
	q.query = append(q.query, newName...)
	q.query = append(q.query, "; "...)
	return q
}

// AlterRowSecurity is a function that returns a SET ROW SECURITY query
func AlterRowSecurity(table string, enable bool) string {
	return getQuery().AlterRowSecurity(table, enable).String()
}

// AlterRowSecurity enables or disables row level security for a table
func (q *Query) AlterRowSecurity(table string, enable bool) *Query {
	q.query = append(q.query, "ALTER TABLE "...)
	q.query = append(q.query, table...)
	if enable {
		q.query = append(q.query, " ENABLE ROW LEVEL SECURITY"...)
	} else {
		q.query = append(q.query, " DISABLE ROW LEVEL SECURITY"...)
	}
	q.query = append(q.query, "; "...)
	return q
}

// AlterForceRowSecurity is a function that returns a SET FORCE ROW SECURITY query
func AlterForceRowSecurity(table string, force bool) string {
	return getQuery().AlterForceRowSecurity(table, force).String()
}

// AlterForceRowSecurity sets the FORCE ROW LEVEL SECURITY property for a table
func (q *Query) AlterForceRowSecurity(table string, force bool) *Query {
	q.query = append(q.query, "ALTER TABLE "...)
	q.query = append(q.query, table...)
	if force {
		q.query = append(q.query, " FORCE ROW LEVEL SECURITY"...)
	} else {
		q.query = append(q.query, " NO FORCE ROW LEVEL SECURITY"...)
	}
	q.query = append(q.query, "; "...)
	return q
}

// DropTable is a function that returns a DROP TABLE query
func DropTable(table string, cascade bool) string {
	return getQuery().DropTable(table, cascade).String()
}

// DropTable builds the query string for a DROP TABLE statement with optional CASCADE
func (q *Query) DropTable(table string, cascade bool) *Query {
	q.query = append(q.query, "DROP TABLE "...)
	q.query = append(q.query, table...)
	if cascade {
		q.query = append(q.query, " CASCADE"...)
	} else {
		q.query = append(q.query, " RESTRICT"...)
	}
	q.query = append(q.query, "; "...)
	return q
}

// CommentOnColumn is a function that returns a COMMENT ON COLUMN query
func CommentOnColumn(table string, column string, comment string) string {
	return getQuery().CommentOnColumn(table, column, comment).String()
}

// CommentOnColumn builds the query string for a COMMENT ON COLUMN statement
func (q *Query) CommentOnColumn(table string, column string, comment string) *Query {
	q.query = append(q.query, "COMMENT ON COLUMN "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, "."...)
	q.query = append(q.query, column...)
	q.query = append(q.query, " IS "...)
	q.query = append(q.query, "'"...)
	q.query = append(q.query, comment...)
	q.query = append(q.query, "'; "...)
	return q
}

// AddColumn is a function that returns an ADD COLUMN query
func AddColumn(table string, name string, dataType string, opts *ColumnOptions) string {
	return getQuery().AddColumn(table, name, dataType, opts).String()
}

// AddColumn builds the query string for an ADD COLUMN statement
func (q *Query) AddColumn(table string, name string, dataType string, opts *ColumnOptions) *Query {
	q.query = append(q.query, "ALTER TABLE "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ADD COLUMN "...)
	q.query = append(q.query, name...)
	q.query = append(q.query, " "...)
	q.query = append(q.query, dataType...)
	if opts.DefaultValue != "" {
		q.query = append(q.query, " DEFAULT "...)
		q.query = append(q.query, opts.DefaultValue...)
	}
	if opts.PrimaryKey {
		q.query = append(q.query, " PRIMARY KEY"...)
	}
	if opts.NotNull {
		q.query = append(q.query, " NOT NULL"...)
	}
	if opts.Unique {
		q.query = append(q.query, " UNIQUE"...)
	}
	if opts.Identity {
		q.query = append(q.query, " GENERATED "...)
		q.query = append(q.query, opts.IdentityGeneration...)
		q.query = append(q.query, " AS IDENTITY"...)
	}
	if opts.Check != "" {
		q.query = append(q.query, " CHECK ("...)
		q.query = append(q.query, opts.Check...)
		q.query = append(q.query, ")"...)
	}
	q.query = append(q.query, "; "...)
	return q
}

// AlterColumnName is a function that returns a RENAME COLUMN query
func AlterColumnName(table string, column string, newColumn string) string {
	return getQuery().AlterColumnName(table, column, newColumn).String()
}

// AlterColumnName builds the query string for an ALTER TABLE RENAME COLUMN statement
func (q *Query) AlterColumnName(table string, oldName string, newName string) *Query {
	q.query = append(q.query, "ALTER TABLE "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " RENAME COLUMN "...)
	q.query = append(q.query, oldName...)
	q.query = append(q.query, " TO "...)
	q.query = append(q.query, newName...)
	q.query = append(q.query, "; "...)
	return q
}

// AlterColumnType is a function that returns an ALTER COLUMN TYPE query
func AlterColumnType(table string, column string, dataType string) string {
	return getQuery().AlterColumnType(table, column, dataType).String()
}

// AlterColumnType builds the query string for an ALTER TABLE ALTER COLUMN TYPE statement
func (q *Query) AlterColumnType(table string, column string, dataType string) *Query {
	q.query = append(q.query, "ALTER TABLE "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ALTER COLUMN "...)
	q.query = append(q.query, column...)
	q.query = append(q.query, " TYPE "...)
	q.query = append(q.query, dataType...)
	q.query = append(q.query, "; "...)
	return q
}

// AlterColumnSetDefault is a function that returns an ALTER COLUMN SET DEFAULT query
func AlterColumnSetDefault(table string, column string, defaultValue string) string {
	return getQuery().AlterColumnSetDefault(table, column, defaultValue).String()
}

// AlterColumnDefault builds the query string for an ALTER TABLE ALTER COLUMN SET DEFAULT statement
func (q *Query) AlterColumnSetDefault(table string, column string, defaultValue string) *Query {
	q.query = append(q.query, "ALTER TABLE "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ALTER COLUMN "...)
	q.query = append(q.query, column...)
	q.query = append(q.query, " SET DEFAULT "...)
	q.query = append(q.query, "'"...)
	q.query = append(q.query, defaultValue...)
	q.query = append(q.query, "'; "...)
	return q
}

// AlterColumnDropDefault is a function that returns an ALTER COLUMN DROP DEFAULT query
func AlterColumnDropDefault(table string, column string) string {
	return getQuery().AlterColumnDropDefault(table, column).String()
}

// AlterColumnDropDefault generates an ALTER TABLE ALTER COLUMN statement to drop the default value of a column
func (q *Query) AlterColumnDropDefault(table string, column string) *Query {
	q.query = append(q.query, "ALTER TABLE "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ALTER COLUMN "...)
	q.query = append(q.query, column...)
	q.query = append(q.query, " DROP DEFAULT"...)
	q.query = append(q.query, "; "...)
	return q
}

// AlterColumnNull is a function that returns an ALTER COLUMN SET NOT NULL or DROP NOT NULL query
func AlterColumnNull(table string, column string, nullable bool) string {
	return getQuery().AlterColumnNull(table, column, nullable).String()
}

// AlterColumnNull alters a column to allow or disallow null values
func (q *Query) AlterColumnNull(table string, column string, nullable bool) *Query {
	q.query = append(q.query, "ALTER TABLE "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ALTER COLUMN "...)
	q.query = append(q.query, column...)
	if nullable {
		q.query = append(q.query, " DROP NOT NULL"...)
	} else {
		q.query = append(q.query, " SET NOT NULL"...)
	}
	q.query = append(q.query, "; "...)
	return q
}

// AlterColumnUsing is a function that returns an ALTER COLUMN USING query
func AlterColumnUsing(table string, column string, expression string) string {
	return getQuery().AlterColumnUsing(table, column, expression).String()
}

// AlterColumnUsing is a function that returns an ALTER COLUMN USING query
func (q *Query) AlterColumnUsing(table string, column string, expression string) *Query {
	q.query = append(q.query, "ALTER TABLE "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ALTER COLUMN "...)
	q.query = append(q.query, column...)
	q.query = append(q.query, " USING "...)
	q.query = append(q.query, expression...)
	q.query = append(q.query, "; "...)
	return q
}

// DropColumn is a function that returns a DROP COLUMN query
func DropColumn(table string, column string) string {
	return getQuery().DropColumn(table, column).String()
}

// DropColumn builds the query string for an ALTER TABLE DROP COLUMN statement
func (q *Query) DropColumn(table string, column string) *Query {
	q.query = append(q.query, "ALTER TABLE "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " DROP COLUMN "...)
	q.query = append(q.query, column...)
	q.query = append(q.query, "; "...)
	return q
}

// InsertInto is a function to start building an INSERT INTO query statement
func InsertInto(table string, fields ...string) *Query {
	return getQuery().InsertInto(table, fields...)
}

// InsertInto builds the query string for an INSERT INTO statement
func (q *Query) InsertInto(table string, fields ...string) *Query {
	q.query = append(q.query, "INSERT INTO "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ("...)
	for i, f := range fields {
		if i > 0 {
			q.query = append(q.query, ", "...)
		}
		q.query = append(q.query, f...)
	}
	q.query = append(q.query, ')')
	return q
}

// Values builds the query string for the VALUES clause in an INSERT INTO statement
func (q *Query) Values(values ...any) *Query {
	if !bytes.Contains(q.query, []byte(" VALUES")) {
		q.query = append(q.query, " VALUES"...)
	}
	q.query = append(q.query, " ("...)
	for i := range values {
		if i > 0 {
			q.query = append(q.query, ", "...)
		}
		q.query = append(q.query, '?')
	}
	q.query = append(q.query, ')')
	q.args = append(q.args, values...)
	return q
}

// DeleteFrom is a function to start building a DELETE FROM query statement
func DeleteFrom(table string) *Query {
	return getQuery().DeleteFrom(table)
}

// DeleteFrom is a method for the Query struct and builds the query string for a DELETE statement
func (q *Query) DeleteFrom(table string) *Query {
	q.query = append(q.query, "DELETE FROM "...)
	q.query = append(q.query, table...)
	return q
}

// Update is a function to start building an UPDATE query statement
func Update(table string) *Query {
	return getQuery().Update(table)
}

// Update builds the query string for an UPDATE statement
func (q *Query) Update(table string) *Query {
	q.query = append(q.query, "UPDATE "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " SET "...)
	return q
}

// Set builds the query string for the SET clause in an UPDATE statement
func (q *Query) Set(fields []*Field) *Query {
	for i, field := range fields {
		q.query = append(q.query, field.Name...)
		q.query = append(q.query, " = ?"...)
		if i < len(fields)-1 {
			q.query = append(q.query, ", "...)
		}
		q.args = append(q.args, field.Value)
	}
	return q
}

// Select is a function to start building a SELECT query statement
func Select(exprs ...string) *Query {
	return getQuery().Select(exprs...)
}

// Select builds the query string for the SELECT clause
func (q *Query) Select(exprs ...string) *Query {
	q.query = append(q.query, "SELECT "...)
	for i, expr := range exprs {
		if i > 0 {
			q.query = append(q.query, ", "...)
		}
		q.query = append(q.query, expr...)
	}
	return q
}

// From builds the query string for the FROM clause
func (q *Query) From(table string) *Query {
	q.query = append(q.query, " FROM "...)
	q.query = append(q.query, table...)
	return q
}

// With is a function to start building a WITH query statement
func With(name string, query *Query) *Query {
	return getQuery().With(name, query)
}

// With builds the query string for the WITH clause
func (q *Query) With(name string, query *Query) *Query {
	q.query = append(q.query, "WITH "...)
	q.query = append(q.query, name...)
	q.query = append(q.query, " AS ("...)
	q.query = append(q.query, query.query...)
	q.query = append(q.query, ") "...)
	q.args = append(q.args, query.args...)
	return q
}

// Where builds the query string for the WHERE clause
func (q *Query) Where(expr string, value any) *Query {
	if !bytes.Contains(q.query, []byte("WHERE")) {
		q.query = append(q.query, " WHERE "...)
	}
	q.query = append(q.query, expr...)
	q.args = append(q.args, value)
	return q
}

// And builds the query string for the AND clause
func (q *Query) And() *Query {
	q.query = append(q.query, " AND "...)
	return q
}

// Or builds the query string for the OR clause
func (q *Query) Or() *Query {
	q.query = append(q.query, " OR "...)
	return q
}

// In builds the query string for the IN clause
func (q *Query) In(column string, values any) *Query {
	if !bytes.Contains(q.query, []byte(" WHERE")) {
		q.query = append(q.query, " WHERE "...)
	}
	q.query = append(q.query, column...)
	q.query = append(q.query, " IN ("...)
	switch v := values.(type) {
	case []int:
		for idx, value := range v {
			q.query = append(q.query, '?')
			if len(v) > 0 && idx < len(v)-1 {
				q.query = append(q.query, ", "...)
			}
			q.args = append(q.args, value)
		}
	case []string:
		for idx, value := range v {
			q.query = append(q.query, '?')
			if len(v) > 0 && idx < len(v)-1 {
				q.query = append(q.query, ", "...)
			}
			q.args = append(q.args, value)
		}
	default:
		for idx, value := range v.([]any) {
			q.query = append(q.query, '?')
			if len(v.([]any)) > 0 && idx < len(v.([]any))-1 {
				q.query = append(q.query, ", "...)
			}
			q.args = append(q.args, value)
		}
	}
	q.query = append(q.query, ')')
	return q
}

// Join builds the query string for the JOIN clause
func (q *Query) Join(table string, onConditions string) *Query {
	q.query = append(q.query, " JOIN "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ON "...)
	q.query = append(q.query, onConditions...)
	return q
}

// LeftJoin builds the query string for the LEFT JOIN clause
func (q *Query) LeftJoin(table string, onConditions string) *Query {
	q.query = append(q.query, " LEFT JOIN "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ON "...)
	q.query = append(q.query, onConditions...)
	return q
}

// RightJoin builds the query string for the RIGHT JOIN clause
func (q *Query) RightJoin(table string, onConditions string) *Query {
	q.query = append(q.query, " RIGHT JOIN "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ON "...)
	q.query = append(q.query, onConditions...)
	return q
}

// FullJoin builds the query string for the FULL JOIN clause
func (q *Query) FullJoin(table string, onConditions string) *Query {
	q.query = append(q.query, " FULL JOIN "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ON "...)
	q.query = append(q.query, onConditions...)
	return q
}

// OrderBy builds the query string for the ORDER BY clause
func (q *Query) OrderBy(exprs ...string) *Query {
	q.query = append(q.query, " ORDER BY "...)
	for i, expr := range exprs {
		if i > 0 {
			q.query = append(q.query, ", "...)
		}
		q.query = append(q.query, expr...)
	}
	return q
}

// GroupBy builds the query string for the GROUP BY clause
func (q *Query) GroupBy(exprs ...string) *Query {
	q.query = append(q.query, " GROUP BY "...)
	for i, expr := range exprs {
		if i > 0 {
			q.query = append(q.query, ", "...)
		}
		q.query = append(q.query, expr...)
	}
	return q
}

// Having builds the query string for the HAVING clause
func (q *Query) Having(expr string, value any) *Query {
	q.query = append(q.query, " HAVING "...)
	q.query = append(q.query, expr...)
	q.args = append(q.args, value)
	return q
}

// Limit builds the query string for the LIMIT clause
func (q *Query) Limit(n int) *Query {
	q.query = append(q.query, " LIMIT ?"...)
	q.args = append(q.args, n)
	return q
}

// Offset builds the query string for the OFFSET clause
func (q *Query) Offset(n int) *Query {
	q.query = append(q.query, " OFFSET ?"...)
	q.args = append(q.args, n)
	return q
}

// Paginate adds a LIMIT and OFFSET clause to the query string to paginate results
func (q *Query) Paginate(page int, pageSize int) *Query {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 1
	}
	if page > 1 {
		q.Offset((page - 1) * pageSize)
	}
	q.Limit(pageSize)
	return q
}

// Returning builds the query string for the RETURNING clause
func (q *Query) Returning(columns ...string) *Query {
	q.query = append(q.query, " RETURNING "...)
	for i, c := range columns {
		if i > 0 {
			q.query = append(q.query, ", "...)
		}
		q.query = append(q.query, c...)
	}
	return q
}

// Union builds the query string for the UNION clause
func (q *Query) Union(other *Query) *Query {
	q.query = append(q.query, " UNION "...)
	q.query = append(q.query, other.query...)
	q.args = append(q.args, other.args...)
	return q
}

// Raw adds a raw query string to the query
func (q *Query) Raw(query string, args ...any) *Query {
	q.query = append(q.query, query...)
	q.args = append(q.args, args...)
	return q
}

// String returns the built query string and resets the query
// This is a convenience method for when you don't need the arguments
func (q *Query) String() string {
	query := string(q.query)
	q.Reset()
	return query
}

// Build returns the built query string and arguments and resets the query
func (q *Query) Build() (string, []any) {
	replacementIndex := 1
	for i := 0; i < len(q.query); i++ {
		if q.query[i] == '?' {
			q.query[i] = '$'
			replacementIndexStr := strconv.Itoa(replacementIndex)
			for j := 0; j < len(replacementIndexStr); j++ {
				q.query = append(q.query, 0)
				copy(q.query[i+j+1:], q.query[i+j:])
				q.query[i+j+1] = replacementIndexStr[j]
			}
			replacementIndex++
		}
	}

	query := string(q.query)
	args := q.args

	q.Reset()
	return query, args
}

// Reset resets the Query struct for reuse
func (q *Query) Reset() {
	q.query = q.query[:0]
	q.args = q.args[:0]
	queryPool.Put(q)
}

var queryPool = sync.Pool{New: func() any {
	return &Query{
		query: make([]byte, 0, 8),
		args:  make([]any, 0, 8),
	}
}}

func getQuery() *Query {
	return queryPool.Get().(*Query)
}
