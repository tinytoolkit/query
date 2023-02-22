package query

import (
	"bytes"
	"strconv"
	"sync"
)

// Query is a struct representing a query statement
type Query struct {
	query []byte
	args  []any
}

// CreateSchema builds the query string for a CREATE SCHEMA statement
func CreateSchema(name string, owner string) *Query {
	q := getQuery()
	q.query = append(q.query, "CREATE SCHEMA "...)
	q.query = append(q.query, name...)
	if owner != "" {
		q.query = append(q.query, " AUTHORIZATION "...)
		q.query = append(q.query, owner...)
	}
	q.query = append(q.query, ';')
	return q
}

// CreateTable is a function to start building a CREATE TABLE query statement
func CreateTable(table string, columns ...string) *Query {
	q := getQuery()
	q.query = append(q.query, "CREATE TABLE "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, ' ', '(')
	for i, c := range columns {
		if i > 0 {
			q.query = append(q.query, ',', ' ')
		}
		q.query = append(q.query, c...)
	}
	q.query = append(q.query, ')', ';')
	return q
}

// CommentOnTable builds the query string for a COMMENT ON TABLE statement
func CommentOnTable(table string, comment string) *Query {
	q := getQuery()
	q.query = append(q.query, "COMMENT ON TABLE "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " IS "...)
	q.query = append(q.query, '"')
	q.query = append(q.query, comment...)
	q.query = append(q.query, '"', ';')
	return q
}

// InsertInto is a function to start building an INSERT INTO query statement
func InsertInto(table string, fields ...string) *Query {
	return getQuery().InsertInto(table, fields...)
}

// DeleteFrom is a function to start building a DELETE FROM query statement
func DeleteFrom(table string) *Query {
	return getQuery().DeleteFrom(table)
}

// Update is a function to start building an UPDATE query statement
func Update(table string) *Query {
	return getQuery().Update(table)
}

// Select is a function to start building a SELECT query statement
func Select(exprs ...string) *Query {
	return getQuery().Select(exprs...)
}

// With is a function to start building a WITH query statement
func With(name string, query *Query) *Query {
	return getQuery().With(name, query)
}

// InsertInto builds the query string for an INSERT INTO statement
func (q *Query) InsertInto(table string, fields ...string) *Query {
	q.query = append(q.query, "INSERT INTO "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, ' ', '(')
	for i, f := range fields {
		if i > 0 {
			q.query = append(q.query, ',', ' ')
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
	q.query = append(q.query, ' ', '(')
	for i := range values {
		if i > 0 {
			q.query = append(q.query, ',', ' ')
		}
		q.query = append(q.query, '?')
	}
	q.query = append(q.query, ')')
	q.args = append(q.args, values...)
	return q
}

// DeleteFrom is a method for the Query struct and builds the query string for a DELETE statement
func (q *Query) DeleteFrom(table string) *Query {
	q.query = append(q.query, "DELETE FROM "...)
	q.query = append(q.query, table...)
	return q
}

// Update builds the query string for an UPDATE statement
func (q *Query) Update(table string) *Query {
	q.query = append(q.query, "UPDATE "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " SET "...)
	return q
}

// Set builds the query string for the SET clause in an UPDATE statement
func (q *Query) Set(fields map[string]any) *Query {
	var i int
	for field, value := range fields {
		q.query = append(q.query, field...)
		q.query = append(q.query, " = "...)
		q.query = append(q.query, '?')
		if i < len(fields)-1 {
			q.query = append(q.query, ',', ' ')
		}
		q.args = append(q.args, value)
		i++
	}
	return q
}

// Select builds the query string for the SELECT clause
func (q *Query) Select(exprs ...string) *Query {
	q.query = append(q.query, "SELECT "...)
	for i, expr := range exprs {
		if i > 0 {
			q.query = append(q.query, ',', ' ')
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
				q.query = append(q.query, ',', ' ')
			}
			q.args = append(q.args, value)
		}
	case []string:
		for idx, value := range v {
			q.query = append(q.query, '?')
			if len(v) > 0 && idx < len(v)-1 {
				q.query = append(q.query, ',', ' ')
			}
			q.args = append(q.args, value)
		}
	default:
		for idx, value := range v.([]any) {
			q.query = append(q.query, '?')
			if len(v.([]any)) > 0 && idx < len(v.([]any))-1 {
				q.query = append(q.query, ',', ' ')
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
			q.query = append(q.query, ',', ' ')
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
			q.query = append(q.query, ',', ' ')
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
func (q *Query) Returning(fields ...string) *Query {
	q.query = append(q.query, " RETURNING "...)
	for i, f := range fields {
		if i > 0 {
			q.query = append(q.query, ',', ' ')
		}
		q.query = append(q.query, f...)
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

// With builds the query string for the WITH clause
func (q *Query) With(name string, query *Query) *Query {
	q.query = append(q.query, "WITH "...)
	q.query = append(q.query, name...)
	q.query = append(q.query, " AS ("...)
	q.query = append(q.query, query.query...)
	q.query = append(q.query, ')', ' ')
	q.args = append(q.args, query.args...)
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
