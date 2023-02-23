# tinytoolkit/query

A simple PostgreSQL query builder library for Go.

## Installation

```bash
go get github.com/tinytoolkit/query
```

## Supported Queries

- `InsertInto(table string, fields ...string)` 
- `Values(values ...any)`
- `DeleteFrom(table string)`
- `Update(table string)`
- `Set(fields []*Field)`
- `Select(exprs ...string)`
- `From(table string)`
- `Where(expr string, value any)`
- `And()`
- `Or()`
- `In(column string, values any)`
- `Join(table string, onConditions string)`
- `LeftJoin(table string, onConditions string)`
- `RightJoin(table string, onConditions string)`
- `FullJoin(table string, onConditions string)`
- `OrderBy(expr ...string)`
- `GroupBy(exprs ...string)`
- `Having(expr string, value any)`
- `Limit(limit int)`
- `Offset(offset int)`
- `Paginate(page int, pageSize int)`
- `Returning(columns ...string)`
- `Union(other *Query)`
- `With(name string, query *Query)`
- `Begin()`
- `Commit()`
- `CreateSchema(name string, owner string)`
- `AlterSchemaName(oldName string, newName string)`
- `AlterSchemaOwner(name string, owner string)`
- `DropSchema(name string, cascade bool)`
- `CreateTable(table string, columns ...string)`
- `CommentOnTable(table string, comment string)`
- `AlterTableName(oldName string, newName string)`
- `AlterRowSecurity(table string, enable bool)`
- `AlterForceRowSecurity(table string, force bool)`
- `DropTable(table string, cascade bool)`
- `CommentOnColumn(table string, column string, comment string)`
- `AddColumn(table string, name string, dataType string, opts *ColumnOptions)`
- `AlterColumnName(table string, oldName string, newName string)`
- `AlterColumnType(table string, column string, dataType string)`
- `AlterColumnSetDefault(table string, column string, defaultValue string)`
- `AlterColumnDropDefault(table string, column string)`\
- `AlterColumnNull(table string, column string, nullable bool)`
- `AlterColumnUsing(table string, column string, dataType string)`
- `DropColumn(table string, column string)`
- `Raw(query string, args ...any)`

### Usage

```go
// Build returns both the query string and the arguments to be passed to the database driver.
// Use ? as placeholders for arguments. They will automatically be replaced with $1, $2, etc.

// INSERT INTO users (name, email) VALUES ($1, $2)
InsertInto("users", "name", "email").
    Values("Jane", "jane@gmail.com").
    Build()

// INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name
InsertInto("users", "name", "email").
	Values("John", "johndoe@gmail.com").
	Returning("id", "name").
	Build()

// DELETE FROM users WHERE id = $1
DeleteFrom("users").
    Where("id = ?", 1).
    Build()

// UPDATE users SET name = $1, email = $2 WHERE id = $3
Update("users").
	Set([]*query.Field{
		{"name", "John"},
		{"email", "john.doe@example.com"},
	}).
	Where("id = ?", 1).
    Build()

// SELECT name, email FROM users WHERE id = $1
Select("name", "email").
	From("users").
	Where("id = ?", 1).
	Build()

// SELECT * FROM users WHERE id = $1 AND name = $2 OR age > $3
Select("*").
	From("users").
	Where("id = ?", 1).
	And().
	Where("name = ?", "John").
	Or().
	Where("age > ?", 18).
	Build()

// SELECT name, email FROM users WHERE id IN ($1, $2, $3)
Select("name", "email").
	From("users").
	In("id", []int{1, 2, 3}).
	Build()

// SELECT name, email FROM users WHERE id = $1 ORDER BY age DESC
Select("name", "email").
	From("users").
	Where("id = ?", 1).
	OrderBy("age DESC").
	Build()

// SELECT name, email FROM users JOIN posts ON users.id = posts.user_id WHERE users.id = $1
Select("name", "email").
	From("users").
	Join("posts", "users.id = posts.user_id").
	Where("users.id = ?", 1).
	Build()

// SELECT name, email FROM users WHERE id = $1 OFFSET $2 LIMIT $3
Select("name", "email").
	From("users").
	Where("id = ?", 1).
	Offset(10).
	Limit(5).
	Build()
```
