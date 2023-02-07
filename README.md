# tinytoolkit/query

A simple PostgreSQL query builder library for Go.

## Installation

```bash
go get github.com/tinytoolkit/query
```

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
	Set(map[string]any{
		"name":  "John",
		"email": "john.doe@example.com",
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
