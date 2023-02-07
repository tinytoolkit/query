package query

import "testing"

func TestInsertInto(t *testing.T) {
	q, v := InsertInto("users", "name", "email").
		Values("John", "johndoe@gmail.com").
		Values("Jane", "jane@gmail.com").
		Build()
	expected := "INSERT INTO users (name, email) VALUES ($1, $2) ($3, $4)"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 4 {
		t.Errorf("Expected 4 values, got %d", len(v))
	}
}

func TestDeleteFrom(t *testing.T) {
	q, v := DeleteFrom("users").
		Where("id = ?", 1).
		Build()
	expected := "DELETE FROM users WHERE id = $1"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 1 {
		t.Errorf("Expected 1 value, got %d", len(v))
	}
}

func TestUpdate(t *testing.T) {
	q, v := Update("users").
		Set(map[string]any{
			"name":  "John",
			"email": "john.doe@example.com",
		}).
		Where("id = ?", 1).
		Build()
	expected := "UPDATE users SET name = $1, email = $2 WHERE id = $3"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 3 {
		t.Errorf("Expected 3 values, got %d", len(v))
	}
}

func TestSelect(t *testing.T) {
	q, v := Select("name", "email").
		From("users").
		Where("id = ?", 1).
		Build()
	expected := "SELECT name, email FROM users WHERE id = $1"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 1 {
		t.Errorf("Expected 1 value, got %d", len(v))
	}
}

func TestSelectAll(t *testing.T) {
	q, v := Select("*").
		From("users").
		Where("id = ?", 1).
		And().
		Where("name = ?", "John").
		Or().
		Where("age > ?", 18).
		Build()
	expected := "SELECT * FROM users WHERE id = $1 AND name = $2 OR age > $3"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 3 {
		t.Errorf("Expected 2 value, got %d", len(v))
	}
}

func TestSelectDistinct(t *testing.T) {
	q, v := Select("DISTINCT name").
		From("users").
		Where("id = ?", 1).
		Build()
	expected := "SELECT DISTINCT name FROM users WHERE id = $1"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 1 {
		t.Errorf("Expected 1 value, got %d", len(v))
	}
}

func TestSelectCount(t *testing.T) {
	q, v := Select("COUNT(*)").
		From("users").
		Where("id = ?", 1).
		Build()
	expected := "SELECT COUNT(*) FROM users WHERE id = $1"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 1 {
		t.Errorf("Expected 1 value, got %d", len(v))
	}
}

func TestAndOrIn(t *testing.T) {
	q, v := Select("name", "email").
		From("users").
		Where("id = ?", 1).
		And().
		Where("name = ?", "John").
		Or().
		In("id", []int{1, 2, 3}).
		And().
		In("test", []any{true, "testing"}).
		Build()
	expected := "SELECT name, email FROM users WHERE id = $1 AND name = $2 OR id IN ($3, $4, $5) AND test IN ($6, $7)"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 7 {
		t.Errorf("Expected 7 values, got %d", len(v))
	}
}

func TestIn(t *testing.T) {
	q, v := Select("name", "email").
		From("users").
		In("id", []int{1, 2, 3}).
		And().
		In("email", []string{"test@test.com", "test2@test.com"}).
		Build()
	expected := "SELECT name, email FROM users WHERE id IN ($1, $2, $3) AND email IN ($4, $5)"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 5 {
		t.Errorf("Expected 5 values, got %d", len(v))
	}
}

func TestOrderBy(t *testing.T) {
	q, v := Select("name", "email").
		From("users").
		Where("id = ?", 1).
		OrderBy("name ASC", "email DESC", "age NULLS FIRST", "created_at NULLS LAST").
		Build()
	expected := "SELECT name, email FROM users WHERE id = $1 ORDER BY name ASC, email DESC, age NULLS FIRST, created_at NULLS LAST"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 1 {
		t.Errorf("Expected 1 value, got %d", len(v))
	}
}

func TestOffsetAndLimit(t *testing.T) {
	q, v := Select("name", "email").
		From("users").
		Where("id = ?", 1).
		Offset(10).
		Limit(5).
		Build()
	expected := "SELECT name, email FROM users WHERE id = $1 OFFSET $2 LIMIT $3"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 3 {
		t.Errorf("Expected 3 value, got %d", len(v))
	}
}

func TestPaginate(t *testing.T) {
	q, v := Select("name", "email").
		From("users").
		Where("id = ?", 1).
		Paginate(2, 5).
		Build()
	expected := "SELECT name, email FROM users WHERE id = $1 OFFSET $2 LIMIT $3"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 3 {
		t.Errorf("Expected 3 value, got %d", len(v))
	}

}

func TestPaginateDefaults(t *testing.T) {
	q, v := Select("name", "email").
		From("users").
		Where("id = ?", 1).
		Paginate(0, 0).
		Build()
	expected := "SELECT name, email FROM users WHERE id = $1 LIMIT $2"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 2 {
		t.Errorf("Expected 2 value, got %d", len(v))
	}
}

func TestReturning(t *testing.T) {
	q, v := InsertInto("users", "name", "email").
		Values("John", "johndoe@gmail.com").
		Returning("id", "name").
		Build()
	expected := "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 2 {
		t.Errorf("Expected 2 values, got %d", len(v))
	}
}

func TestGroupBy(t *testing.T) {
	q, v := Select("name", "email").
		From("users").
		Where("id = ?", 1).
		GroupBy("name", "email").
		Build()
	expected := "SELECT name, email FROM users WHERE id = $1 GROUP BY name, email"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 1 {
		t.Errorf("Expected 1 value, got %d", len(v))
	}
}

func TestHaving(t *testing.T) {
	q, v := Select("name", "email").
		From("users").
		Where("id = ?", 1).
		GroupBy("name", "email").
		Having("name = ?", "John").
		Build()
	expected := "SELECT name, email FROM users WHERE id = $1 GROUP BY name, email HAVING name = $2"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 2 {
		t.Errorf("Expected 2 value, got %d", len(v))
	}
}

func TestJoin(t *testing.T) {
	q, v := Select("name", "email").
		From("users").
		Join("posts", "users.id = posts.user_id").
		Where("users.id = ?", 1).
		Build()
	expected := "SELECT name, email FROM users JOIN posts ON users.id = posts.user_id WHERE users.id = $1"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 1 {
		t.Errorf("Expected 1 value, got %d", len(v))
	}
}

func TestLeftJoin(t *testing.T) {
	q, v := Select("name", "email").
		From("users").
		LeftJoin("posts", "users.id = posts.user_id").
		Where("users.id = ?", 1).
		Build()
	expected := "SELECT name, email FROM users LEFT JOIN posts ON users.id = posts.user_id WHERE users.id = $1"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 1 {
		t.Errorf("Expected 1 value, got %d", len(v))
	}
}

func TestRightJoin(t *testing.T) {
	q, v := Select("name", "email").
		From("users").
		RightJoin("posts", "users.id = posts.user_id").
		Where("users.id = ?", 1).
		Build()
	expected := "SELECT name, email FROM users RIGHT JOIN posts ON users.id = posts.user_id WHERE users.id = $1"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 1 {
		t.Errorf("Expected 1 value, got %d", len(v))
	}
}

func TestFullJoin(t *testing.T) {
	q, v := Select("name", "email").
		From("users").
		FullJoin("posts", "users.id = posts.user_id").
		Where("users.id = ?", 1).
		Build()
	expected := "SELECT name, email FROM users FULL JOIN posts ON users.id = posts.user_id WHERE users.id = $1"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 1 {
		t.Errorf("Expected 1 value, got %d", len(v))
	}
}

func TestUnion(t *testing.T) {
	q, v := Select("name", "email").
		From("users").
		Where("id = ?", 1).
		Union(Select("name", "email").
			From("users").
			Where("id = ?", 1)).
		Build()
	expected := "SELECT name, email FROM users WHERE id = $1 UNION SELECT name, email FROM users WHERE id = $2"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 2 {
		t.Errorf("Expected 2 values, got %d", len(v))
	}
}

func TestWith(t *testing.T) {
	q, v := With("users", Select("name", "email").From("users")).
		Select("name", "email").
		From("users").
		Where("id = ?", 1).
		Build()
	expected := "WITH users AS (SELECT name, email FROM users) SELECT name, email FROM users WHERE id = $1"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 1 {
		t.Errorf("Expected 1 value, got %d", len(v))
	}
}

func TestRaw(t *testing.T) {
	q, v := Select("name").
		From("users").
		Where("id = ?", 1).
		Raw(" AND name = ?", "John").
		Build()
	expected := "SELECT name FROM users WHERE id = $1 AND name = $2"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
	if len(v) != 2 {
		t.Errorf("Expected 2 value, got %d", len(v))
	}
}

func BenchmarkInsertInto(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = InsertInto("users", "name", "email").
			Values("John", "johndoe@gmail.com").
			Values("Jane", "jane@gmail.com").
			Build()
	}
}

func BenchmarkDeleteFrom(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = DeleteFrom("users").
			Where("id = ?", 1).
			Build()
	}
}

func BenchmarkUpdate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = Update("users").
			Set(map[string]any{
				"name":  "John",
				"email": "john.doe@example.com",
			}).
			Where("id = ?", 1).
			Build()
	}
}

func BenchmarkSelect(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = Select("name", "email").
			From("users").
			Where("id = ?", 1).
			Build()
	}
}

func BenchmarkComplex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = Select("name", "email").
			From("users").
			Where("id = ?", 1).
			Where("name = ?", "John").
			Or().
			In("id", []int{1, 2, 3}).
			Or().
			In("id", []int{1, 2, 3}).
			Build()
	}
}
