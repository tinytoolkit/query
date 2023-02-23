package query_test

import (
	"testing"

	"github.com/tinytoolkit/query"
)

func TestCreateSchema(t *testing.T) {
	q := query.Begin().CreateSchema("test", "postgres").Commit().String()
	expected := "BEGIN; CREATE SCHEMA test AUTHORIZATION postgres; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestAlterSchemaName(t *testing.T) {
	q := query.Begin().AlterSchemaName("test", "test2").Commit().String()
	expected := "BEGIN; ALTER SCHEMA test RENAME TO test2; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestAlterSchemaOwner(t *testing.T) {
	q := query.Begin().AlterSchemaOwner("test", "postgres").Commit().String()
	expected := "BEGIN; ALTER SCHEMA test OWNER TO postgres; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestDropSchema(t *testing.T) {
	q := query.Begin().DropSchema("test", true).Commit().String()
	expected := "BEGIN; DROP SCHEMA test CASCADE; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestCreateSchemaWithOwner(t *testing.T) {
	q := query.Begin().CreateSchema("test", "postgres").Commit().String()
	expected := "BEGIN; CREATE SCHEMA test AUTHORIZATION postgres; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestCreateTable(t *testing.T) {
	q := query.Begin().CreateTable("users", nil).Commit().String()
	expected := "BEGIN; CREATE TABLE users (); COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestCreateTableWithColumns(t *testing.T) {
	c := []*query.Column{
		{Name: "id", Type: "serial PRIMARY KEY"},
		{Name: "name", Type: "varchar(255)"},
		{Name: "email", Type: "varchar(255)"},
	}

	q := query.Begin().
		CreateTable("users", c).
		Commit().
		String()
	expected := "BEGIN; CREATE TABLE users (id serial PRIMARY KEY, name varchar(255), email varchar(255)); COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestCommentOnTable(t *testing.T) {
	q := query.Begin().
		CommentOnTable("users", "This is a table for users").
		Commit().
		String()
	expected := `BEGIN; COMMENT ON TABLE users IS 'This is a table for users'; COMMIT;`
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestAlterTableName(t *testing.T) {
	q := query.Begin().AlterTableName("users", "users2").Commit().String()
	expected := "BEGIN; ALTER TABLE users RENAME TO users2; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestCommentOnColumn(t *testing.T) {
	q := query.Begin().
		CommentOnColumn("users", "name", "This is a column for user name").
		Commit().
		String()
	expected := `BEGIN; COMMENT ON COLUMN users.name IS 'This is a column for user name'; COMMIT;`
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestAddColumn(t *testing.T) {
	opt := &query.ColumnOptions{
		DefaultValue:       "0",
		PrimaryKey:         true,
		NotNull:            true,
		Unique:             true,
		Identity:           true,
		IdentityGeneration: "ALWAYS",
		Check:              "age > 0",
	}
	q := query.Begin().AddColumn("users", "age", "integer", opt).Commit().String()
	expected := `BEGIN; ALTER TABLE users ADD COLUMN age integer DEFAULT 0 PRIMARY KEY NOT NULL UNIQUE GENERATED ALWAYS AS IDENTITY CHECK (age > 0); COMMIT;`
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestAlterColumnName(t *testing.T) {
	q := query.Begin().AlterColumnName("users", "name", "fullname").Commit().String()
	expected := "BEGIN; ALTER TABLE users RENAME COLUMN name TO fullname; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestAlterColumnType(t *testing.T) {
	q := query.Begin().AlterColumnType("users", "name", "varchar(255)").Commit().String()
	expected := "BEGIN; ALTER TABLE users ALTER COLUMN name TYPE varchar(255); COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestAlterColumnSetDefault(t *testing.T) {
	q := query.Begin().AlterColumnSetDefault("users", "name", "John Doe").Commit().String()
	expected := `BEGIN; ALTER TABLE users ALTER COLUMN name SET DEFAULT 'John Doe'; COMMIT;`
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestAlterColumnDropDefault(t *testing.T) {
	q := query.Begin().AlterColumnDropDefault("users", "name").Commit().String()
	expected := "BEGIN; ALTER TABLE users ALTER COLUMN name DROP DEFAULT; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestAlterColumnNull(t *testing.T) {
	q := query.Begin().AlterColumnNull("users", "name", true).Commit().String()
	expected := "BEGIN; ALTER TABLE users ALTER COLUMN name SET NOT NULL; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestAlterColumnUsing(t *testing.T) {
	q := query.Begin().AlterColumnUsing("users", "name", "name::varchar(255)").Commit().String()
	expected := "BEGIN; ALTER TABLE users ALTER COLUMN name USING name::varchar(255); COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestAlterColumnAddGeneratedIdentity(t *testing.T) {
	q := query.Begin().AlterColumnAddGeneratedIdentity("users", "name", "ALWAYS").Commit().String()
	expected := "BEGIN; ALTER TABLE users ALTER COLUMN name ADD GENERATED ALWAYS AS IDENTITY; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestAlterColumnSetGenerated(t *testing.T) {
	q := query.Begin().AlterColumnSetGenerated("users", "name", "ALWAYS").Commit().String()
	expected := "BEGIN; ALTER TABLE users ALTER COLUMN name SET GENERATED ALWAYS; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestAlterColumnDropIdentity(t *testing.T) {
	q := query.Begin().AlterColumnDropIdentity("users", "name").Commit().String()
	expected := "BEGIN; ALTER TABLE users ALTER COLUMN name DROP IDENTITY IF EXISTS; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestDropColumn(t *testing.T) {
	q := query.Begin().DropColumn("users", "name", true).Commit().String()
	expected := "BEGIN; ALTER TABLE users DROP COLUMN name CASCADE; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestAlterRowSecurity(t *testing.T) {
	q := query.Begin().AlterRowSecurity("users", true).Commit().String()
	expected := "BEGIN; ALTER TABLE users ENABLE ROW LEVEL SECURITY; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}

	q = query.Begin().AlterRowSecurity("users", false).Commit().String()
	expected = "BEGIN; ALTER TABLE users DISABLE ROW LEVEL SECURITY; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestAlterForceRowSecurity(t *testing.T) {
	q := query.Begin().AlterForceRowSecurity("users", true).Commit().String()
	expected := "BEGIN; ALTER TABLE users FORCE ROW LEVEL SECURITY; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}

	q = query.Begin().AlterForceRowSecurity("users", false).Commit().String()
	expected = "BEGIN; ALTER TABLE users NO FORCE ROW LEVEL SECURITY; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestDropTable(t *testing.T) {
	q := query.Begin().DropTable("users", true).Commit().String()
	expected := "BEGIN; DROP TABLE users CASCADE; COMMIT;"
	if q != expected {
		t.Errorf("Expected %s, got %s", expected, q)
	}
}

func TestInsertInto(t *testing.T) {
	q, v := query.InsertInto("users", "name", "email").
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
	q, v := query.DeleteFrom("users").
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
	fields := []*query.Field{
		{"name", "John"},
		{"email", "john.doe@example.com"},
	}
	q, v := query.Update("users").
		Set(fields).
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
	q, v := query.Select("name", "email").
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
	q, v := query.Select("*").
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
	q, v := query.Select("DISTINCT name").
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
	q, v := query.Select("COUNT(*)").
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
	q, v := query.Select("name", "email").
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
	q, v := query.Select("name", "email").
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
	q, v := query.Select("name", "email").
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
	q, v := query.Select("name", "email").
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
	q, v := query.Select("name", "email").
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
	q, v := query.Select("name", "email").
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
	q, v := query.InsertInto("users", "name", "email").
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
	q, v := query.Select("name", "email").
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
	q, v := query.Select("name", "email").
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
	q, v := query.Select("name", "email").
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
	q, v := query.Select("name", "email").
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
	q, v := query.Select("name", "email").
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
	q, v := query.Select("name", "email").
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
	q, v := query.Select("name", "email").
		From("users").
		Where("id = ?", 1).
		Union(query.Select("name", "email").
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
	q, v := query.With("users", query.Select("name", "email").From("users")).
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
	q, v := query.Select("name").
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

func BenchmarkCreateSchema(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = query.Begin().CreateSchema("test", "postgres").Commit().String()
	}
}

func BenchmarkCreateTable(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c := []*query.Column{
			{Name: "id", Type: "serial PRIMARY KEY"},
			{Name: "name", Type: "varchar(255)"},
			{Name: "email", Type: "varchar(255)"},
		}

		_ = query.
			Begin().
			CreateTable("users", c).
			Commit().
			String()
	}
}

func BenchmarkAddColumnWithOptions(b *testing.B) {
	for i := 0; i < b.N; i++ {
		opt := &query.ColumnOptions{
			PrimaryKey:         true,
			NotNull:            true,
			Unique:             true,
			Identity:           true,
			IdentityGeneration: "ALWAYS",
			Check:              "age > 0",
		}
		_ = query.
			Begin().
			AddColumn("users", "age", "integer", opt).
			Commit().
			String()
	}
}

func BenchmarkInsertInto(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = query.InsertInto("users", "name", "email").
			Values("John", "johndoe@gmail.com").
			Values("Jane", "jane@gmail.com").
			Build()
	}
}

func BenchmarkDeleteFrom(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = query.DeleteFrom("users").
			Where("id = ?", 1).
			Build()
	}
}

func BenchmarkUpdate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fields := []*query.Field{
			{"name", "John"},
			{"email", "john.doe@example.com"},
		}

		_, _ = query.Update("users").
			Set(fields).
			Where("id = ?", 1).
			Build()
	}
}

func BenchmarkSelect(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = query.Select("name", "email").
			From("users").
			Where("id = ?", 1).
			Build()
	}
}

func BenchmarkComplex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = query.Select("name", "email").
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
