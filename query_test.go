package query_test

import (
	"testing"

	"github.com/tinytoolkit/query"
)

func TestBegin(t *testing.T) {
	q := query.Begin("").String()
	expected := "BEGIN TRANSACTION;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.Begin("DEFERRED").String()
	expected = "BEGIN DEFERRED TRANSACTION;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestCommit(t *testing.T) {
	q := query.Commit().String()
	expected := "COMMIT TRANSACTION;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestRollback(t *testing.T) {
	q := query.Rollback("").String()
	expected := "ROLLBACK TRANSACTION;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.Rollback("foo").String()
	expected = "ROLLBACK TRANSACTION TO SAVEPOINT foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestSavepoint(t *testing.T) {
	q := query.Savepoint("foo").String()
	expected := "SAVEPOINT foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.ReleaseSavepoint("foo").String()
	expected = "RELEASE SAVEPOINT foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestDatabase(t *testing.T) {
	q := query.AttachDatabase("foo.db", "foo").String()
	expected := "ATTACH DATABASE 'foo.db' AS foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.DetachDatabase("foo").String()
	expected = "DETACH DATABASE foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestCreateTable(t *testing.T) {
	q := query.CreateTable("foo", []query.Column{
		{Name: "id", Type: "INTEGER", PrimaryKey: true, AutoIncrement: true},
		{Name: "name", Type: "TEXT", NotNull: true, Unique: true},
		{Name: "age", Type: "INTEGER", NotNull: true, Default: "0", Check: "age > 0"},
		{Name: "created_at", Type: "DATETIME", NotNull: true, Default: "CURRENT_TIMESTAMP"},
	}).String()
	expected := "CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, age INTEGER NOT NULL CHECK (age > 0) DEFAULT 0, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP);"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestDropTable(t *testing.T) {
	q := query.DropTable("foo").String()
	expected := "DROP TABLE foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestAlterTable(t *testing.T) {
	q := query.AlterTable("foo").RenameTo("bar").String()
	expected := "ALTER TABLE foo RENAME TO bar;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.AlterTable("foo").RenameColumn("id", "foo_id").String()
	expected = "ALTER TABLE foo RENAME COLUMN id TO foo_id;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.AlterTable("foo").AddColumn(query.Column{
		Name: "id", Type: "INTEGER", PrimaryKey: true, AutoIncrement: true,
	}).String()
	expected = "ALTER TABLE foo ADD COLUMN id INTEGER PRIMARY KEY AUTOINCREMENT;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.AlterTable("foo").DropColumn("id").String()
	expected = "ALTER TABLE foo DROP COLUMN id;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestCreateIndex(t *testing.T) {
	q := query.CreateIndex("foo", "bar", []string{"name"}, true).String()
	expected := "CREATE UNIQUE INDEX foo ON bar (name);"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.DropIndex("foo").String()
	expected = "DROP INDEX foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestCreateView(t *testing.T) {
	q := query.CreateView("foo", "SELECT * FROM bar").String()
	expected := "CREATE VIEW foo AS SELECT * FROM bar;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.DropView("foo").String()
	expected = "DROP VIEW foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestCreateTrigger(t *testing.T) {
	q := query.CreateTrigger("foo", "bar", "BEFORE", "INSERT", "BEGIN SELECT 1; END").String()
	expected := "CREATE TRIGGER foo BEFORE INSERT ON bar BEGIN SELECT 1; END;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.DropTrigger("foo").String()
	expected = "DROP TRIGGER foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestDeleteFrom(t *testing.T) {
	q := query.DeleteFrom("foo").Where("id = ?").Args(1).String()

	expected := "DELETE FROM foo WHERE id = ?"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestInsertInto(t *testing.T) {
	q := query.InsertInto("foo").Columns("name", "age").Values("foo", 1).String()
	expected := "INSERT INTO foo (name, age) VALUES (?, ?)"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestOnConflict(t *testing.T) {
	q := query.InsertInto("foo").Columns("name", "age").Values("foo", 1).OnConflict("name").String()
	expected := "INSERT INTO foo (name, age) VALUES (?, ?) ON CONFLICT (name)"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.InsertInto("foo").Columns("name", "age").Values("foo", 1).OnConflict("name").Do().Nothing().String()
	expected = "INSERT INTO foo (name, age) VALUES (?, ?) ON CONFLICT (name) DO NOTHING"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.InsertInto("foo").Columns("name", "age").Values("foo", 1).OnConflict("name").Do().Update("age", "").Set([]*query.Field{
		{Name: "age", Value: 1},
	}).String()
	expected = "INSERT INTO foo (name, age) VALUES (?, ?) ON CONFLICT (name) DO UPDATE age SET age = ?"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestSelectFrom(t *testing.T) {
	q := query.Select("*").From("foo").String()
	expected := "SELECT * FROM foo"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.Select("name", "age").From("foo").String()
	expected = "SELECT name, age FROM foo"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestJoins(t *testing.T) {
	q := query.Select("name", "age").From("foo").Join("bar", "foo.id = bar.id").String()
	expected := "SELECT name, age FROM foo JOIN bar ON foo.id = bar.id"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.Select("name", "age").From("foo").LeftJoin("bar", "foo.id = bar.id").String()
	expected = "SELECT name, age FROM foo LEFT JOIN bar ON foo.id = bar.id"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.Select("name", "age").From("foo").RightJoin("bar", "foo.id = bar.id").String()
	expected = "SELECT name, age FROM foo RIGHT JOIN bar ON foo.id = bar.id"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.Select("name", "age").From("foo").FullJoin("bar", "foo.id = bar.id").String()
	expected = "SELECT name, age FROM foo FULL JOIN bar ON foo.id = bar.id"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestHaving(t *testing.T) {
	q := query.Select("name", "age").From("foo").Having("age > ?").Args(1).String()
	expected := "SELECT name, age FROM foo HAVING age > ?"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestGroupBy(t *testing.T) {
	q := query.Select("name", "age").From("foo").GroupBy("name").String()
	expected := "SELECT name, age FROM foo GROUP BY name"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestOrderBy(t *testing.T) {
	q := query.Select("name", "age").From("foo").OrderBy("name").String()
	expected := "SELECT name, age FROM foo ORDER BY name"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestLimit(t *testing.T) {
	q := query.Select("name", "age").From("foo").Limit(1).String()
	expected := "SELECT name, age FROM foo LIMIT ?"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestOffset(t *testing.T) {
	q := query.Select("name", "age").From("foo").Offset(1).String()
	expected := "SELECT name, age FROM foo OFFSET ?"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestIndexBy(t *testing.T) {
	q := query.Select("name", "age").From("foo").IndexBy("name").String()
	expected := "SELECT name, age FROM foo INDEX BY name"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestNotInde(t *testing.T) {
	q := query.Select("name", "age").From("foo").NotIndex().String()
	expected := "SELECT name, age FROM foo NOT INDEX"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestReindex(t *testing.T) {
	q := query.Select("name", "age").From("foo").Reindex("test").String()
	expected := "SELECT name, age FROM foo REINDEX test"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestPagination(t *testing.T) {
	q := query.Select("name", "age").From("foo").Paginate(1, 10).String()
	expected := "SELECT name, age FROM foo LIMIT ?"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestReturning(t *testing.T) {
	q := query.InsertInto("foo").Columns("name", "age").Values("foo", 1).Returning("id").String()
	expected := "INSERT INTO foo (name, age) VALUES (?, ?) RETURNING id"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestWith(t *testing.T) {
	q := query.With("foo", query.Select("name", "age").From("foo")).Select("*").From("foo").String()
	expected := "WITH foo AS (SELECT name, age FROM foo) SELECT * FROM foo"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestVacuum(t *testing.T) {
	q := query.Vacuum("foo", "foo.db").String()
	expected := "VACUUM foo INTO foo.db;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}
