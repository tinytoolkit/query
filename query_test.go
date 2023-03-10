package query_test

import (
	"testing"

	"github.com/tinytoolkit/query"
)

func TestBegin(t *testing.T) {
	q := query.Begin("").Query()
	expected := "BEGIN TRANSACTION;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.Begin("DEFERRED").Query()
	expected = "BEGIN DEFERRED TRANSACTION;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestCommit(t *testing.T) {
	q := query.Commit().Query()
	expected := "COMMIT TRANSACTION;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestRollback(t *testing.T) {
	q := query.Rollback("").Query()
	expected := "ROLLBACK TRANSACTION;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.Rollback("foo").Query()
	expected = "ROLLBACK TRANSACTION TO SAVEPOINT foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestSavepoint(t *testing.T) {
	q := query.Savepoint("foo").Query()
	expected := "SAVEPOINT foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.ReleaseSavepoint("foo").Query()
	expected = "RELEASE SAVEPOINT foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestDatabase(t *testing.T) {
	q := query.AttachDatabase("foo.db", "foo").Query()
	expected := "ATTACH DATABASE 'foo.db' AS foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.DetachDatabase("foo").Query()
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
	}).Query()
	expected := "CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, age INTEGER NOT NULL CHECK (age > 0) DEFAULT 0, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP);"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestDropTable(t *testing.T) {
	q := query.DropTable("foo").Query()
	expected := "DROP TABLE foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestAlterTable(t *testing.T) {
	q := query.AlterTable("foo").RenameTo("bar").Query()
	expected := "ALTER TABLE foo RENAME TO bar;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.AlterTable("foo").RenameColumn("id", "foo_id").Query()
	expected = "ALTER TABLE foo RENAME COLUMN id TO foo_id;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.AlterTable("foo").AddColumn(query.Column{
		Name: "id", Type: "INTEGER", PrimaryKey: true, AutoIncrement: true,
	}).Query()
	expected = "ALTER TABLE foo ADD COLUMN id INTEGER PRIMARY KEY AUTOINCREMENT;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.AlterTable("foo").DropColumn("id").Query()
	expected = "ALTER TABLE foo DROP COLUMN id;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestCreateIndex(t *testing.T) {
	q := query.CreateIndex("foo", "bar", []string{"name"}, true).Query()
	expected := "CREATE UNIQUE INDEX foo ON bar (name);"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.DropIndex("foo").Query()
	expected = "DROP INDEX foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestCreateView(t *testing.T) {
	q := query.CreateView("foo", "SELECT * FROM bar").Query()
	expected := "CREATE VIEW foo AS SELECT * FROM bar;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.DropView("foo").Query()
	expected = "DROP VIEW foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestCreateTrigger(t *testing.T) {
	q := query.CreateTrigger("foo", "bar", "BEFORE", "INSERT", "BEGIN SELECT 1; END").Query()
	expected := "CREATE TRIGGER foo BEFORE INSERT ON bar BEGIN SELECT 1; END;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.DropTrigger("foo").Query()
	expected = "DROP TRIGGER foo;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestDeleteFrom(t *testing.T) {
	q := query.DeleteFrom("foo").Where("id = ?", 1).Query()
	expected := "DELETE FROM foo WHERE id = ?"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestInsertInto(t *testing.T) {
	q := query.InsertInto("foo").Columns("name", "age").Values("foo", 1).Query()
	expected := "INSERT INTO foo (name, age) VALUES (?, ?)"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestOnConflict(t *testing.T) {
	q := query.InsertInto("foo").Columns("name", "age").Values("foo", 1).OnConflict("name").Query()
	expected := "INSERT INTO foo (name, age) VALUES (?, ?) ON CONFLICT (name)"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.InsertInto("foo").Columns("name", "age").Values("foo", 1).OnConflict("name").Do().Nothing().Query()
	expected = "INSERT INTO foo (name, age) VALUES (?, ?) ON CONFLICT (name) DO NOTHING"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.InsertInto("foo").Columns("name", "age").Values("foo", 1).OnConflict("name").Do().Update("age", "").Set([]*query.Field{
		{Name: "age", Value: 1},
	}).Query()
	expected = "INSERT INTO foo (name, age) VALUES (?, ?) ON CONFLICT (name) DO UPDATE age SET age = ?"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestSelectFrom(t *testing.T) {
	q := query.Select("*").From("foo").Query()
	expected := "SELECT * FROM foo"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.Select("name", "age").From("foo").Query()
	expected = "SELECT name, age FROM foo"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestJoins(t *testing.T) {
	q := query.Select("name", "age").From("foo").Join("bar", "foo.id = bar.id").Query()
	expected := "SELECT name, age FROM foo JOIN bar ON foo.id = bar.id"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.Select("name", "age").From("foo").LeftJoin("bar", "foo.id = bar.id").Query()
	expected = "SELECT name, age FROM foo LEFT JOIN bar ON foo.id = bar.id"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.Select("name", "age").From("foo").RightJoin("bar", "foo.id = bar.id").Query()
	expected = "SELECT name, age FROM foo RIGHT JOIN bar ON foo.id = bar.id"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}

	q = query.Select("name", "age").From("foo").FullJoin("bar", "foo.id = bar.id").Query()
	expected = "SELECT name, age FROM foo FULL JOIN bar ON foo.id = bar.id"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestHaving(t *testing.T) {
	q := query.Select("name", "age").From("foo").Having("age > ?", 1).Query()
	expected := "SELECT name, age FROM foo HAVING age > ?"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestGroupBy(t *testing.T) {
	q := query.Select("name", "age").From("foo").GroupBy("name").Query()
	expected := "SELECT name, age FROM foo GROUP BY name"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestOrderBy(t *testing.T) {
	q := query.Select("name", "age").From("foo").OrderBy("name").Query()
	expected := "SELECT name, age FROM foo ORDER BY name"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestLimit(t *testing.T) {
	q := query.Select("name", "age").From("foo").Limit(1).Query()
	expected := "SELECT name, age FROM foo LIMIT ?"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestOffset(t *testing.T) {
	q := query.Select("name", "age").From("foo").Offset(1).Query()
	expected := "SELECT name, age FROM foo OFFSET ?"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestIndexBy(t *testing.T) {
	q := query.Select("name", "age").From("foo").IndexBy("name").Query()
	expected := "SELECT name, age FROM foo INDEX BY name"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestNotInde(t *testing.T) {
	q := query.Select("name", "age").From("foo").NotIndex().Query()
	expected := "SELECT name, age FROM foo NOT INDEX"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestReindex(t *testing.T) {
	q := query.Select("name", "age").From("foo").Reindex("test").Query()
	expected := "SELECT name, age FROM foo REINDEX test"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestPagination(t *testing.T) {
	q := query.Select("name", "age").From("foo").Paginate(1, 10).Query()
	expected := "SELECT name, age FROM foo LIMIT ?"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestReturning(t *testing.T) {
	q := query.InsertInto("foo").Columns("name", "age").Values("foo", 1).Returning("id").Query()
	expected := "INSERT INTO foo (name, age) VALUES (?, ?) RETURNING id"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestWith(t *testing.T) {
	q := query.With("foo", query.Select("name", "age").From("foo")).Select("*").From("foo").Query()
	expected := "WITH foo AS (SELECT name, age FROM foo) SELECT * FROM foo"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}

func TestVacuum(t *testing.T) {
	q := query.Vacuum("foo", "foo.db").Query()
	expected := "VACUUM foo INTO foo.db;"
	if q != expected {
		t.Errorf("Expected query '%s', but got '%s'", expected, q)
	}
}
