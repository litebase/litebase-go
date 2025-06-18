package sql_test

import (
	"database/sql"
	"testing"

	litebaseSql "github.com/litebase/litebase-go/sql"
)

func TestDriver(t *testing.T) {
	db, err := sql.Open("litebase", "access_key_id=test access_key_secret=test url=http://localhost:8080")

	if err != nil {
		t.Fatal(err)
	}

	if db == nil {
		t.Fatal("Expected db to be non-nil")
	}

	if db.Driver() == nil {
		t.Fatal("Expected db.Driver() to be non-nil")
	}

	if _, ok := db.Driver().(*litebaseSql.Driver); !ok {
		t.Fatal("Expected db.Driver() to be of type *Driver")
	}

	// Fails without an access key
	_, err = sql.Open("litebase", "access=key_id= access_key_secret=test url=http://localhost:8080")

	if err == nil {
		t.Fatal("Expected error but got nil")
	}

	// Fails without a secret key
	_, err = sql.Open("litebase", "access_key_id=test access_key_secret= url=http://localhost:8080")

	if err == nil {
		t.Fatal("Expected error but got nil")
	}

	// Fails without a URL
	_, err = sql.Open("litebase", "access_key_id=test access_key_secret=test url=")

	if err == nil {
		t.Fatal("Expected error but got nil")
	}
}

func TestDriverExec(t *testing.T) {
	db, err := sql.Open("litebase", "access_key_id=test access_key_secret=test url=http://localhost:8080")

	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO test (id, name) VALUES (?, ?)", 1, "test")

	if err != nil {
		t.Fatal(err)
	}
}
