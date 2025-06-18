package sql_test

import (
	"database/sql"
	"testing"
)

func TestMain(t *testing.T) {
	_, err := sql.Open("litebase", "access_key_id=your_access_key_id access_key_secret=your_access_key_secret url=http://localhost:8080")

	if err != nil {
		t.Fatal(err)
	}
}
