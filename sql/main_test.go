package sql_test

import (
	"database/sql"
	"testing"
)

func TestMain(t *testing.T) {
	_, err := sql.Open("litebase", "accessKeyId=your_access_key_id accessKeySecret=your_access_key_secret url=http://localhost:8080")

	if err != nil {
		t.Fatal(err)
	}
}
