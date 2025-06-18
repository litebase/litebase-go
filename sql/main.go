package sql

import (
	"database/sql"
)

func init() {
	sql.Register("litebase", &Driver{})
}
