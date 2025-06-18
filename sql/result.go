package sql

type Result struct {
	Columns      [][]byte
	changes      int64
	lastInsertId int64
	Rows         [][]Column
}

func NewResult(columns [][]byte, changes, lastInsertId int64, rows [][]Column) *Result {
	return &Result{
		Columns:      columns,
		changes:      changes,
		lastInsertId: lastInsertId,
		Rows:         rows,
	}
}

func (r *Result) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r *Result) RowsAffected() (int64, error) {
	return r.changes, nil
}
