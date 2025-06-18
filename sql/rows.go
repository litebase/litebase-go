package sql

import (
	"database/sql/driver"
	"errors"
)

type Rows struct {
	columns []string
	index   int
	rows    [][]Column
}

func NewRows(columnData [][]byte, rows [][]Column) *Rows {
	columns := make([]string, len(columnData))

	for i, column := range columnData {
		columns[i] = string(column)
	}

	return &Rows{
		columns: columns,
		index:   -1,
		rows:    rows,
	}
}

func (r *Rows) Columns() []string {
	return r.columns
}

func (r *Rows) Close() error {

	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows)-1 {
		return errors.New("no more rows")
	}

	r.index++

	for i, column := range r.rows[r.index] {
		dest[i] = column.Value
	}

	return nil
}
