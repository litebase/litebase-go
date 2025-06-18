package sql

import (
	"database/sql/driver"
	"errors"
	"regexp"

	"github.com/google/uuid"
)

type Statement struct {
	closed bool
	pool   *ConnectionPool
	SQL    string
}

func NewStatement(pool *ConnectionPool, sql string) *Statement {
	return &Statement{
		pool: pool,
		SQL:  sql,
	}
}

func (s *Statement) Close() error {
	if s.closed {
		return errors.New("statement is already closed")
	}

	s.closed = true

	return nil
}

func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	connection, err := s.pool.Get()

	if err != nil {
		return nil, err
	}

	defer s.pool.Put(connection)

	parameters, err := prepareParameters(args)

	if err != nil {
		return nil, err
	}

	response, err := connection.Send(Query{
		ID:         uuid.NewString(),
		Statement:  s.SQL,
		Parameters: parameters,
	})

	if err != nil {
		return nil, err
	}

	if len(response.Error) > 0 {
		return nil, errors.New(string(response.Error))
	}

	return NewResult(
		response.Data.Columns,
		int64(response.Data.Changes),
		int64(response.Data.LastInsertRowID),
		response.Data.Rows,
	), nil
}

func (s *Statement) NumInput() int {
	count := 0
	paramRegex := regexp.MustCompile(`[?]\d+|[:@$]\w+`)
	params := paramRegex.FindAllString(s.SQL, -1)
	count += len(params)

	return count
}

func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	connection, err := s.pool.Get()

	if err != nil {
		return nil, err
	}

	defer s.pool.Put(connection)

	parameters, err := prepareParameters(args)

	if err != nil {
		return nil, err
	}

	response, err := connection.Send(Query{
		ID:         uuid.NewString(),
		Statement:  s.SQL,
		Parameters: parameters,
	})

	if err != nil {
		return nil, err
	}

	if len(response.Error) > 0 {
		return nil, errors.New(string(response.Error))
	}

	return NewRows(response.Data.Columns, response.Data.Rows), nil
}
