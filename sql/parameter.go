package sql

import (
	"database/sql/driver"
	"fmt"
)

type Parameter struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
}

func prepareParameters(args []driver.Value) ([]Parameter, error) {
	parameters := make([]Parameter, len(args))

	for i, arg := range args {
		paramType := "TEXT"

		switch v := arg.(type) {
		case int64:
			paramType = "INTEGER"
		case float64:
			paramType = "FLOAT"
		case []byte:
			paramType = "BLOB"
		case string:
			paramType = "TEXT"
		case nil:
			paramType = "NULL"
		default:
			return nil, fmt.Errorf("unsupported parameter type: %T", v)
		}

		parameters[i] = Parameter{
			Type:  paramType,
			Value: arg,
		}
	}
	return parameters, nil
}

func prepareParametersNamed(args []driver.NamedValue) ([]Parameter, error) {
	parameters := make([]Parameter, len(args))

	for i, arg := range args {
		paramType := "TEXT"

		switch v := arg.Value.(type) {
		case int64:
			paramType = "INTEGER"
		case float64:
			paramType = "FLOAT"
		case []byte:
			paramType = "BLOB"
		case string:
			paramType = "TEXT"
		case nil:
			paramType = "NULL"
		default:
			return nil, fmt.Errorf("unsupported parameter type: %T", v)
		}

		parameters[i] = Parameter{
			Type:  paramType,
			Value: arg.Value,
		}
	}

	return parameters, nil
}
