package sql

import (
	"database/sql/driver"
	"errors"
	"strings"
)

type Driver struct{}

// No-op: driver creates connections using the OpenConnector method.
func (d *Driver) Open(name string) (driver.Conn, error) {
	return nil, nil
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	// Parse the connection string
	args := make(map[string]string)

	var accessKeyId, accessKeySecret, url string

	for _, pair := range strings.Split(name, " ") {
		kv := strings.Split(pair, "=")

		if len(kv) == 2 {
			args[kv[0]] = kv[1]
		}
	}

	// Validate required fields
	if args["accessKeyId"] == "" {
		return nil, errors.New("accessKeyId is required")
	}

	if args["accessKeySecret"] == "" {
		return nil, errors.New("accessKeySecret is required")
	}

	if args["url"] == "" {
		return nil, errors.New("url is required")
	}

	accessKeyId = args["accessKeyId"]
	accessKeySecret = args["accessKeySecret"]
	url = args["url"]

	return &Connector{
		driver: d,
		pool: NewConnectionPool(
			accessKeyId,
			accessKeySecret,
			url,
			10,
		),
	}, nil
}
