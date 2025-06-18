package sql

import (
	"context"
	"database/sql/driver"
)

type Connector struct {
	driver driver.Driver
	pool   *ConnectionPool
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	return NewConn(
		c.pool.url,
		c.pool,
	), nil
}

func (c *Connector) Driver() driver.Driver {
	return c.driver
}
