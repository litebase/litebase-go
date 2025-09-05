package sql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
)

type Conn struct {
	url         string
	pool        *ConnectionPool
	transaction *Transaction
}

func NewConn(url string, pool *ConnectionPool) *Conn {
	return &Conn{
		pool: pool,
		url:  url,
	}
}

func (c *Conn) Begin() (driver.Tx, error) {
	// Get a connection
	connection, err := c.pool.Get()

	if err != nil {
		return nil, err
	}

	c.transaction = NewTransaction(
		uuid.NewString(),
		c.pool,
		connection,
	)

	return c.transaction, nil
}

func (c *Conn) Close() error {
	// Implement connection close logic
	if c.transaction != nil {
		c.transaction.Rollback()
		c.pool.Put(c.transaction.connection)
	}

	return nil
}

func (c *Conn) ExecContext(ctx context.Context, sql string, args []driver.NamedValue) (driver.Result, error) {
	// TODO: Support transaction id
	connection, err := c.pool.Get()

	if err != nil {
		return nil, err
	}

	defer c.pool.Put(connection)

	parameters, err := prepareParametersNamed(args)

	if err != nil {
		return nil, err
	}

	response, err := connection.Send(Query{
		ID:         uuid.NewString(),
		Statement:  sql,
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

// Send a ping message to the database server and wait for a response
func (c *Conn) Ping(ctx context.Context) error {
	url, err := url.Parse(c.url)

	if err != nil {
		log.Fatalln(err)
		return err
	}

	host := url.Hostname()

	if url.Port() != "" {
		host = fmt.Sprintf("%s:%s", host, url.Port())
	}

	token := SignRequest(
		c.pool.accessKeyId,
		c.pool.accessKeySecret,
		"POST",
		"/query/stream",
		map[string]string{
			"Content-Length":  "0",
			"Content-Type":    "application/octet-stream",
			"Host":            host,
			"X-Litebase-Date": fmt.Sprintf("%d", time.Now().Unix()),
		},
		nil,
		map[string]string{},
	)

	httpClient := &http.Client{
		Timeout: 0,
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("%s/query/stream", c.url), nil)

	if err != nil {
		log.Fatalln(err)

		return err
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Litebase-Date", fmt.Sprintf("%d", time.Now().Unix()))
	req.Header.Set("Authorization", fmt.Sprintf("Litebase-HMAC-SHA256 %s", token))

	resp, err := httpClient.Do(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping failed: %s", resp.Status)
	}

	return nil
}

func (c *Conn) Prepare(sql string) (driver.Stmt, error) {
	return NewStatement(c.pool, sql), nil
}
