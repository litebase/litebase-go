package sql

type Transaction struct {
	connection *Connection
	id         string
	pool       *ConnectionPool
}

func NewTransaction(id string, pool *ConnectionPool, connection *Connection) *Transaction {
	return &Transaction{
		connection: connection,
		id:         id,
		pool:       pool,
	}
}

func (t *Transaction) Commit() error {
	// Implement commit logic
	t.pool.Put(t.connection)

	return nil
}

func (t *Transaction) Rollback() error {
	t.pool.Put(t.connection)

	return nil
}
