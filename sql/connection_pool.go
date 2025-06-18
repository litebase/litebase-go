package sql

import (
	"errors"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

type ConnectionPool struct {
	accessKeyId       string
	accessKeySecret   string
	activeConnections int
	connections       []*ConnectionPoolItem
	maxConnections    int
	mutex             sync.Mutex
	url               string
}

type ConnectionPoolItem struct {
	connection *Connection
	semaphore  *semaphore.Weighted
}

func NewConnectionPool(
	accessKeyID, accessKeySecret, url string,
	maxConnections int,
) *ConnectionPool {
	pool := &ConnectionPool{
		accessKeyId:       accessKeyID,
		accessKeySecret:   accessKeySecret,
		activeConnections: 0,
		connections:       []*ConnectionPoolItem{},
		maxConnections:    maxConnections,
		mutex:             sync.Mutex{},
		url:               url,
	}

	return pool
}

func (p *ConnectionPool) Close() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, item := range p.connections {
		item.connection.Close()
	}

	p.connections = []*ConnectionPoolItem{}
	p.activeConnections = 0
}

// Find an available connection from the pool or create a new one if the pool
// has an empty slot. If the pool is full, the function will block until a
// connection is available.
func (p *ConnectionPool) Get() (*Connection, error) {
	tries := 0

	for {
		p.mutex.Lock()

		if tries > 10 {
			break
		}

		for _, item := range p.connections {
			if item.semaphore.TryAcquire(1) {
				p.mutex.Unlock()
				return item.connection, nil
			}
		}

		if p.activeConnections < p.maxConnections {
			connection := NewConnection(p.url, p.accessKeyId, p.accessKeySecret)

			p.activeConnections++

			item := &ConnectionPoolItem{
				connection: connection,
				semaphore:  semaphore.NewWeighted(50),
			}

			item.semaphore.TryAcquire(1)

			p.connections = append(p.connections, item)
			p.mutex.Unlock()

			return connection, nil
		}

		tries++

		time.Sleep(1 * time.Millisecond)
	}

	p.mutex.Unlock()

	return nil, errors.New("no available connections")
}

func (p *ConnectionPool) Put(conn *Connection) {
	for _, item := range p.connections {
		if item.connection.id == conn.id {
			item.semaphore.Release(1)
			return
		}
	}
}

func (p *ConnectionPool) Remove(connection *Connection) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for i, item := range p.connections {
		if item.connection == connection {
			p.connections = append(p.connections[:i], p.connections[i+1:]...)
			p.activeConnections--
			connection.Close()
			return
		}
	}
}
