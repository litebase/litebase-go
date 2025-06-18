package sql

import (
	"context"
	"log"
	"sync"
	"time"
)

// const writeDelay = 50 * time.Millisecond

type WriteQueue struct {
	connection *Connection
	cancel     context.CancelFunc
	ctx        context.Context
	frames     []*Frame
	mutex      *sync.Mutex
	// queryCount int
	// lastCheck  time.Time
}

func NewWriteQueue(connection *Connection) *WriteQueue {
	ctx, cancel := context.WithCancel(context.Background())

	w := &WriteQueue{
		cancel:     cancel,
		connection: connection,
		ctx:        ctx,
		frames:     []*Frame{},
		mutex:      &sync.Mutex{},
		// queryCount: 0,
		// lastCheck:  time.Now(),
	}

	go w.work()

	return w
}

// func (w *WriteQueue) calculateDelay() time.Duration {
// 	now := time.Now()
// 	elapsed := now.Sub(w.lastCheck).Seconds()

// 	if elapsed >= 1 {
// 		qps := float64(w.queryCount) / elapsed
// 		w.queryCount = 0
// 		w.lastCheck = now

// 		// Add 1ms delay per 1000 QPS
// 		if qps > 500 {
// 			return time.Duration(qps/500) * time.Millisecond
// 		}
// 	}

// 	return 0
// }

func (w *WriteQueue) Close() {
	w.cancel()
}

func (w *WriteQueue) work() {
	for {
		select {
		case <-w.ctx.Done():
			log.Println("Write queue stopped")
			return
		default:
			// if delay > 0 {
			time.Sleep(100 * time.Microsecond)
			// }

			w.mutex.Lock()
			w.connection.mutex.Lock()

			if len(w.frames) > 0 {
				frame := w.frames[0]
				// log.Println("Writing frame:", len(frame.queries))
				_, err := w.connection.writer.Write(frame.Encode())

				if err != nil {
					log.Println("Error writing request:", err)
					// TODO: Handle error
				}

				err = w.connection.writer.Flush()

				if err != nil {
					log.Println("Error flushing buffer:", err)
					// TODO: Handle error
				}

				w.frames = w.frames[1:]
			}

			w.connection.mutex.Unlock()
			w.mutex.Unlock()
		}
	}
}

func (w *WriteQueue) Write(query []byte) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Find a frame that has capacity
	var writingFrame *Frame

	for _, frame := range w.frames {
		if !frame.IsFull() && !frame.IsClosed() {
			writingFrame = frame
			break
		}
	}

	// Create new frame if needed
	if writingFrame == nil {
		writingFrame = NewFrame()
		w.frames = append(w.frames, writingFrame)
	}

	writingFrame.AddQuery(query)
}
