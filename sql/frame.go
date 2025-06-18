package sql

import (
	"encoding/binary"
	"sync"
)

const MaxFrameSize = 100

type Frame struct {
	closed  bool
	mutex   *sync.Mutex
	queries [][]byte
}

func NewFrame() *Frame {
	return &Frame{
		closed: false,
		mutex:  &sync.Mutex{},
	}
}

func (f *Frame) AddQuery(query []byte) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.queries = append(f.queries, query)
}

func (f *Frame) Encode() []byte {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.closed = true

	// Write the message type to be a QueryStreamFrame
	frame := []byte{byte(QueryStreamFrame)}

	queryDataLength := 0

	for _, queryRequest := range f.queries {
		queryDataLength += len(queryRequest) + 4 // 4 bytes for the length of the query
	}

	// Write the length of the frame
	frame = binary.LittleEndian.AppendUint32(frame, uint32(queryDataLength)) // Frame length

	// Write the query requests
	for _, queryRequest := range f.queries {
		frame = binary.LittleEndian.AppendUint32(frame, uint32(len(queryRequest))) // Query request length
		frame = append(frame, queryRequest...)                                     // Query request
	}

	return frame
}

func (f *Frame) IsClosed() bool {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.closed
}

func (f *Frame) IsFull() bool {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return len(f.queries) >= MaxFrameSize
}

func (f *Frame) Write(query []byte) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.queries = append(f.queries, query)
}
