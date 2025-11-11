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

// EncodeWithSignature creates a signed frame with chunk signature following the LQTP protocol.
// This implements chunked signature validation similar to AWS Signature Version 4.
// The signature chains from the previous signature to ensure order and integrity.
// Returns the encoded frame bytes and the signature for use in the next chunk.
//
// Frame format (LQTP protocol):
// [MessageType:1][FrameLength:4][SignatureLength:4][Signature:N][FrameData]
func (f *Frame) EncodeWithSignature(accessKeySecret, date, previousSignature string) ([]byte, string) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.closed = true

	// Build the frame data first (without the signature header)
	frameData := []byte{}
	
	for _, queryRequest := range f.queries {
		frameData = binary.LittleEndian.AppendUint32(frameData, uint32(len(queryRequest))) // Query request length
		frameData = append(frameData, queryRequest...)                                     // Query request
	}

	// Calculate the chunk signature for this frame data
	chunkSignature := SignChunk(accessKeySecret, date, previousSignature, frameData)

	// Now build the complete frame with signature metadata
	// Format: [MessageType:1][FrameLength:4][SignatureLength:4][Signature:N][FrameData]
	frame := []byte{byte(QueryStreamFrame)}
	
	// Calculate total length: signature length (4) + signature + frame data
	signatureBytes := []byte(chunkSignature)
	totalLength := 4 + len(signatureBytes) + len(frameData)
	
	frame = binary.LittleEndian.AppendUint32(frame, uint32(totalLength))
	frame = binary.LittleEndian.AppendUint32(frame, uint32(len(signatureBytes)))
	frame = append(frame, signatureBytes...)
	frame = append(frame, frameData...)

	return frame, chunkSignature
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
