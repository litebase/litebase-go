package sql

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Connection struct {
	accessKeyID     string
	accessKeySecret string
	buffers         *sync.Pool
	cancel          context.CancelFunc
	closed          bool
	connecting      bool
	connected       chan struct{}
	ctx             context.Context
	connectionError error
	id              string
	mutex           *sync.Mutex
	reader          io.ReadCloser
	responses       map[string]chan QueryResponse
	writeQueue      *WriteQueue
	writer          *bufio.Writer
	url             string
}

func NewConnection(url, accessKeyId, accessKeySecret string) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	reader, writer := io.Pipe()

	bufferedWriter := bufio.NewWriterSize(writer, 4096) // 4096 bytes buffer size

	c := &Connection{
		accessKeyID:     accessKeyId,
		accessKeySecret: accessKeySecret,
		buffers: &sync.Pool{
			New: func() interface{} {
				return &bytes.Buffer{}
			},
		},
		cancel:     cancel,
		connected:  make(chan struct{}, 1),
		connecting: false,
		ctx:        ctx,
		id:         uuid.NewString(),
		mutex:      &sync.Mutex{},
		reader:     reader,
		responses:  map[string]chan QueryResponse{},
		url:        url,
		writer:     bufferedWriter,
	}

	c.writeQueue = NewWriteQueue(c)
	c.connecting = true

	go func() {
		err := c.connect()

		if err != nil {
			c.connectionError = err
			c.Close()
		}
	}()

	return c
}

func (c *Connection) connect() error {
	connectionURL := fmt.Sprintf("%s/query/stream", c.url)

	url, err := url.Parse(connectionURL)

	if err != nil {
		return err
	}

	host := url.Hostname()

	if url.Port() != "" {
		host = fmt.Sprintf("%s:%s", host, url.Port())
	}

	token := SignRequest(
		c.accessKeyID,
		c.accessKeySecret,
		"POST",
		url.Path,
		map[string]string{
			"Content-Length": "0",
			"Content-Type":   "application/octet-stream",
			"Host":           host,
			"X-LBDB-Date":    fmt.Sprintf("%d", time.Now().Unix()),
		},
		nil,
		map[string]string{},
	)

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	httpClient := &http.Client{
		Timeout:   0,
		Transport: transport,
	}

	req, err := http.NewRequest("POST", url.String(), c.reader)

	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-LBDB-Date", fmt.Sprintf("%d", time.Now().Unix()))
	req.Header.Set("Authorization", fmt.Sprintf("Litebase-HMAC-SHA256 %s", token))

	respChan := make(chan *http.Response, 1)
	httpErrChan := make(chan error, 1)

	// Start the HTTP request
	go func() {
		resp, err := httpClient.Do(req)

		if err != nil {
			httpErrChan <- err
			return
		}
		respChan <- resp
	}()

	// Send connection message with timeout after request starts
	connectionMsgChan := make(chan error, 1)
	go func() {
		// Give the HTTP request a moment to start
		time.Sleep(10 * time.Millisecond)

		_, err := c.writer.Write([]byte{byte(QueryStreamOpenConnection)})
		if err != nil {
			connectionMsgChan <- err
			return
		}

		err = c.writer.Flush()
		if err != nil {
			connectionMsgChan <- err
			return
		}

		connectionMsgChan <- nil
	}()

	// Wait for connection message to be sent or timeout
	select {
	case err := <-connectionMsgChan:
		if err != nil {
			return fmt.Errorf("failed to send connection message: %w", err)
		}
	case <-time.After(3 * time.Second):
		return fmt.Errorf("timeout sending connection message after 3 seconds")
	}

	// Wait for HTTP response
	var resp *http.Response
	select {
	case resp = <-respChan:
		// HTTP request completed successfully
	case err := <-httpErrChan:
		return err
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for HTTP response")
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("request failed: %s", resp.Status)
	}

	defer resp.Body.Close()

	responseChan := make(chan *bytes.Buffer, 1)
	errChan := make(chan error, 1)

	// Read responses in a separate goroutine
	go func() {
		defer close(responseChan)
		defer close(errChan)

		messageHeaderBytes := make([]byte, 5)
		scanBuffer := bytes.NewBuffer(make([]byte, 1024))

		for {
			select {
			case <-c.ctx.Done():
				return
			default:
				scanBuffer.Reset()

				_, err := resp.Body.Read(messageHeaderBytes)

				if err != nil {
					errChan <- err
					return
				}

				messageType := messageHeaderBytes[0]
				messageLength := int(binary.LittleEndian.Uint32(messageHeaderBytes[1:]))

				bytesRead := 0

				for bytesRead < messageLength {
					chunkSize := min(messageLength-bytesRead, 1024)

					n, err := io.CopyN(scanBuffer, resp.Body, int64(chunkSize))

					if err != nil {
						log.Println(err)
						break
					}

					bytesRead += int(n)
				}

				switch QueryStreamMessageType(messageType) {
				case QueryStreamOpenConnection:
					c.connected <- struct{}{}
					c.connecting = false
				case QueryStreamError:
					errChan <- errors.New(string(scanBuffer.Bytes()[0:messageLength]))
				case QueryStreamFrame:
					responseBuffer := c.buffers.Get().(*bytes.Buffer)
					responseBuffer.Reset()
					responseBuffer.Write(scanBuffer.Bytes()[0:messageLength])
					responseChan <- responseBuffer
				}
			}
		}
	}()

	// Read responses
	for {
		select {
		case <-c.ctx.Done():
			return nil
		case err := <-errChan:
			log.Println("Error reading response:", err)
			c.Close()
			return err
		case responseBuffer := <-responseChan:
			for responseBuffer.Len() > 0 {
				queryResponses := QueryResponseDecoder(responseBuffer)
				data := queryResponses[0].Data
				id := data.ID

				c.mutex.Lock()
				responseChannel, ok := c.responses[string(id)]
				c.mutex.Unlock()

				if ok {
					responseChannel <- queryResponses[0]
				} else {
					c.buffers.Put(responseBuffer)
					log.Println("No response channel for id:", string(id))
					continue
				}
			}

			c.buffers.Put(responseBuffer)
		}
	}
}

func (c *Connection) Close() error {
	if c.closed {
		return nil
	}

	c.closed = true
	c.cancel()
	c.writeQueue.Close()
	c.writer.Flush()
	c.reader.Close()

	return nil
}

func (c *Connection) Send(query Query) (QueryResponse, error) {
	c.mutex.Lock()

	if c.connecting {
		select {
		case <-c.ctx.Done():
			if c.connectionError != nil {
				return QueryResponse{}, c.connectionError
			}
		case <-c.connected:
		}
	}

	c.mutex.Unlock()

	if c.closed {
		return QueryResponse{}, fmt.Errorf("connection is closed")
	}

	if query.ID == "" {
		return QueryResponse{}, fmt.Errorf("message must have an id")
	}

	responseChannel := make(chan QueryResponse, 1)

	c.mutex.Lock()
	c.responses[query.ID] = responseChannel
	c.mutex.Unlock()

	defer func() {
		c.mutex.Lock()
		delete(c.responses, query.ID)
		c.mutex.Unlock()
	}()

	outputBuffer := c.buffers.Get().(*bytes.Buffer)
	defer c.buffers.Put(outputBuffer)

	parametersBuffer := c.buffers.Get().(*bytes.Buffer)
	defer c.buffers.Put(parametersBuffer)

	queryRequest := QueryRequestEncoder(query, outputBuffer, parametersBuffer)

	c.writeQueue.Write(queryRequest)

	select {
	case response := <-responseChannel:
		return response, nil
	case <-time.After(3 * time.Second):
		return QueryResponse{}, fmt.Errorf("timeout waiting for response %s", query.ID)
	}
}
