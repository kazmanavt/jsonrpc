package client

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
)

// receiver is a goroutine that processes incoming JSON-RPC messages from the connection.
//
// It continuously reads from the connection, decodes messages, and routes them to the appropriate channel
// based on message type:
//   - Responses to client requests go to responseChan
//   - Server notifications go to notificationChan
//   - Server calls go to callChan
//
// The receiver handles error conditions during message reading and decoding:
//   - For fatal errors (EOF, deadline exceeded with connection failure, closed connection),
//     it marks the connection as failed and terminates
//   - For non-fatal decoding errors, it logs the error and continues processing
//
// When the receiver terminates (due to connection errors), it properly closes all output channels
// and decrements the connection's wait group counter.
// Parameters:
//   - c: The Connection instance that owns this receiver
//   - responseChan: Channel for sending received responses to pending client requests
//   - notificationChan: Channel for sending received server notifications
//   - callChan: Channel for sending received server calls
func receiver(c *Connection, responseChan chan<- *Response, notificationChan chan<- *Response, callChan chan<- *Request) {
	defer c.wg.Done()
	defer c.log.Debug("receiver closed")
	defer close(notificationChan)
	defer close(responseChan)
	defer close(callChan)
	defer c.cancel()

	dec := json.NewDecoder(c.conn)

	for {
		var resp Response
		if err := dec.Decode(&resp); err != nil && !errors.Is(err, io.EOF) {
			//if errors.Is(err, io.EOF) ||
			//	(errors.Is(err, os.ErrDeadlineExceeded) && c.conn.failState.Load()) ||
			//	strings.Contains(err.Error(), "use of closed network connection") ||
			//	strings.Contains(err.Error(), "io: read/write on closed pipe") {
			//	c.log.Debug("broken connection", slog.String("error", err.Error()))
			//	c.conn.failState.Store(true)
			//	return
			//}
			c.conn.failState.Store(true)
			c.log.Debug("connection problem or fail to decode response", slog.String("error", err.Error()))
			return
		}
		if resp.IsNotification() {
			// notification
			notificationChan <- &resp
			continue
		}
		if resp.IsCall() {
			// call
			callChan <- &resp.Request
			continue
		}
		responseChan <- &resp
	}
}
