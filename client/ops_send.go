package client

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

// doSendRequest sends a JSON-RPC request to the server and manages the request lifecycle.
// It creates a request with the provided ID, sets appropriate deadlines, handles context cancellation,
// and processes the response or error conditions. The function also manages the pending requests map
// for tracking in-flight requests.
// Parameters:
//   - c: The Connection instance that owns this request
//   - nextID: The ID to use for this request
//   - act: The action containing request details (method, params, context, etc.)
//   - enc: JSON encoder to use for writing the request
//   - pendingRequests: Map of pending requests indexed by ID
//
// Returns:
//   - nextID2: The next available ID for future requests
//   - err: non nil if the connection enters a failed state during request sending,
//     signaling that the broker should terminate its event loop.
func doSendRequest(c *Connection, nextID uint64, act *action, enc *json.Encoder, pendingRequests map[string]*Request) (nextID2 uint64, err error) {
	req := newRequest(nextID, act)
	nextID++
	deadline, ok := act.ctx.Deadline()
	if !ok {
		deadline = time.Time{} // no deadline -- use zero time to wait forever
	}
	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		c.log.Warn("fail to set write deadline", slog.String("error", err.Error()))
	}
	fin := make(chan struct{})
	go func() {
		select {
		case <-act.ctx.Done():
			if act.ctx.Err() == context.Canceled {
				_ = c.conn.SetWriteDeadline(time.Now())
				c.log.Debug("request cancelled",
					slog.String("method", act.method),
					slog.String("id", *req.Id))
			}
		case <-fin:
		}
	}()
	err = enc.Encode(req)
	close(fin)
	if err != nil {
		act.idChan <- *req.Id
		close(act.idChan)
		act.respChan <- &Response{Request: Request{Id: req.Id}, Err: err}
		close(act.respChan)
		if c.conn.failState.Load() {
			c.log.Debug("connection moved to failed state")
			return 0, fmt.Errorf("connection moved to failed state: %w", err)
		}
		return nextID, nil
	}

	if c.log.Enabled(context.Background(), slog.LevelDebug) {
		req, _ := json.Marshal(req)
		c.log.Debug("sent request", slog.String("request", string(req)))
	}
	pendingRequests[*req.Id] = req
	act.idChan <- *req.Id
	close(act.idChan)
	return nextID, nil
}

// doSendNotification sends a JSON-RPC notification to the server.
// Parameters:
//   - c: The Connection instance that owns this notification
//   - act: The action containing notification details (method, params, context)
//   - enc: JSON encoder to use for writing the notification
//
// Returns error if the connection enters a failed state during notification sending,
// signaling that the broker should terminate its event loop.
func doSendNotification(c *Connection, act *action, enc *json.Encoder) error {
	req := newRequest(0, act)
	deadline, ok := act.ctx.Deadline()
	if !ok {
		deadline = time.Time{} // no deadline -- use zero time to wait forever
	}
	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		c.log.Warn("fail to set write deadline", slog.String("error", err.Error()))
	}
	fin := make(chan struct{})
	go func() {
		select {
		case <-act.ctx.Done():
			if act.ctx.Err() == context.Canceled {
				_ = c.conn.SetWriteDeadline(time.Now())
				c.log.Debug("notification canceled",
					slog.String("method", act.method))
			}
		case <-fin:
		}
	}()
	err := enc.Encode(req)
	close(fin)
	if err != nil {
		act.respChan <- &Response{Err: err}
		close(act.respChan)
		if c.conn.failState.Load() {
			c.log.Debug("connection moved to failed state")
			return fmt.Errorf("connection moved to failed state: %w", err)
		}
		return nil
	}
	if c.log.Enabled(context.Background(), slog.LevelDebug) {
		//req, _ := json.Marshal(req)
		c.log.Debug("sent notification", slog.Any("request", req))
	}
	act.respChan <- &Response{}
	close(act.respChan)
	return nil
}

// doSendCallResponse sends a JSON-RPC response back to the server for a received call.
// It sets a write deadline for the operation, encodes the response, and handles any errors
// that occur during sending.
//
// Parameters:
//   - c: The Connection instance that owns this call response
//   - act: The action containing call response details
//   - enc: JSON encoder to use for writing the response
//
// Returns error if the connection enters a failed state during response sending,
// signaling that the broker should terminate its event loop. Otherwise, returns true.
func doSendCallResponse(c *Connection, act *action, enc *json.Encoder) error {
	deadline := time.Now().Add(c.defaultTimeout)
	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		c.log.Warn("fail to set write deadline", slog.String("error", err.Error()))
	}
	err := enc.Encode(act.callResp)
	if err != nil {
		if c.conn.failState.Load() {
			c.log.Debug("connection moved to failed state")
			return fmt.Errorf("connection moved to failed state: %w", err)
		}
		c.log.Debug("failed to send call response", slog.String("error", err.Error()))
		return nil
	}
	if c.log.Enabled(context.Background(), slog.LevelDebug) {
		resp, _ := json.Marshal(act.callResp)
		c.log.Debug("sent call response", slog.String("response", string(resp)))
	}
	return nil
}
