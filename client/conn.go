package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

var netDial = net.Dial

// NewClient creates a new JSON-RPC connection over the specified network protocol and address.
//
// Parameters:
//   - network: The network protocol to use (e.g., "tcp", "unix").
//   - addr: The address to connect to.
//   - _log: Optional logger. If nil, the default logger will be used.
//
// Returns:
//   - A new Connection instance ready to send and receive JSON-RPC messages.
//   - An error if the connection could not be established.
//
// The connection establishes communication channels for different message types
// and starts background goroutines for message processing.
func NewClient(network, addr string, _log *slog.Logger) (*Connection, error) {
	if _log == nil {
		_log = slog.Default()
	}

	c, err := netDial(network, addr)
	if err != nil {
		_log.Debug("failed to connect", slog.String("network", network), slog.String("addr", addr))
		return nil, fmt.Errorf("не удалось установить соединение: %w", err)
	}

	_log = _log.With(slog.String("jRPC-client", network+"://"+addr))
	_log.Info("connected", slog.String("jRPC-client", c.RemoteAddr().String()))

	return NewConnection(c, _log), nil
}

// NewConnection creates a new JSON-RPC Connection instance from an existing network connection.
//
// It sets up all necessary communication channels, initializes the connection structure,
// and launches background goroutines for message processing.
//
// Parameters:
//   - c: An established network connection implementing the net.Conn interface
//   - _log: Logger for connection-related events. If nil, the default logger will be used.
//
// Returns:
//   - A fully initialized Connection ready to send and receive JSON-RPC messages.
//
// The connection starts with two background goroutines:
//   - broker: Handles message distribution and maintains state for requests and handlers
//   - receiver: Reads and decodes incoming messages from the network connection
func NewConnection(c net.Conn, _log *slog.Logger) *Connection {
	_log = _log.With("jRPC-client", c.RemoteAddr().String())

	// Create communication channels for message exchange
	const chanBufferSize = 200
	actionChan := make(chan *action, chanBufferSize)
	notificationChan := make(chan *Response, chanBufferSize)
	responseChan := make(chan *Response, chanBufferSize)
	callChan := make(chan *Request, chanBufferSize)

	ctx, cancel := context.WithCancel(context.Background())
	conn := &Connection{
		defaultTimeout: 5 * time.Second,
		conn:           &rawConnection{Conn: c},
		actionChan:     actionChan,
		log:            _log,
		ctx:            ctx,
		cancel:         cancel,
	}

	// Start background goroutines for message processing
	conn.wg.Add(2)
	go broker(conn, actionChan, responseChan, notificationChan, callChan)
	go receiver(conn, responseChan, notificationChan, callChan)

	return conn
}

// Connection represents a JSON-RPC client that facilitates sending and receiving messages.
// It supports both synchronous and asynchronous calls, notifications, and
// processing incoming notifications/calls from the server.
type Connection struct {
	defaultTimeout time.Duration      // defaultTimeout default timeout for requests
	conn           *rawConnection     // conn basic net connection with error state
	mu             sync.RWMutex       // mu locks outgoing requests and Close function
	actionChan     chan *action       // actionChan channel for sending actions to the broker
	wg             sync.WaitGroup     // wg wait group for goroutines
	log            *slog.Logger       // log is a logger
	ctx            context.Context    // ctx is a context for the connection
	cancel         context.CancelFunc // cancel is a cancel function for the context
}

func (c *Connection) Done() <-chan struct{} {
	return c.ctx.Done()
}

// Error returns an error if the connection is in a failed state.
func (c *Connection) Error() error {
	var err error = nil
	switch {
	case c.conn.failState.Load():
		err = errors.New("failed connection")
	}
	return err
}

// Close closes the connection.
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.conn.Close()
	close(c.actionChan)
	c.cancel()
	c.wg.Wait()
	c.log.Info("connection closed")
	return err
}

func (c *Connection) DropPending(id string) {
	c.actionChan <- &action{
		action: dropPendingRequestAction,
		hId:    id,
	}
}

// validateContext check if context not nil and set default timeout if needed.
func (c *Connection) validateContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx, _ = context.WithTimeout(context.Background(), c.defaultTimeout)
	}
	return ctx
}

func (c *Connection) Notify(ctx context.Context, method string, params ...any) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.Error(); err != nil {
		return err
	}

	var paramsJSON []byte
	switch len(params) {
	case 0:
		paramsJSON = []byte(`[]`)
	default:
		var err error
		if paramsJSON, err = json.Marshal(params); err != nil {
			return fmt.Errorf("failed to marshal params: %w", err)
		}
	}

	respChan := make(chan *Response, 1)
	c.actionChan <- &action{
		action:   notificationAction,
		method:   method,
		params:   paramsJSON,
		ctx:      c.validateContext(ctx),
		respChan: respChan,
	}
	resp := <-respChan
	if resp.Err != nil {
		return resp.Error()
	}
	return nil
}

// Send sends a single JSON-RPC request asynchronously.
func (c *Connection) Send(ctx context.Context, method string, params ...any) (<-chan *Response, string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.Error(); err != nil {
		return nil, "", err
	}

	var paramsJSON []byte
	switch len(params) {
	case 0:
		paramsJSON = []byte(`[]`)
	default:
		var err error
		if paramsJSON, err = json.Marshal(params); err != nil {
			return nil, "", fmt.Errorf("failed to marshal params: %w", err)
		}
	}

	respChan := make(chan *Response, 1)
	idChan := make(chan string)
	c.actionChan <- &action{
		action:   requestAction,
		method:   method,
		params:   paramsJSON,
		ctx:      c.validateContext(ctx),
		idChan:   idChan,
		respChan: respChan,
	}
	return respChan, <-idChan, nil
}

// Call sends a single JSON-RPC request synchronously.
func (c *Connection) Call(ctx context.Context, method string, params ...any) (json.RawMessage, error) {
	ctx = c.validateContext(ctx)
	respChan, id, err := c.Send(ctx, method, params...)
	if err != nil {
		return nil, err
	}
	// wait for response or context cancellation
	select {
	case res := <-respChan:
		return res.Res, res.Error()
	case <-ctx.Done():
		c.DropPending(id)
		return nil, ctx.Err()
	}
}

// Handle sets notification handler for incoming JSON-RPC notification.
func (c *Connection) Handle(method string, handler NotificationHandler) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if err := c.Error(); err != nil {
		return err
	}
	c.actionChan <- &action{
		action:  setNotificationHandlerAction,
		method:  method,
		handler: handler,
	}
	return nil
}

// HandleCall sets call handler for incoming JSON-RPC call.
func (c *Connection) HandleCall(method string, handler CallHandler) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if err := c.Error(); err != nil {
		return err
	}
	c.actionChan <- &action{
		action:      setCallHandlerAction,
		method:      method,
		callHandler: handler,
	}
	return nil
}
