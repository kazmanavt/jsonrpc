package client

import (
	"encoding/json"
	"fmt"
	"log/slog"
)

// broker is the central goroutine that manages message routing for a JSON-RPC connection.
// It handles both outgoing and incoming message flows:
//  1. Outgoing: processes actions from actionChan to send requests, notifications, and server call responses
//  2. Incoming: processes messages from responseChan, notificationChan, and callChan
//
// The broker maintains several state maps:
//   - pendingRequests: tracks outgoing requests awaiting responses
//   - pendingCalls: tracks ongoing server calls being processed
//   - notificationHandlers: registered handlers for incoming notifications
//   - callHandlers: registered handlers for incoming server calls
//
// When the connection closes, the broker ensures proper cleanup by:
//   - Notifying all pending requests with appropriate errors
//   - Cancelling any ongoing server calls
//
// Parameters:
//   - c: The Connection instance that owns this broker
//   - responseChan: Channel for incoming responses to client requests
//   - notificationChan: Channel for incoming server notifications
//   - callChan: Channel for incoming server calls
//
// The broker terminates when any channel is closed or when a critical connection error occurs.
func broker(c *Connection, actionChan <-chan *action, responseChan <-chan *Response, notificationChan <-chan *Response, callChan <-chan *Request) {
	defer c.wg.Done()
	reason := "unknown"
	defer func() { c.log.Debug("broker closed", slog.String("reason", reason)) }()
	defer c.cancel()
	//var actionChan <-chan *action = c.actionChan
	enc := json.NewEncoder(c.conn)
	nextID := uint64(0)
	pendingRequests := make(map[string]*Request)
	//pendingCalls := make(map[string]chan<- struct{}, 0)
	defer func() {
		for _, req := range pendingRequests {
			req.res <- &Response{Request: Request{Id: req.Id}, Err: c.Error()}
			close(req.res)
		}
		for act := range actionChan {
			switch act.action {
			case requestAction:
				act.idChan <- "0"
				act.respChan <- &Response{Err: c.Error()}
				close(act.respChan)
			case notificationAction:
				act.respChan <- &Response{Err: c.Error()}
				close(act.respChan)
			default:
			}
		}
		//for _, ch := range pendingCalls {
		//	close(ch)
		//}
	}()
	notificationHandlers := make(map[string]NotificationHandler)
	callHandlers := make(map[string]CallHandler)

	for {
		select {
		case <-c.ctx.Done():
			reason = "context done"
			return
		// send request/notification/call_response or
		// handle internal incoming actions supposed to operate in current goroutine
		case act, ok := <-actionChan:
			if !ok {
				reason = "action channel closed"
				return
			}
			switch act.action {
			case setNotificationHandlerAction:
				// ------------------------------------------
				// handle installation of notification handler
				notificationHandlers[act.method] = act.handler
			case setCallHandlerAction:
				// ------------------------------------------
				// handle installation of call handler
				callHandlers[act.method] = act.callHandler
			case dropPendingRequestAction:
				// ------------------------------------------
				// cancel pending request
				req, ok := pendingRequests[act.hId]
				if !ok {
					continue
				}
				delete(pendingRequests, act.hId)
				req.res <- &Response{Request: Request{Id: req.Id}, Err: fmt.Errorf("request cancelled")}
				close(req.res)
			case requestAction:
				// ------------------------------------------
				// handle outgoing request
				var err error
				nextID, err = doSendRequest(c, nextID, act, enc, pendingRequests)
				if err != nil {
					reason = err.Error()
					return
				}
			case notificationAction:
				// ------------------------------------------
				// handle outgoing notification
				if err := doSendNotification(c, act, enc); err != nil {
					reason = err.Error()
					return
				}
			case responseAction:
				// ------------------------------------------
				// handle outgoing call response
				if err := doSendCallResponse(c, act, enc); err != nil {
					reason = "failed to send call response"
					return
				}
			}

		// receive request result
		case res, ok := <-responseChan:
			if !ok {
				reason = "response channel closed"
				return
			}
			c.log.Debug("received response",
				slog.String("id", *res.Id),
				slog.String("response", string(res.Res)),
				slog.Any("error", res.Error()))
			req, ok := pendingRequests[*res.Id]
			if !ok {
				c.log.Debug("unknown response", slog.String("id", *res.Id))
				continue
			}
			delete(pendingRequests, *res.Id)
			req.res <- res
			close(req.res)

		// receive server notification
		case note, ok := <-notificationChan:
			if !ok {
				reason = "notification channel closed"
				return
			}
			c.log.Debug("received notification",
				slog.String("method", note.Method),
				slog.String("params", string(note.Params)))
			handler, ok := notificationHandlers[note.Method]
			if !ok {
				c.log.Debug("unknown notification", slog.String("method", note.Method))
				continue
			}
			handler(note.Params)

		// receive server call
		case call, ok := <-callChan:
			if !ok {
				reason = "call channel closed"
				return
			}
			c.log.Debug("received call",
				slog.String("id", *call.Id),
				slog.String("method", call.Method),
				slog.Any("params", call.Params))
			handler, ok := callHandlers[call.Method]
			if !ok {
				c.log.Debug("unknown call", slog.String("method", call.Method))
				continue
			}

			callRespChan := make(chan *Response)
			go func(method string) {
				select {
				case <-c.ctx.Done():
				case resp := <-callRespChan:
					c.actionChan <- &action{
						action:   responseAction,
						callResp: resp,
					}
				}
			}(call.Method)
			handler(*call.Id, call.Params, callRespChan)
		}
	}
}
