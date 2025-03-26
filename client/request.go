package client

import (
	"encoding/json"
	"strconv"
)

// request represent JSON-RPC request.
type Request struct {
	Id     *string          `json:"id"`     // Id is request Id.
	Method string           `json:"method"` // Method name of method to call
	Params json.RawMessage  `json:"params"` // Params is method parameters array
	res    chan<- *Response // res is channel to pass results of method invocation
}

func newRequest(_id uint64, _action *action) *Request {
	var id *string = nil

	if _action.action == requestAction {
		_idS := strconv.FormatUint(_id, 10)
		id = &_idS
	}

	return &Request{
		Id:     id,
		Method: _action.method,
		Params: _action.params,
		res:    _action.respChan,
	}
}
