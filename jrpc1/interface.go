package jrpc1

import (
	"context"
	"encoding/json"
)

//type NotificationHandler func(params []byte)

//type CallHandler func(id string, params [][]byte, respChan chan<- any)

type jsonValueType = json.RawMessage

type Connection interface {
	Done() <-chan struct{}
	//Error() error
	Close() error
	Notify(ctx context.Context, method string, params ...any) error
	Send(ctx context.Context, method string, params ...any) (<-chan Response, error)
	Call(ctx context.Context, method string, params ...any) (Response, error)
	HandleNotification(method string, fn any) error
	HandleCall(method string, fn any) error
}

type Response interface {
	GetErr() []byte
	GetResult() []byte
	Error() error
}
type Server interface {
	Close() error
	HandleNotification(method string, fn any) error
	HandleCall(method string, fn any) error
}
