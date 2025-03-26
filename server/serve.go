package server

import (
	"github.com/kazmanavt/jsonrpc/client"
	"log/slog"
)

func serve(srv *ServerConnection) {
	defer srv.cancel()
	defer srv.log.Info("server stopped")
LISTEN:
	for {
		conn, err := srv.listener.Accept()
		if err != nil {
			srv.log.Error("failed to accept connection", slog.String("error", err.Error()))
			return
		}
		srv.log.Info("new connection accepted", slog.Any("remote", conn.RemoteAddr()))

		clog := srv.log.With(slog.String("remote", conn.RemoteAddr().String()))
		c := client.NewConnection(conn, clog)
		for method, handler := range srv.notificationHandlers {
			if err := c.Handle(method, func(params []byte) { handler(c, params) }); err != nil {
				clog.Error("failed to set notification handler", slog.String("method", method), slog.String("error", err.Error()))
				if err := c.Close(); err != nil {
					clog.Warn("failed to close connection", slog.String("error", err.Error()))
				}
				continue LISTEN
			}
		}
		for method, handler := range srv.callHandlers {
			if err := c.HandleCall(method, func(id string, params []byte, respChan chan<- *client.Response) { handler(c, id, params, respChan) }); err != nil {
				clog.Error("failed to set call handler", slog.String("method", method), slog.String("error", err.Error()))
				if err := c.Close(); err != nil {
					clog.Warn("failed to close connection", slog.String("error", err.Error()))
				}
				continue LISTEN
			}
		}

		go func() {
			select {
			case <-srv.ctx.Done():
				clog.Info("server connection closed")
			case <-c.Done():
				clog.Info("connection closed")
			}
			if err := c.Close(); err != nil {
				clog.Warn("failed to close connection", slog.String("error", err.Error()))
			}
		}()
	}
}
