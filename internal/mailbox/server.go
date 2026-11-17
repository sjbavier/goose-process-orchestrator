package mailbox

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
)

// Serve accepts connections on listener until ctx is cancelled or Accept
// fails, handling each one independently and concurrently. Every connection
// carries exactly one request/response pair (see Handle): nothing here
// holds any state across multiple calls, so each call is independently
// token-checked rather than trusting an earlier handshake on the same
// connection.
func Serve(ctx context.Context, listener net.Listener, tokens *Tokens, store Store) error {
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go serveConn(ctx, conn, tokens, store)
	}
}

func serveConn(ctx context.Context, conn net.Conn, tokens *Tokens, store Store) {
	defer conn.Close()
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil && len(line) == 0 {
		return
	}
	var req Request
	var resp Response
	if err := json.Unmarshal(line, &req); err != nil {
		resp = Response{Error: "malformed request"}
	} else {
		resp = Handle(ctx, tokens, store, req)
	}
	out, err := json.Marshal(resp)
	if err != nil {
		return
	}
	out = append(out, '\n')
	_, _ = conn.Write(out)
}
