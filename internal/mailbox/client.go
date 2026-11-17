package mailbox

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// Client is a minimal, dial-per-call client for the mailbox wire protocol.
// Each call opens its own connection, sends exactly one request, reads
// exactly one response, and closes, mirroring Serve's one-shot-per-
// connection contract. It is the only thing cmd/mailbox-extension's tool
// handlers use to reach the orchestrator.
type Client struct {
	Socket string
	Token  string
	// Timeout bounds dial, write, and read. Zero uses a 5-second default.
	Timeout time.Duration
}

func (c Client) call(req Request) (Response, error) {
	req.Token = c.Token
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	conn, err := net.DialTimeout("unix", c.Socket, timeout)
	if err != nil {
		return Response{}, fmt.Errorf("dial mailbox socket: %w", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	payload, err := json.Marshal(req)
	if err != nil {
		return Response{}, err
	}
	payload = append(payload, '\n')
	if _, err := conn.Write(payload); err != nil {
		return Response{}, fmt.Errorf("write mailbox request: %w", err)
	}

	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil && len(line) == 0 {
		return Response{}, fmt.Errorf("read mailbox response: %w", err)
	}
	var resp Response
	if err := json.Unmarshal(line, &resp); err != nil {
		return Response{}, fmt.Errorf("decode mailbox response: %w", err)
	}
	return resp, nil
}

// ReadInbox returns the token's own task's undelivered messages.
func (c Client) ReadInbox() (Response, error) {
	return c.call(Request{Op: OpReadInbox})
}

// SendMessage sends a task-to-task message to recipient to. The sender the
// orchestrator records is always the token's own task id; this client never
// supplies one.
func (c Client) SendMessage(to, kind, body string) (Response, error) {
	return c.call(Request{Op: OpSendMessage, To: to, Kind: kind, Body: body})
}
