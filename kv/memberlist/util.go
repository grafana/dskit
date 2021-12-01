package memberlist

import (
	"net"
	"time"

	"github.com/pkg/errors"
)

type connectionWithTimeout struct {
	net.Conn
	timeout time.Duration
}

func newConnectionWithTimeout(conn net.Conn, timeout time.Duration) net.Conn {
	return &connectionWithTimeout{
		Conn:    conn,
		timeout: timeout,
	}
}

// Read implements net.Conn.
func (c *connectionWithTimeout) Read(b []byte) (n int, err error) {
	if err := c.Conn.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, errors.Wrap(err, "set deadline")
	}

	return c.Conn.Read(b)
}

// Write implements net.Conn.
func (c *connectionWithTimeout) Write(b []byte) (n int, err error) {
	if err := c.Conn.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, errors.Wrap(err, "set deadline")
	}

	return c.Conn.Write(b)
}
