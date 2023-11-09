package middleware

import (
	"errors"
	"net"
	"os"
	"regexp"
	"strconv"
	"sync"

	"github.com/go-kit/log"
	"go.uber.org/atomic"
)

// NewZeroResponseListener returns a Listener that logs all connections that encountered io timeout on reads, and were closed before sending any response.
func NewZeroResponseListener(list net.Listener, log log.Logger) net.Listener {
	return &zeroResponseListener{
		Listener: list,
		log:      log,
		bufPool: sync.Pool{
			New: func() any { return make([]byte, 0, requestBufSize) },
		},
	}
}

// Size of buffer for read data. We log this eventually.
const requestBufSize = 512

type zeroResponseListener struct {
	net.Listener
	log     log.Logger
	bufPool sync.Pool
}

func (zl *zeroResponseListener) Accept() (net.Conn, error) {
	conn, err := zl.Listener.Accept()
	if err != nil {
		return nil, err
	}
	buf := zl.bufPool.Get().([]byte)
	buf = buf[:0]
	return &zeroResponseConn{Conn: conn, log: zl.log, buf: buf, returnPool: &zl.bufPool}, nil
}

type zeroResponseConn struct {
	net.Conn

	log        log.Logger
	once       sync.Once
	returnPool *sync.Pool

	bufMu sync.Mutex
	buf   []byte // Buffer with first requestBufSize bytes from connection. Set to nil as soon as data is written to the connection.

	lastReadErrIsDeadlineExceeded atomic.Bool
}

func (zc *zeroResponseConn) Read(b []byte) (n int, err error) {
	n, err = zc.Conn.Read(b)
	if err != nil && errors.Is(err, os.ErrDeadlineExceeded) {
		zc.lastReadErrIsDeadlineExceeded.Store(true)
	} else {
		zc.lastReadErrIsDeadlineExceeded.Store(false)
	}

	// Store first requestBufSize read bytes on connection into the buffer for logging.
	if n > 0 {
		zc.bufMu.Lock()
		defer zc.bufMu.Unlock()

		if zc.buf != nil {
			rem := requestBufSize - len(zc.buf) // how much space is in our buffer.
			if rem > n {
				rem = n
			}
			if rem > 0 {
				zc.buf = append(zc.buf, b[:rem]...)
			}
		}
	}
	return
}

func (zc *zeroResponseConn) Write(b []byte) (n int, err error) {
	n, err = zc.Conn.Write(b)
	if n > 0 {
		zc.bufMu.Lock()
		if zc.buf != nil {
			zc.returnPool.Put(zc.buf)
			zc.buf = nil
		}
		zc.bufMu.Unlock()
	}
	return
}

var authRegexp = regexp.MustCompile(`((?i)\r\nauthorization:\s+)(\S+\s+)(\S+)`)

func (zc *zeroResponseConn) Close() error {
	err := zc.Conn.Close()

	zc.once.Do(func() {
		zc.bufMu.Lock()
		defer zc.bufMu.Unlock()

		// If buffer was already returned, it means there was some data written on the connection, nothing to do.
		if zc.buf == nil {
			return
		}

		// If we didn't write anything to this connection, and we've got timeout while reading data, it looks like
		// slow a slow client failing to send a request to us.
		if !zc.lastReadErrIsDeadlineExceeded.Load() {
			return
		}

		b := zc.buf
		b = authRegexp.ReplaceAll(b, []byte("${1}${2}***")) // Replace value in Authorization header with ***.

		_ = zc.log.Log("msg", "read timeout, connection closed with no response", "read", strconv.Quote(string(b)), "remote", zc.RemoteAddr().String())

		zc.returnPool.Put(zc.buf)
		zc.buf = nil
	})

	return err
}
