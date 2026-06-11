package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"
)

// MuxAcceptor owns one TCP listener and demultiplexes incoming raft
// connections to per-shard StreamLayers by a uint32 group ID written
// on the connection as the first 4 bytes after dial.
//
// Multi-Raft motivation: every shard binds its own logical raft
// transport, but they all share one OS-level TCP port. The wire shape
// is a 4-byte BE group ID prefix on every accept, then raw
// hraft.NetworkTransport takes over. No new RPCs, no protobuf, no
// breaking change to hashicorp/raft.
type MuxAcceptor struct {
	listener net.Listener

	mu     sync.Mutex
	groups map[uint32]*muxStreamLayer
	closed bool

	logger io.Writer
}

// NewMuxAcceptor opens a TCP listener at bindAddr and starts the
// accept loop. Each call to RegisterGroup returns a StreamLayer that
// receives connections whose group-ID prefix matches.
func NewMuxAcceptor(bindAddr string, logOut io.Writer) (*MuxAcceptor, error) {
	if logOut == nil {
		logOut = io.Discard
	}
	l, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("mux listen %s: %w", bindAddr, err)
	}
	a := &MuxAcceptor{
		listener: l,
		groups:   make(map[uint32]*muxStreamLayer),
		logger:   logOut,
	}
	go a.acceptLoop()
	return a, nil
}

// Addr returns the actual bind address of the underlying listener
// (useful when bindAddr was passed as ":0" for an ephemeral port).
func (a *MuxAcceptor) Addr() net.Addr { return a.listener.Addr() }

// Close stops the accept loop and closes the underlying listener and
// every registered StreamLayer.
func (a *MuxAcceptor) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	groups := a.groups
	a.groups = nil
	a.mu.Unlock()

	_ = a.listener.Close()
	for _, g := range groups {
		g.closeQueue()
	}
	return nil
}

// RegisterGroup returns a StreamLayer that owns connections matching
// the given groupID. Each groupID must be registered exactly once per
// MuxAcceptor.
func (a *MuxAcceptor) RegisterGroup(groupID uint32) (hraft.StreamLayer, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, errors.New("mux: acceptor closed")
	}
	if _, exists := a.groups[groupID]; exists {
		return nil, fmt.Errorf("mux: groupID %d already registered", groupID)
	}
	sl := newMuxStreamLayer(a, groupID)
	a.groups[groupID] = sl
	return sl, nil
}

// UnregisterGroup removes and closes a previously registered group.
// It is used when a shard fails to finish opening after reserving its
// mux group.
func (a *MuxAcceptor) UnregisterGroup(groupID uint32) error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return errors.New("mux: acceptor closed")
	}
	sl, ok := a.groups[groupID]
	if ok {
		delete(a.groups, groupID)
	}
	a.mu.Unlock()
	if ok {
		sl.closeQueue()
	}
	return nil
}

func (a *MuxAcceptor) acceptLoop() {
	for {
		conn, err := a.listener.Accept()
		if err != nil {
			a.mu.Lock()
			closed := a.closed
			a.mu.Unlock()
			if closed {
				return
			}
			fmt.Fprintf(a.logger, "mux: accept error: %v\n", err)
			continue
		}
		go a.routeConn(conn)
	}
}

func (a *MuxAcceptor) routeConn(conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	var idBuf [4]byte
	if _, err := io.ReadFull(conn, idBuf[:]); err != nil {
		_ = conn.Close()
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	groupID := binary.BigEndian.Uint32(idBuf[:])

	a.mu.Lock()
	sl, ok := a.groups[groupID]
	a.mu.Unlock()
	if !ok {
		fmt.Fprintf(a.logger, "mux: unknown groupID %d, dropping connection\n", groupID)
		_ = conn.Close()
		return
	}
	sl.push(conn)
}

// muxStreamLayer is the per-group StreamLayer surface hraft's
// NetworkTransport consumes.
type muxStreamLayer struct {
	parent  *MuxAcceptor
	groupID uint32

	queue  chan net.Conn
	closed chan struct{}
}

func newMuxStreamLayer(parent *MuxAcceptor, groupID uint32) *muxStreamLayer {
	return &muxStreamLayer{
		parent:  parent,
		groupID: groupID,
		queue:   make(chan net.Conn, 32),
		closed:  make(chan struct{}),
	}
}

func (l *muxStreamLayer) push(conn net.Conn) {
	select {
	case l.queue <- conn:
	case <-l.closed:
		_ = conn.Close()
	}
}

func (l *muxStreamLayer) closeQueue() {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
drain:
	for {
		select {
		case c := <-l.queue:
			_ = c.Close()
		default:
			break drain
		}
	}
}

// Accept blocks until a connection arrives or the layer is closed.
func (l *muxStreamLayer) Accept() (net.Conn, error) {
	select {
	case c := <-l.queue:
		return c, nil
	case <-l.closed:
		return nil, errors.New("mux: stream layer closed")
	}
}

func (l *muxStreamLayer) Close() error {
	l.closeQueue()
	return nil
}

func (l *muxStreamLayer) Addr() net.Addr { return l.parent.Addr() }

// Dial opens a TCP connection to address and prefixes it with this
// group's ID. The returned conn is the raw connection (prefix
// already written); hraft.NetworkTransport runs its handshake on top.
func (l *muxStreamLayer) Dial(address hraft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.Dial("tcp", string(address))
	if err != nil {
		return nil, err
	}
	var idBuf [4]byte
	binary.BigEndian.PutUint32(idBuf[:], l.groupID)
	if _, err := conn.Write(idBuf[:]); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("mux dial %s: write groupID: %w", address, err)
	}
	return conn, nil
}
