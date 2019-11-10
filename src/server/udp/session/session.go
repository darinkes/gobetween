package session

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet"
	"github.com/yyyar/gobetween/core"
	"github.com/yyyar/gobetween/logging"
	"github.com/yyyar/gobetween/server/scheduler"

	"golang.org/x/sys/unix"
)

const (
	UDP_PACKET_SIZE   = 65507
	MAX_PACKETS_QUEUE = 10000
)

var log = logging.For("udp/server/session")
var bufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, UDP_PACKET_SIZE)
	},
}


type packet struct {
	// pointer to object that has to be returned to buf pool
	payload []byte
	// length of the usable part of buffer
	len int
}

func (p packet) buf() []byte {
	if p.payload == nil {
		return nil
	}

	return p.payload[0:p.len]
}

func (p packet) release() {
	if p.payload == nil {
		return
	}
	bufPool.Put(p.payload)
}

type Session struct {
	*gnet.EventServer

	//counters
	sent uint64
	recv uint64

	//session config
	cfg Config

	clientAddr *net.UDPAddr

	//connection to backend
	conn    *gnet.Client
	backend core.Backend

	serverConn gnet.Conn

	//communication
	out     chan packet
	stopC   chan struct{}
	stopped uint32

	//scheduler
	scheduler *scheduler.Scheduler
}

var establishedChannel chan *gnet.Client

func (s *Session) OnConnectionEstablished(c *gnet.Client) (action gnet.Action) {
        establishedChannel <- c
        log.Debugf("Established: %v -> %v\n", c.LocalAddr(), c.RemoteAddr())
        return
}

func (s *Session) React(c gnet.Conn) (out []byte, action gnet.Action) {
	data := c.Read()

	s.scheduler.IncrementRx(s.backend, uint(len(data)))

	_ = unix.Sendto(s.serverConn.Fd(), data, 0, s.serverConn.Sa())

	if s.cfg.MaxResponses > 0 && atomic.AddUint64(&s.recv, 1) >= s.cfg.MaxResponses {
		return
	}

        c.ResetBuffer()
        return
}

func NewSession(clientAddr *net.UDPAddr, backend core.Backend, scheduler *scheduler.Scheduler, cfg Config, serverConn gnet.Conn) *Session {
	scheduler.IncrementConnection(backend)
	s := &Session{
		cfg:        cfg,
		clientAddr: clientAddr,
		backend:    backend,
		scheduler:  scheduler,
		out:        make(chan packet, MAX_PACKETS_QUEUE),
		stopC:      make(chan struct{}, 1),
		serverConn: serverConn,
	}

	establishedChannel = make(chan *gnet.Client, 1)

	go func() {
		addrStr := fmt.Sprintf("udp://%s:%s", backend.Host, backend.Port)
		err := gnet.Connect(s, addrStr)
		if err != nil {
			panic(err)
		}
	}()
	s.conn = <-establishedChannel

	go func() {

		var t *time.Timer
		var tC <-chan time.Time

		if cfg.ClientIdleTimeout > 0 {
			t = time.NewTimer(cfg.ClientIdleTimeout)
			tC = t.C
		}

		for {
			select {

			case <-tC:
				s.Close()
			case pkt := <-s.out:
				if t != nil {
					if !t.Stop() {
						<-t.C
					}
					t.Reset(cfg.ClientIdleTimeout)
				}

				if pkt.payload == nil {
					panic("Program error, output channel should not be closed here")
				}

				s.conn.Write(pkt.buf())
				pkt.release()

				s.scheduler.IncrementTx(s.backend, uint(pkt.len))

				if s.cfg.MaxRequests > 0 && atomic.AddUint64(&s.sent, 1) > s.cfg.MaxRequests {
					log.Errorf("Restricted to send more UDP packets")
					break
				}
			case <-s.stopC:
				atomic.StoreUint32(&s.stopped, 1)
				if t != nil {
					t.Stop()
				}
				s.conn.Close()
				s.scheduler.DecrementConnection(s.backend)
				// drain output packets channel and free buffers
				for {
					select {
					case pkt := <-s.out:
						pkt.release()
					default:
						return
					}
				}

			}
		}

	}()

	return s
}

func (s *Session) Write(buf []byte) error {
	if atomic.LoadUint32(&s.stopped) == 1 {
		return fmt.Errorf("Closed session")
	}

	dup := bufPool.Get().([]byte)
	n := copy(dup, buf)

	select {
	case s.out <- packet{dup, n}:
	default:
		bufPool.Put(dup)
	}

	return nil
}

func (s *Session) IsDone() bool {
	return atomic.LoadUint32(&s.stopped) == 1
}

func (s *Session) Close() {
	select {
	case s.stopC <- struct{}{}:
	default:
	}
}
