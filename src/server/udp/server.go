package udp

/**
 * server.go - UDP server implementation
 *
 * @author Illarion Kovalchuk <illarion.kovalchuk@gmail.com>
 */

import (
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yyyar/gobetween/balance"
	"github.com/yyyar/gobetween/config"
	"github.com/yyyar/gobetween/core"
	"github.com/yyyar/gobetween/discovery"
	"github.com/yyyar/gobetween/healthcheck"
	"github.com/yyyar/gobetween/logging"
	"github.com/yyyar/gobetween/server/modules/access"
	"github.com/yyyar/gobetween/server/scheduler"
	"github.com/yyyar/gobetween/server/udp/session"
	"github.com/yyyar/gobetween/stats"
	"github.com/yyyar/gobetween/utils"
	"github.com/panjf2000/gnet"
)

const UDP_PACKET_SIZE = 65507
const CLEANUP_EVERY = time.Second * 2

var log = logging.For("udp/server")

/**
 * UDP server implementation
 */
type Server struct {
	/* gnet */
	*gnet.EventServer

	/* Server name */
	name string

	/* Server configuration */
	cfg config.Server

	/* Scheduler */
	scheduler *scheduler.Scheduler

	/* Flag indicating that server is stopped */
	stopped uint32

	/* Stop channel */
	stop chan bool

	/* ----- modules ----- */

	/* Access module checks if client is allowed to connect */
	access *access.Access

	/* ----- sessions ----- */
	sessions map[string]*session.Session
	mu       sync.Mutex
}

/**
 * Creates new UDP server
 */
func New(name string, cfg config.Server) (*Server, error) {

	statsHandler := stats.NewHandler(name)
	scheduler := &scheduler.Scheduler{
		Balancer:     balance.New(nil, cfg.Balance),
		Discovery:    discovery.New(cfg.Discovery.Kind, *cfg.Discovery),
		Healthcheck:  healthcheck.New(cfg.Healthcheck.Kind, *cfg.Healthcheck),
		StatsHandler: statsHandler,
	}

	server := &Server{
		name:      name,
		cfg:       cfg,
		scheduler: scheduler,
		stop:      make(chan bool),
		sessions:  make(map[string]*session.Session),
	}

	/* Add access if needed */
	if cfg.Access != nil {
		access, err := access.NewAccess(cfg.Access)
		if err != nil {
			return nil, fmt.Errorf("Could not initialize access restrictions: %v", err)
		}
		server.access = access
	}

	log.Info("Creating UDP server '", name, "': ", cfg.Bind, " ", cfg.Balance, " ", cfg.Discovery.Kind, " ", cfg.Healthcheck.Kind)
	return server, nil
}

/**
 * Returns current server configuration
 */
func (this *Server) Cfg() config.Server {
	return this.cfg
}

/**
 * Starts server
 */
func (this *Server) Start() error {

	// Start listening
	if err := this.listen(); err != nil {
		return fmt.Errorf("Could not start listening UDP: %v", err)
	}

	this.scheduler.StatsHandler.Start()
	this.scheduler.Start()

	go func() {

		ticker := time.NewTicker(CLEANUP_EVERY)

		for {
			select {
			case <-ticker.C:
				this.cleanup()
				/* handle server stop */
			case <-this.stop:
				log.Info("Stopping ", this.name)
				atomic.StoreUint32(&this.stopped, 1)

				ticker.Stop()

				this.scheduler.StatsHandler.Stop()
				this.scheduler.Stop()

				this.mu.Lock()
				for k, s := range this.sessions {
					delete(this.sessions, k)
					s.Close()
				}
				this.mu.Unlock()

				return
			}
		}
	}()

	return nil
}

/**
 * Start accepting connections
 */
func (this *Server) listen() error {

	for i := 0;  i <= runtime.NumCPU(); i++ {
		go func() {
			listenAddr := fmt.Sprintf("udp://%s", this.cfg.Bind)
			err := gnet.Serve(this, listenAddr, gnet.WithMulticore(false), gnet.WithReusePort(true))
			log.Errorf("Failed to create listening udp socket: %v", err)
			return
		}()
	}

	return nil
}

/**
 * Safely remove connections that have marked themself as done
 */
func (this *Server) cleanup() {
	this.mu.Lock()
	defer this.mu.Unlock()

	for k, s := range this.sessions {
		if s.IsDone() {
			delete(this.sessions, k)
		}

	}

	this.scheduler.StatsHandler.Connections <- uint(len(this.sessions))
}

func (this *Server) OnInitComplete(srv gnet.Server) (action gnet.Action) {
	log.Debugf("Server is listening on %s (multi-cores: %t, loops: %d)", srv.Addr.String(), srv.Multicore, srv.NumLoops)
	return
}

func (this *Server) React(c gnet.Conn) (out []byte, action gnet.Action) {
	data := append([]byte{}, c.Read()...)
	c.ResetBuffer()

	this.proxy(c, data)
	return
}

/**
 * Elect and connect to backend
 */
func (this *Server) elect(clientAddr *net.UDPAddr) (*core.Backend, error) {
	backend, err := this.scheduler.TakeBackend(core.UdpContext{
		ClientAddr: *clientAddr,
	})

	if err != nil {
		return nil, fmt.Errorf("Could not elect backend for clientAddr %v: %v", clientAddr, err)
	}

	return backend, nil
}

/**
 * Get or create session
 */
func (this *Server) getOrCreateSession(clientAddr *net.UDPAddr, serverConn gnet.Conn) (*session.Session, error) {
	key := clientAddr.String()

	this.mu.Lock()
	defer this.mu.Unlock()

	s, ok := this.sessions[key]

	//session exists and is not done yet
	if ok && !s.IsDone() {
		return s, nil
	}

	//session exists but should be replaced with a new one
	if ok {
		go func() { s.Close() }()
	}

	backend, err := this.elect(clientAddr)
	if err != nil {
		return nil, fmt.Errorf("Could not elect to backend: %v", err)
	}

	cfg := session.Config{
		MaxRequests:        this.cfg.Udp.MaxRequests,
		MaxResponses:       this.cfg.Udp.MaxResponses,
		ClientIdleTimeout:  utils.ParseDurationOrDefault(*this.cfg.ClientIdleTimeout, 0),
		BackendIdleTimeout: utils.ParseDurationOrDefault(*this.cfg.BackendIdleTimeout, 0),
		Transparent:        this.cfg.Udp.Transparent,
	}

	s = session.NewSession(clientAddr, *backend, this.scheduler, cfg, serverConn)

	log.Debugf("%s -> %s -> %s:%s", serverConn.RemoteAddr(), serverConn.LocalAddr(), backend.Host, backend.Port)

	this.sessions[key] = s

	this.scheduler.StatsHandler.Connections <- uint(len(this.sessions))

	return s, nil
}

/**
 * Get the session and send data via chosen session
 */
func (this *Server) proxy(serverConn gnet.Conn, buf []byte) {

	clientAddr, ok := serverConn.RemoteAddr().(*net.UDPAddr)
	if !ok {
		log.Errorf("Not an UDP Addr")
		return
	}

	s, err := this.getOrCreateSession(clientAddr, serverConn)
	if err != nil {
		log.Error(err)
		return
	}

	err = s.Write(buf)
	if err != nil {
		log.Errorf("Could not write data to UDP 'session' %v: %v", s, err)
		return
	}

}

/**
 * Stop, dropping all connections
 */
func (this *Server) Stop() {
	this.stop <- true
}
