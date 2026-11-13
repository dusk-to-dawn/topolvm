package lvmd

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/topolvm/topolvm/pkg/lvmd/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// FeaturesMetadataKey is the gRPC response header in which lvmd lists
	// the features its callers may rely on.
	FeaturesMetadataKey = "topolvm-lvmd-features"
	// FeatureCapacityReservations means that lvmd checks free capacity
	// correctly when it serves several capacity changes at once.
	FeatureCapacityReservations = "capacity-reservations"
)

// FeaturesUnaryInterceptor adds lvmd's features to the response headers of
// every unary call, so that callers can tell this lvmd from an older one
// without a change to the protocol.
func FeaturesUnaryInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	// An error means the call did not come through a gRPC server (for example
	// an embedded lvmd); there is nobody to tell then.
	_ = grpc.SetHeader(ctx, metadata.Pairs(FeaturesMetadataKey, FeatureCapacityReservations))
	return handler(ctx, req)
}

// NewCapacityGuardedLVServiceClient returns a client of the lvmd listening on
// socketPath, which may be older than this topolvm-node, for example while the
// two DaemonSets roll or after lvmd is rolled back. An older lvmd checks free
// capacity without accounting for concurrent changes, so the calls that check
// and change capacity (CreateLV, ResizeLV and CreateLVSnapshot) are made one
// at a time until a successful response from the lvmd process at the other
// end shows that it reserves capacity. Other calls are passed through.
//
// What is known about lvmd is tied to one lvmd process: each connection the
// client makes talks to the first lvmd it reaches and never reconnects, so a
// call made on it cannot reach a replacement. When the connection leaves the
// READY state, which with idleness disabled happens only when lvmd closes or
// drains it, or a call on it fails with Unavailable, the client retires it:
// later calls use a new connection, on which capacity changes are serial
// again until a response shows the feature. Calls still in flight on a
// retired connection may finish, as lvmd's graceful stop lets them; the
// connection is closed once they have, or after retiredConnectionDrainTimeout.
// Connections are closed when ctx ends.
func NewCapacityGuardedLVServiceClient(ctx context.Context, socketPath string) proto.LVServiceClient {
	g := &capacityGuardedLVServiceClient{ctx: ctx, socketPath: socketPath}
	go func() {
		<-ctx.Done()
		g.mu.Lock()
		c := g.cur
		g.cur = nil
		g.mu.Unlock()
		if c != nil {
			c.close()
		}
	}()
	return g
}

var errLVMDReplaced = errors.New("lvmd connection is over: a new connection is used for the next call")

// retiredConnectionDrainTimeout bounds how long a retired connection is kept
// open for the calls still in flight on it.
var retiredConnectionDrainTimeout = 5 * time.Minute

type capacityGuardedLVServiceClient struct {
	ctx        context.Context
	socketPath string
	// serial is held around each guarded call while the connection's lvmd
	// is not known to reserve capacity.
	serial sync.Mutex
	// mu protects cur, and makes taking a connection for a call atomic with
	// retiring it.
	mu  sync.Mutex
	cur *lvmdConnection
}

// lvmdConnection is a connection to one lvmd process.
type lvmdConnection struct {
	conn   *grpc.ClientConn
	client proto.LVServiceClient
	// connected is set once the connection has reached an lvmd; its dialer
	// refuses to connect again after that.
	connected atomic.Bool
	// reserves is whether the last successful response came from an lvmd
	// that reserves capacity.
	reserves atomic.Bool

	// mu protects inflight, retired and drainTimer.
	mu         sync.Mutex
	inflight   int
	retired    bool
	drainTimer *time.Timer
	closed     sync.Once
}

func (c *lvmdConnection) close() {
	c.closed.Do(func() { _ = c.conn.Close() })
}

// acquire returns the connection for a call, making one if there is none,
// and counts the call on it. Call release when the call has returned.
func (g *capacityGuardedLVServiceClient) acquire() (*lvmdConnection, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ctx.Err() != nil {
		return nil, g.ctx.Err()
	}
	if g.cur == nil {
		c, err := g.newConnection()
		if err != nil {
			return nil, err
		}
		g.cur = c
	}
	// Counted under g.mu: a connection is retired only after it has been
	// taken out of g.cur under g.mu, so every call that got it is counted
	// before its retirement looks at the count.
	g.cur.mu.Lock()
	g.cur.inflight++
	g.cur.mu.Unlock()
	return g.cur, nil
}

// release ends a call counted by acquire, and closes the connection if it is
// retired and this was its last call.
func (g *capacityGuardedLVServiceClient) release(c *lvmdConnection) {
	c.mu.Lock()
	c.inflight--
	last := c.retired && c.inflight == 0
	if last && c.drainTimer != nil {
		c.drainTimer.Stop()
	}
	c.mu.Unlock()
	if last {
		c.close()
	}
}

func (g *capacityGuardedLVServiceClient) newConnection() (*lvmdConnection, error) {
	c := &lvmdConnection{}
	dial := func(ctx context.Context, _ string) (net.Conn, error) {
		if c.connected.Load() {
			return nil, errLVMDReplaced
		}
		nc, err := (&net.Dialer{}).DialContext(ctx, "unix", g.socketPath)
		if err == nil {
			c.connected.Store(true)
		}
		return nc, err
	}
	conn, err := grpc.NewClient("passthrough:///"+g.socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dial),
		// Without idleness, the connection leaves READY only when its
		// transport is lost, so an idle period does not retire it.
		grpc.WithIdleTimeout(0))
	if err != nil {
		return nil, err
	}
	c.conn = conn
	c.client = proto.NewLVServiceClient(conn)
	go g.watch(c)
	return c, nil
}

// watch retires c once it leaves the READY state, that is once the lvmd it
// reached has closed or is draining the connection.
func (g *capacityGuardedLVServiceClient) watch(c *lvmdConnection) {
	state := c.conn.GetState()
	for state != connectivity.Ready {
		if state == connectivity.Shutdown || !c.conn.WaitForStateChange(g.ctx, state) {
			return
		}
		state = c.conn.GetState()
	}
	if c.conn.WaitForStateChange(g.ctx, connectivity.Ready) {
		g.retire(c)
	}
}

// retire stops giving c to new calls. Calls in flight on it may finish; it is
// closed when the last of them returns, or after the drain timeout.
func (g *capacityGuardedLVServiceClient) retire(c *lvmdConnection) {
	c.reserves.Store(false)
	g.mu.Lock()
	if g.cur == c {
		g.cur = nil
	}
	g.mu.Unlock()

	c.mu.Lock()
	if c.retired {
		c.mu.Unlock()
		return
	}
	c.retired = true
	idle := c.inflight == 0
	if !idle {
		c.drainTimer = time.AfterFunc(retiredConnectionDrainTimeout, c.close)
	}
	c.mu.Unlock()
	if idle {
		c.close()
	}
}

func guardedCall[Resp any](ctx context.Context, g *capacityGuardedLVServiceClient, opts []grpc.CallOption,
	call func(client proto.LVServiceClient, opts ...grpc.CallOption) (Resp, error)) (Resp, error) {
	var zero Resp
	c, err := g.acquire()
	if err != nil {
		return zero, status.Error(codes.Unavailable, err.Error())
	}
	// Only a call on a connection whose lvmd has shown that it reserves
	// capacity runs concurrently with others. Any other call waits for the
	// others to finish, and then uses whichever connection is current.
	if !c.reserves.Load() {
		g.release(c)
		g.serial.Lock()
		defer g.serial.Unlock()
		if c, err = g.acquire(); err != nil {
			return zero, status.Error(codes.Unavailable, err.Error())
		}
	}
	defer g.release(c)

	var header metadata.MD
	resp, err := call(c.client, append(slices.Clone(opts), grpc.Header(&header))...)
	if err != nil {
		// An error response may not carry the headers. Unavailable means the
		// lvmd process may be gone, and its successor may be older.
		if status.Code(err) == codes.Unavailable && c.connected.Load() {
			g.retire(c)
		}
		return resp, err
	}
	reserves := slices.Contains(header.Get(FeaturesMetadataKey), FeatureCapacityReservations)
	if c.reserves.Swap(reserves) != reserves {
		if reserves {
			log.FromContext(ctx).Info("lvmd reserves capacity; capacity changes may run concurrently")
		} else {
			log.FromContext(ctx).Info("lvmd does not reserve capacity; capacity changes run one at a time")
		}
	}
	return resp, nil
}

func (g *capacityGuardedLVServiceClient) CreateLV(ctx context.Context, in *proto.CreateLVRequest, opts ...grpc.CallOption) (*proto.CreateLVResponse, error) {
	return guardedCall(ctx, g, opts, func(client proto.LVServiceClient, opts ...grpc.CallOption) (*proto.CreateLVResponse, error) {
		return client.CreateLV(ctx, in, opts...)
	})
}

func (g *capacityGuardedLVServiceClient) ResizeLV(ctx context.Context, in *proto.ResizeLVRequest, opts ...grpc.CallOption) (*proto.ResizeLVResponse, error) {
	return guardedCall(ctx, g, opts, func(client proto.LVServiceClient, opts ...grpc.CallOption) (*proto.ResizeLVResponse, error) {
		return client.ResizeLV(ctx, in, opts...)
	})
}

func (g *capacityGuardedLVServiceClient) CreateLVSnapshot(ctx context.Context, in *proto.CreateLVSnapshotRequest, opts ...grpc.CallOption) (*proto.CreateLVSnapshotResponse, error) {
	return guardedCall(ctx, g, opts, func(client proto.LVServiceClient, opts ...grpc.CallOption) (*proto.CreateLVSnapshotResponse, error) {
		return client.CreateLVSnapshot(ctx, in, opts...)
	})
}

func (g *capacityGuardedLVServiceClient) RemoveLV(ctx context.Context, in *proto.RemoveLVRequest, opts ...grpc.CallOption) (*proto.Empty, error) {
	c, err := g.acquire()
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	defer g.release(c)
	resp, err := c.client.RemoveLV(ctx, in, opts...)
	if status.Code(err) == codes.Unavailable && c.connected.Load() {
		g.retire(c)
	}
	return resp, err
}
