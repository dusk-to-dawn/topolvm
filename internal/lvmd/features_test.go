package lvmd

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/topolvm/topolvm/pkg/lvmd/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// slowLVService records how many CreateLV calls run at once.
type slowLVService struct {
	proto.UnimplementedLVServiceServer
	running, peak, calls atomic.Int32
}

func (s *slowLVService) CreateLV(ctx context.Context, req *proto.CreateLVRequest) (*proto.CreateLVResponse, error) {
	s.calls.Add(1)
	n := s.running.Add(1)
	defer s.running.Add(-1)
	for {
		p := s.peak.Load()
		if n <= p || s.peak.CompareAndSwap(p, n) {
			break
		}
	}
	time.Sleep(50 * time.Millisecond)
	return &proto.CreateLVResponse{Volume: &proto.LogicalVolume{Name: req.GetName()}}, nil
}

// startLVService serves svc on the Unix socket at sock until the test ends or
// the returned server stops, as one lvmd process would. It waits for the
// socket path to be free, as a replacement lvmd would.
func startLVService(t *testing.T, sock string, svc proto.LVServiceServer, opts ...grpc.ServerOption) *grpc.Server {
	t.Helper()
	var lis net.Listener
	var err error
	deadline := time.Now().Add(5 * time.Second)
	for {
		if lis, err = net.Listen("unix", sock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	srv := grpc.NewServer(opts...)
	proto.RegisterLVServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return srv
}

func guardedClient(t *testing.T, sock string) proto.LVServiceClient {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return NewCapacityGuardedLVServiceClient(ctx, sock)
}

func createConcurrently(t *testing.T, c proto.LVServiceClient, n int) {
	t.Helper()
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			if _, err := c.CreateLV(context.Background(), &proto.CreateLVRequest{Name: "x"}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}

func TestCapacityGuardSerialisesAgainstOlderLVMD(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "lvmd.sock")
	svc := &slowLVService{}
	// An older lvmd does not send the features header.
	startLVService(t, sock, svc)
	c := guardedClient(t, sock)
	createConcurrently(t, c, 4)
	createConcurrently(t, c, 4)
	if peak := svc.peak.Load(); peak != 1 {
		t.Errorf("%d CreateLV calls ran at once against an lvmd without capacity reservations, want 1", peak)
	}
}

func TestCapacityGuardAllowsConcurrencyWithThisLVMD(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "lvmd.sock")
	svc := &slowLVService{}
	startLVService(t, sock, svc, grpc.UnaryInterceptor(FeaturesUnaryInterceptor))
	c := guardedClient(t, sock)
	// The first calls run one at a time until a response shows the feature.
	createConcurrently(t, c, 1)
	createConcurrently(t, c, 4)
	if peak := svc.peak.Load(); peak < 2 {
		t.Errorf("CreateLV calls never overlapped against an lvmd that reserves capacity (peak %d)", peak)
	}
}

// replaceLVMD runs 8 workers that keep creating volumes through one guarded
// client, waits until they overlap against an lvmd that reserves capacity,
// then replaces that lvmd on the same socket with next, as a restart or a
// rollback of lvmd would, and keeps the workers busy against next. lvmd
// itself stops gracefully, letting calls in flight finish; a hard stop is
// what a killed lvmd does.
func replaceLVMD(t *testing.T, graceful, nextReserves bool) *slowLVService {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "lvmd.sock")
	first := &slowLVService{}
	firstSrv := startLVService(t, sock, first, grpc.UnaryInterceptor(FeaturesUnaryInterceptor))
	c := guardedClient(t, sock)

	ctx, stopWorkers := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for ctx.Err() == nil {
				if _, err := c.CreateLV(ctx, &proto.CreateLVRequest{Name: "x"}); err != nil {
					time.Sleep(10 * time.Millisecond)
				}
			}
		})
	}
	deadline := time.Now().Add(10 * time.Second)
	for first.peak.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the workers never overlapped against an lvmd that reserves capacity")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if graceful {
		go firstSrv.GracefulStop()
	} else {
		firstSrv.Stop()
	}
	next := &slowLVService{}
	var opts []grpc.ServerOption
	if nextReserves {
		opts = append(opts, grpc.UnaryInterceptor(FeaturesUnaryInterceptor))
	}
	startLVService(t, sock, next, opts...)
	deadline = time.Now().Add(15 * time.Second)
	for next.calls.Load() < 20 {
		if time.Now().After(deadline) {
			t.Fatalf("only %d calls reached the replacement lvmd", next.calls.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopWorkers()
	wg.Wait()
	return next
}

func TestCapacityGuardAfterRollbackToOlderLVMD(t *testing.T) {
	for _, graceful := range []bool{true, false} {
		t.Run(stopName(graceful), func(t *testing.T) {
			next := replaceLVMD(t, graceful, false)
			if peak := next.peak.Load(); peak != 1 {
				t.Errorf("%d capacity changes ran at once against the older lvmd that replaced one that reserves capacity, want 1", peak)
			}
		})
	}
}

func TestCapacityGuardAfterRestartOfThisLVMD(t *testing.T) {
	for _, graceful := range []bool{true, false} {
		t.Run(stopName(graceful), func(t *testing.T) {
			next := replaceLVMD(t, graceful, true)
			if peak := next.peak.Load(); peak < 2 {
				t.Errorf("capacity changes never overlapped again after lvmd restarted (peak %d)", peak)
			}
		})
	}
}

func stopName(graceful bool) string {
	if graceful {
		return "graceful stop"
	}
	return "hard stop"
}

// gatedLVService holds each CreateLV for the volume named gated until
// released (one per value sent on release, or all once it is closed), or
// until its caller gives up, and reports when such a call has arrived. It
// answers calls for other volumes at once.
type gatedLVService struct {
	proto.UnimplementedLVServiceServer
	gated   string
	arrived chan struct{}
	release chan struct{}
}

func newGatedLVService(gated string) *gatedLVService {
	return &gatedLVService{gated: gated, arrived: make(chan struct{}, 8), release: make(chan struct{})}
}

func (s *gatedLVService) CreateLV(ctx context.Context, req *proto.CreateLVRequest) (*proto.CreateLVResponse, error) {
	if req.GetName() == s.gated {
		s.arrived <- struct{}{}
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &proto.CreateLVResponse{Volume: &proto.LogicalVolume{Name: req.GetName()}}, nil
}

// TestCapacityGuardLetsCallsFinishOnGracefulStop stops lvmd gracefully while
// a creation is in flight, as a Pod deletion, a drain or a rollout does. The
// creation must finish, and later calls must go to the replacement lvmd and
// start serial there.
func TestCapacityGuardLetsCallsFinishOnGracefulStop(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "lvmd.sock")
	first := newGatedLVService("in-flight")
	firstSrv := startLVService(t, sock, first, grpc.UnaryInterceptor(FeaturesUnaryInterceptor))
	c := guardedClient(t, sock)

	// A first creation shows that this lvmd reserves capacity, so the next
	// one runs on the concurrent path and does not hold the serial lock.
	if _, err := c.CreateLV(context.Background(), &proto.CreateLVRequest{Name: "first"}); err != nil {
		t.Fatal(err)
	}

	errc := make(chan error, 1)
	go func() {
		_, err := c.CreateLV(context.Background(), &proto.CreateLVRequest{Name: "in-flight"})
		errc <- err
	}()
	<-first.arrived
	stopped := make(chan struct{})
	go func() {
		firstSrv.GracefulStop()
		close(stopped)
	}()

	// The replacement comes up while the first lvmd drains. It is older:
	// it does not reserve capacity.
	next := &slowLVService{}
	startLVService(t, sock, next)
	// Calls sent before the client has seen the stop may still be served by
	// the draining lvmd, or fail with Unavailable, as with any gRPC client.
	// Workers keep calling until the replacement has served 8 of them.
	const want = 8
	deadline := time.Now().Add(5 * time.Second)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for next.calls.Load() < want && time.Now().Before(deadline) {
				_, err := c.CreateLV(context.Background(), &proto.CreateLVRequest{Name: "x"})
				if err == nil {
					continue
				}
				if status.Code(err) != codes.Unavailable {
					t.Error(err)
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
	wg.Wait()
	if n := next.calls.Load(); n < want {
		t.Errorf("only %d calls reached the replacement, want %d", n, want)
	}
	if peak := next.peak.Load(); peak != 1 {
		t.Errorf("%d capacity changes ran at once against the replacement, want 1", peak)
	}

	close(first.release)
	select {
	case err := <-errc:
		if err != nil {
			t.Errorf("the creation in flight during the graceful stop failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the creation in flight during the graceful stop never returned")
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the first lvmd never finished its graceful stop")
	}
}

// TestCapacityGuardBoundsCallsOnRetiredConnection checks that a call that
// never returns on a retired connection is ended after the drain timeout.
func TestCapacityGuardBoundsCallsOnRetiredConnection(t *testing.T) {
	timeout := retiredConnectionDrainTimeout
	retiredConnectionDrainTimeout = 300 * time.Millisecond
	t.Cleanup(func() { retiredConnectionDrainTimeout = timeout })

	sock := filepath.Join(t.TempDir(), "lvmd.sock")
	hung := newGatedLVService("hung")
	hungSrv := startLVService(t, sock, hung, grpc.UnaryInterceptor(FeaturesUnaryInterceptor))
	c := guardedClient(t, sock)

	errc := make(chan error, 1)
	go func() {
		_, err := c.CreateLV(context.Background(), &proto.CreateLVRequest{Name: "hung"})
		errc <- err
	}()
	<-hung.arrived
	// The graceful stop waits for the hung call for ever; the client retires
	// the connection when lvmd starts draining it.
	go hungSrv.GracefulStop()
	start := time.Now()
	select {
	case err := <-errc:
		if err == nil {
			t.Error("the hung call succeeded")
		}
		if d := time.Since(start); d > 3*time.Second {
			t.Errorf("the hung call ended after %v, want about the drain timeout", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the hung call on the retired connection never ended")
	}
	close(hung.release)
}

// slowFirstCallLVService answers CreateLV after 700 ms, or when its caller
// gives up, and reports when a call has started.
type slowFirstCallLVService struct {
	proto.UnimplementedLVServiceServer
	started chan struct{}
}

func (s *slowFirstCallLVService) CreateLV(ctx context.Context, req *proto.CreateLVRequest) (*proto.CreateLVResponse, error) {
	close(s.started)
	select {
	case <-time.After(700 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &proto.CreateLVResponse{Volume: &proto.LogicalVolume{Name: req.GetName()}}, nil
}

// TestGracefulStopLetsFirstCallFinish stops lvmd gracefully while the very
// first creation, which runs serially, is in flight: like a plain gRPC
// client, the guarded client must let it finish.
func TestGracefulStopLetsFirstCallFinish(t *testing.T) {
	for _, guarded := range []bool{false, true} {
		t.Run(fmt.Sprintf("guarded client: %v", guarded), func(t *testing.T) {
			sock := filepath.Join(t.TempDir(), "lvmd.sock")
			svc := &slowFirstCallLVService{started: make(chan struct{})}
			srv := startLVService(t, sock, svc, grpc.UnaryInterceptor(FeaturesUnaryInterceptor))

			var c proto.LVServiceClient
			if guarded {
				c = guardedClient(t, sock)
			} else {
				conn, err := grpc.NewClient("unix:"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = conn.Close() })
				c = proto.NewLVServiceClient(conn)
			}
			errc := make(chan error, 1)
			go func() {
				_, err := c.CreateLV(context.Background(), &proto.CreateLVRequest{Name: "x"})
				errc <- err
			}()
			<-svc.started
			go srv.GracefulStop()
			select {
			case err := <-errc:
				if err != nil {
					t.Errorf("the call in flight failed during the graceful stop: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the call did not return")
			}
		})
	}
}

// TestCapacityGuardUnderFlappingLVMD restarts lvmd 30 times on one socket
// while 8 workers keep creating volumes, alternating an lvmd that reserves
// capacity with one that does not, and hard with graceful stops. No two
// capacity changes may ever overlap against an lvmd that does not reserve,
// and no goroutine may be left behind.
func TestCapacityGuardUnderFlappingLVMD(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "lvmd.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewCapacityGuardedLVServiceClient(ctx, sock)

	base := runtime.NumGoroutine()
	var olderPeak int32
	stopWorkers := make(chan struct{})
	var wg sync.WaitGroup
	defer func() {
		select {
		case <-stopWorkers:
		default:
			close(stopWorkers)
		}
		wg.Wait()
	}()
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stopWorkers:
					return
				default:
				}
				cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, _ = c.CreateLV(cctx, &proto.CreateLVRequest{Name: "x"})
				ccancel()
				time.Sleep(2 * time.Millisecond)
			}
		})
	}
	for i := range 30 {
		svc := &slowLVService{}
		var opts []grpc.ServerOption
		reserving := i%2 == 0
		if reserving {
			opts = append(opts, grpc.UnaryInterceptor(FeaturesUnaryInterceptor))
		}
		srv := startLVService(t, sock, svc, opts...)
		time.Sleep(150 * time.Millisecond)
		if i%3 == 0 {
			srv.Stop()
		} else {
			srv.GracefulStop()
		}
		if !reserving {
			olderPeak = max(olderPeak, svc.peak.Load())
		}
	}
	close(stopWorkers)
	wg.Wait()
	if olderPeak > 1 {
		t.Errorf("%d capacity changes ran at once against an lvmd without reservations", olderPeak)
	}

	cancel()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > base+5 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > base+5 {
		buf := make([]byte, 1<<16)
		k := runtime.Stack(buf, true)
		t.Errorf("goroutines: %d after, %d before; stacks:\n%s", n, base, buf[:k])
	}
}
