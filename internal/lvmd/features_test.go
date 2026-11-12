package lvmd

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/topolvm/topolvm/pkg/lvmd/proto"
	"google.golang.org/grpc"
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

// serveLVService serves svc on the Unix socket at sock until the returned
// function stops it, as one lvmd process would.
func serveLVService(t *testing.T, sock string, svc proto.LVServiceServer, opts ...grpc.ServerOption) func() {
	t.Helper()
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(opts...)
	proto.RegisterLVServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	var once sync.Once
	stop := func() { once.Do(srv.Stop) }
	t.Cleanup(stop)
	return stop
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
	serveLVService(t, sock, svc)
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
	serveLVService(t, sock, svc, grpc.UnaryInterceptor(FeaturesUnaryInterceptor))
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
// rollback of lvmd would, and keeps the workers busy against next.
func replaceLVMD(t *testing.T, nextReserves bool) *slowLVService {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "lvmd.sock")
	first := &slowLVService{}
	stopFirst := serveLVService(t, sock, first, grpc.UnaryInterceptor(FeaturesUnaryInterceptor))
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

	stopFirst()
	next := &slowLVService{}
	var opts []grpc.ServerOption
	if nextReserves {
		opts = append(opts, grpc.UnaryInterceptor(FeaturesUnaryInterceptor))
	}
	serveLVService(t, sock, next, opts...)
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
	next := replaceLVMD(t, false)
	if peak := next.peak.Load(); peak != 1 {
		t.Errorf("%d capacity changes ran at once against the older lvmd that replaced one that reserves capacity, want 1", peak)
	}
}

func TestCapacityGuardAfterRestartOfThisLVMD(t *testing.T) {
	next := replaceLVMD(t, true)
	if peak := next.peak.Load(); peak < 2 {
		t.Errorf("capacity changes never overlapped again after lvmd restarted (peak %d)", peak)
	}
}
