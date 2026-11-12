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
	"google.golang.org/grpc/credentials/insecure"
)

// slowLVService records how many CreateLV calls run at once.
type slowLVService struct {
	proto.UnimplementedLVServiceServer
	running, peak atomic.Int32
}

func (s *slowLVService) CreateLV(ctx context.Context, req *proto.CreateLVRequest) (*proto.CreateLVResponse, error) {
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

func serveLVService(t *testing.T, svc proto.LVServiceServer, opts ...grpc.ServerOption) proto.LVServiceClient {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "lvmd.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(opts...)
	proto.RegisterLVServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("unix:"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return proto.NewLVServiceClient(conn)
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
	svc := &slowLVService{}
	// An older lvmd does not send the features header.
	c := NewCapacityGuardedLVServiceClient(serveLVService(t, svc))
	createConcurrently(t, c, 4)
	createConcurrently(t, c, 4)
	if peak := svc.peak.Load(); peak != 1 {
		t.Errorf("%d CreateLV calls ran at once against an lvmd without capacity reservations, want 1", peak)
	}
}

func TestCapacityGuardAllowsConcurrencyWithThisLVMD(t *testing.T) {
	svc := &slowLVService{}
	c := NewCapacityGuardedLVServiceClient(serveLVService(t, svc, grpc.UnaryInterceptor(FeaturesUnaryInterceptor)))
	// The first calls run one at a time until a response shows the feature.
	createConcurrently(t, c, 1)
	createConcurrently(t, c, 4)
	if peak := svc.peak.Load(); peak < 2 {
		t.Errorf("CreateLV calls never overlapped against an lvmd that reserves capacity (peak %d)", peak)
	}
}
