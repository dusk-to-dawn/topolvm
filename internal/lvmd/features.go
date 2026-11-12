package lvmd

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/topolvm/topolvm/pkg/lvmd/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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

// NewCapacityGuardedLVServiceClient wraps a client of an lvmd reached over its
// socket, which may be older than this topolvm-node, for example while the two
// DaemonSets roll. An older lvmd checks free capacity without accounting for
// concurrent changes, so the calls that check and change capacity (CreateLV,
// ResizeLV and CreateLVSnapshot) are made one at a time until lvmd's response
// headers show that it reserves capacity, and again whenever a response does
// not show it. Other calls are passed through.
func NewCapacityGuardedLVServiceClient(c proto.LVServiceClient) proto.LVServiceClient {
	return &capacityGuardedLVServiceClient{LVServiceClient: c}
}

type capacityGuardedLVServiceClient struct {
	proto.LVServiceClient
	// reserves is whether the last successful response came from an lvmd that
	// reserves capacity.
	reserves atomic.Bool
	// serial is held around each guarded call while reserves is false.
	serial sync.Mutex
}

func guardedCall[Resp any](ctx context.Context, g *capacityGuardedLVServiceClient, opts []grpc.CallOption,
	call func(opts ...grpc.CallOption) (Resp, error)) (Resp, error) {
	if !g.reserves.Load() {
		g.serial.Lock()
		defer g.serial.Unlock()
	}
	var header metadata.MD
	resp, err := call(append(slices.Clone(opts), grpc.Header(&header))...)
	if err != nil {
		// An error response may not carry the headers; keep what is known.
		return resp, err
	}
	reserves := slices.Contains(header.Get(FeaturesMetadataKey), FeatureCapacityReservations)
	if g.reserves.Swap(reserves) != reserves {
		if reserves {
			log.FromContext(ctx).Info("lvmd reserves capacity; capacity changes may run concurrently")
		} else {
			log.FromContext(ctx).Info("lvmd does not reserve capacity; capacity changes run one at a time")
		}
	}
	return resp, nil
}

func (g *capacityGuardedLVServiceClient) CreateLV(ctx context.Context, in *proto.CreateLVRequest, opts ...grpc.CallOption) (*proto.CreateLVResponse, error) {
	return guardedCall(ctx, g, opts, func(opts ...grpc.CallOption) (*proto.CreateLVResponse, error) {
		return g.LVServiceClient.CreateLV(ctx, in, opts...)
	})
}

func (g *capacityGuardedLVServiceClient) ResizeLV(ctx context.Context, in *proto.ResizeLVRequest, opts ...grpc.CallOption) (*proto.ResizeLVResponse, error) {
	return guardedCall(ctx, g, opts, func(opts ...grpc.CallOption) (*proto.ResizeLVResponse, error) {
		return g.LVServiceClient.ResizeLV(ctx, in, opts...)
	})
}

func (g *capacityGuardedLVServiceClient) CreateLVSnapshot(ctx context.Context, in *proto.CreateLVSnapshotRequest, opts ...grpc.CallOption) (*proto.CreateLVSnapshotResponse, error) {
	return guardedCall(ctx, g, opts, func(opts ...grpc.CallOption) (*proto.CreateLVSnapshotResponse, error) {
		return g.LVServiceClient.CreateLVSnapshot(ctx, in, opts...)
	})
}
