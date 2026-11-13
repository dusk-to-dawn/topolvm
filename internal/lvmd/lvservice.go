package lvmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/topolvm/topolvm/internal/lvmd/command"
	"github.com/topolvm/topolvm/pkg/lvmd/proto"
	lvmdTypes "github.com/topolvm/topolvm/pkg/lvmd/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// snapshotCleanupTimeout bounds the removal of an incomplete snapshot, which
// runs even when the call that created it has been cancelled.
var snapshotCleanupTimeout = 2 * time.Minute

// incompleteSnapshotCleanupFailures counts incomplete snapshots that could not
// be removed after a failed CreateLVSnapshot.
var incompleteSnapshotCleanupFailures = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "topolvm",
	Subsystem: "lvmd",
	Name:      "incomplete_snapshot_cleanup_failures_total",
	Help:      "Incomplete snapshot volumes that lvmd failed to remove after a failed CreateLVSnapshot.",
})

func init() {
	prometheus.MustRegister(incompleteSnapshotCleanupFailures)
}

// RegisterMetrics registers lvmd's metrics with reg too. lvmd serves the
// default registry; topolvm-node, which serves a registry of its own, calls
// this when it embeds lvmd.
func RegisterMetrics(reg prometheus.Registerer) error {
	err := reg.Register(incompleteSnapshotCleanupFailures)
	if errors.As(err, &prometheus.AlreadyRegisteredError{}) {
		return nil
	}
	return err
}

// NewLVService creates a new LVServiceServer
func NewLVService(dcmapper *DeviceClassManager, ocmapper *LvcreateOptionClassManager, notifyFunc func()) proto.LVServiceServer {
	return &lvService{
		dcmapper:     dcmapper,
		ocmapper:     ocmapper,
		notifyFunc:   notifyFunc,
		reservations: newReservations(),
	}
}

type lvService struct {
	proto.UnimplementedLVServiceServer
	dcmapper   *DeviceClassManager
	ocmapper   *LvcreateOptionClassManager
	notifyFunc func()
	// reservations keeps concurrent calls from overcommitting capacity.
	reservations *reservations
}

func (s *lvService) notify() {
	if s.notifyFunc != nil {
		s.notifyFunc()
	}
}

// reportedSizes returns the sizes of the volumes in a report, for a capacity check.
func reportedSizes(vols map[string]*command.LogicalVolume) reportedSize {
	return func(name string) (uint64, bool) {
		if v, ok := vols[name]; ok {
			return v.Size(), true
		}
		return 0, false
	}
}

func (s *lvService) CreateLV(ctx context.Context, req *proto.CreateLVRequest) (*proto.CreateLVResponse, error) {
	logger := log.FromContext(ctx).WithValues("name", req.GetName())

	dc, err := s.dcmapper.DeviceClass(req.DeviceClass)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: %s", err.Error(), req.DeviceClass)
	}
	capacityCheck := s.reservations.begin(reservationKey(dc))
	defer capacityCheck.close()
	pool, err := storagePoolForDeviceClass(ctx, dc)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get pool from device class: %v", err)
	}
	oc := s.ocmapper.LvcreateOptionClass(req.LvcreateOptionClass)

	// The volume can exist already, for example when the caller stopped after
	// creating it but before recording that. Report it as AlreadyExists, from
	// the report taken above, instead of failing in lvcreate, so that callers
	// do not need to list the volumes before every creation.
	existing, err := pool.FindVolume(ctx, req.GetName())
	switch {
	case err == nil && (dc.Type != lvmdTypes.TypeThick || !existing.IsThin()):
		return nil, status.Errorf(codes.AlreadyExists, "logical volume %s already exists", req.GetName())
	case err != nil && !errors.Is(err, command.ErrNotFound):
		return nil, status.Errorf(codes.Internal, "failed to look up volume: %v", err)
	}

	requested := uint64(req.GetSizeBytes())
	free, err := pool.Free(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get free bytes: %v", err)
	}
	vols, err := pool.ListVolumes(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list volumes: %v", err)
	}
	reserved, free := capacityCheck.reserve(free, req.GetName(), requested, reportedSizes(vols))
	if reserved == nil {
		logger.Error(err, "not enough space left on VG", "free", free, "requested", requested)
		return nil, status.Errorf(codes.ResourceExhausted, "no enough space left on VG: free=%d, requested=%d", free, requested)
	}
	defer s.reservations.release(reserved)

	var stripe uint
	var stripeSize string
	var lvcreateOptions []string
	if oc != nil {
		lvcreateOptions = oc.Options
	} else if req.LvcreateOptionClass != "" {
		return nil, status.Error(codes.Internal, fmt.Sprintf("unsupported lvcreate-option-class target: %s", req.LvcreateOptionClass))
	} else {
		stripeSize = dc.StripeSize
		if dc.Stripe != nil {
			stripe = *dc.Stripe
		}
		if dc.LVCreateOptions != nil {
			lvcreateOptions = dc.LVCreateOptions
		}
	}

	err = pool.CreateVolume(ctx, req.GetName(), requested, req.GetTags(), stripe, stripeSize, lvcreateOptions)
	if err != nil {
		logger.Error(err, "failed to create volume",
			"requested", requested,
			"tags", req.GetTags())
		return nil, status.Error(codes.Internal, err.Error())
	}

	// The pool describes the state before the volume was created, so read the
	// new volume from LVM to return its actual size and device numbers.
	lv, err := pool.LookupVolume(ctx, req.GetName())
	if err != nil {
		logger.Error(err, "failed to find volume",
			"requested", requested,
			"tags", req.GetTags())
		return nil, status.Error(codes.Internal, err.Error())
	}

	s.notify()

	logger.Info("created a new LV", "size", requested)

	return &proto.CreateLVResponse{
		Volume: &proto.LogicalVolume{
			Name: lv.Name(),
			// convert to int64 because lvmd internals and lvm use uint64 but CSI uses int64.
			// For most conventional lvm use cases overflow here will never occur (9223372 TB or above cause overflow)
			SizeBytes: int64(lv.Size()),
			DevMajor:  lv.MajorNumber(),
			DevMinor:  lv.MinorNumber(),
		},
	}, nil
}

func (s *lvService) RemoveLV(ctx context.Context, req *proto.RemoveLVRequest) (*proto.Empty, error) {
	logger := log.FromContext(ctx).WithValues("name", req.GetName())

	dc, err := s.dcmapper.DeviceClass(req.DeviceClass)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: %s", err.Error(), req.DeviceClass)
	}

	// lvremove fails with a "not found" error for a missing volume group as
	// well as for a missing volume, so it needs no separate look-up first.
	if err := command.RemoveVolume(ctx, dc.VolumeGroup, req.GetName()); errors.Is(err, command.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "%s: %s", err.Error(), req.DeviceClass)
	} else if err != nil {
		logger.Error(err, "failed to remove volume", "name", req.GetName())
		return nil, err
	}

	s.notify()

	logger.Info("removed a LV", "name", req.GetName())

	return &proto.Empty{}, nil
}

func (s *lvService) CreateLVSnapshot(ctx context.Context, req *proto.CreateLVSnapshotRequest) (*proto.CreateLVSnapshotResponse, error) {
	logger := log.FromContext(ctx).WithValues("name", req.GetName())
	dc, err := s.dcmapper.DeviceClass(req.DeviceClass)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: %s", err.Error(), req.DeviceClass)
	}
	var snapType string
	switch dc.Type {
	case lvmdTypes.TypeThin:
		snapType = "thin-snapshot"
	case lvmdTypes.TypeThick:
		return nil, status.Error(codes.Unimplemented, "device class is not thin. Thick snapshots are not implemented yet")
	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid device class type %v", string(dc.Type))
	}

	capacityCheck := s.reservations.begin(reservationKey(dc))
	defer capacityCheck.close()
	// One report answers the look-ups of the source volume, the pool and its
	// usage below.
	vg, err := command.FindVolumeGroupWithVolumes(ctx, dc.VolumeGroup)
	if err != nil {
		return nil, err
	}

	// Fetch the source logical volume
	sourceVolume := req.GetSourceVolume()
	sourceLV, err := vg.FindVolume(ctx, sourceVolume)
	if errors.Is(err, command.ErrNotFound) {
		logger.Error(err, "source logical volume is not found", "sourceVolume", sourceVolume)
		return nil, status.Errorf(codes.NotFound, "source logical volume %s is not found", sourceVolume)
	}
	if err != nil {
		logger.Error(err, "failed to find source volume", "sourceVolume", sourceVolume)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if !sourceLV.IsThin() {
		return nil, status.Error(codes.Unimplemented, "snapshot can be created for only thin volumes")
	}

	// In case of thin-snapshots, the size is the same as the source volume on snapshot creation, and then
	// gets resized after extension into the correct size
	sizeOnCreation := sourceLV.Size()

	desiredSize := uint64(req.GetSizeBytes())

	// in case there is no desired size in the request, we can still attempt to create the Snapshot with Source size.
	if desiredSize == 0 {
		desiredSize = sizeOnCreation
	}

	if sizeOnCreation > desiredSize {
		return nil, status.Errorf(codes.OutOfRange, "requested size %v is smaller than source logical volume: %v", desiredSize, sizeOnCreation)
	}

	pool, err := vg.FindPool(ctx, dc.ThinPoolConfig.Name)
	if err != nil {
		logger.Error(err, "failed to get thinpool")
		return nil, status.Error(codes.Internal, err.Error())
	}
	poolUsage, err := pool.Usage(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get pool usage: %v", err)
	}
	free, err := poolUsage.FreeBytes(dc.ThinPoolConfig.OverprovisionRatio)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get free bytes: %v", err)
	}
	vols, err := pool.ListVolumes(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list volumes: %v", err)
	}
	// The snapshot is built under a name of its own and renamed to the
	// requested name only once it is complete (see below). Reports taken
	// meanwhile show it under that name, so the reservation uses it too.
	buildName, err := incompleteSnapshotName(req.GetName())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	// Registered before the LV can exist, and until after the clean-up, so
	// that the sweep of leftover builds never takes it for one.
	startBuild(buildName)
	defer endBuild(buildName)
	reserved, free := capacityCheck.reserve(free, buildName, desiredSize, reportedSizes(vols))
	if reserved == nil {
		logger.Error(err, "not enough space left on VG", "free", free, "desiredSize", desiredSize)
		return nil, status.Errorf(codes.ResourceExhausted, "no enough space left on VG: free=%d, desiredSize=%d", free, desiredSize)
	}
	defer s.reservations.release(reserved)

	logger.Info(
		"lvservice req",
		"sizeOnCreation", sizeOnCreation,
		"desiredSize", desiredSize,
		"sourceVol", sourceVolume,
		"snapType", snapType,
		"accessType", req.AccessType,
	)
	// Create snapshot lv

	// A volume of this name that existed before this call is not ours to
	// remove: report it, as CreateLV does.
	if _, err := vg.FindVolume(ctx, req.GetName()); err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "logical volume %s already exists", req.GetName())
	} else if !errors.Is(err, command.ErrNotFound) {
		return nil, status.Errorf(codes.Internal, "failed to look up volume: %v", err)
	}

	// The snapshot is created, read back, resized and activated under a name
	// that only this call uses, and given the requested name by lvrename as
	// the last step. lvrename refuses a name that exists, under the volume
	// group's lock. So the requested name only ever appears on a complete
	// snapshot, which a look-up may adopt, and a failed call removes only the
	// LV it built itself, never one of the requested name that another call
	// for the same volume created (for example the call of a restarted
	// topolvm-node while the cancelled call's lvm still runs).
	//
	// If any step from lvcreate -s on fails, remove that LV, so that an
	// incomplete snapshot (at the wrong size, or writable when it should be
	// read-only) is never left behind; the caller records the failure and
	// retries from scratch. This includes lvcreate -s itself: when the call is
	// cancelled while it runs, the lvm process can still create the LV after
	// lvmd stops waiting. The removal runs under a context of its own, because
	// a cancelled call is the common reason to clean up. An LV left behind by
	// an lvmd that was killed in between keeps its build name, which no
	// LogicalVolume refers to, and is removed when lvmd starts again.
	fail := func(msg string, err error) (*proto.CreateLVSnapshotResponse, error) {
		logger.Error(err, msg, "buildName", buildName)
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), snapshotCleanupTimeout)
		defer cancel()
		rmErr := command.RemoveVolume(cleanupCtx, vg.Name(), buildName)
		switch {
		case rmErr == nil:
			logger.Info("deleted incomplete snapshot volume", "buildName", buildName)
		case errors.Is(rmErr, command.ErrNotFound):
		default:
			incompleteSnapshotCleanupFailures.Inc()
			logger.Error(rmErr, "failed to delete incomplete snapshot volume", "buildName", buildName)
			return nil, status.Errorf(codes.Internal, "%v; removing the incomplete snapshot volume %s also failed: %v", err, buildName, rmErr)
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := sourceLV.ThinSnapshot(ctx, buildName, req.GetTags()); err != nil {
		return fail("failed to create snapshot volume", err)
	}

	snapLV, err := vg.LookupVolume(ctx, buildName)
	if err != nil {
		return fail("failed to get snapshot after creation", err)
	}

	// The source may have grown since the report, in which case the snapshot is
	// larger than desiredSize and cannot be shrunk to it: fail and start over.
	if err := snapLV.Resize(ctx, desiredSize); err != nil {
		return fail("failed to resize snapshot volume", err)
	}

	// If source volume is thin, activate the thin snapshot lv with accessmode.
	if err := snapLV.Activate(ctx, req.AccessType); err != nil {
		return fail("failed to activate snapshot volume", err)
	}

	if err := snapLV.Rename(ctx, req.GetName()); err != nil {
		return fail("failed to give the snapshot volume its name", err)
	}

	s.notify()

	logger.Info(
		"created a new snapshot LV",
		"size", desiredSize,
		"accessType", req.AccessType,
		"sourceID", sourceVolume,
	)

	return &proto.CreateLVSnapshotResponse{
		Snapshot: &proto.LogicalVolume{
			Name: snapLV.Name(),
			// convert to int64 because lvmd internals and lvm use uint64 but CSI uses int64.
			// For most conventional lvm use cases overflow here will never occur (9223372 TB or above cause overflow)
			SizeBytes: int64(snapLV.Size()),
			DevMajor:  snapLV.MajorNumber(),
			DevMinor:  snapLV.MinorNumber(),
		},
	}, nil
}

func (s *lvService) ResizeLV(ctx context.Context, req *proto.ResizeLVRequest) (*proto.ResizeLVResponse, error) {
	logger := log.FromContext(ctx).WithValues("name", req.GetName())

	dc, err := s.dcmapper.DeviceClass(req.DeviceClass)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: %s", err.Error(), req.DeviceClass)
	}
	capacityCheck := s.reservations.begin(reservationKey(dc))
	defer capacityCheck.close()
	pool, err := storagePoolForDeviceClass(ctx, dc)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get pool from device class: %v", err)
	}
	// FindVolume on VolumeGroup or ThinPool returns ThinLogicalVolumes as well
	// and no special handling for resize of LogicalVolume is needed
	lv, err := pool.FindVolume(ctx, req.GetName())
	if errors.Is(err, command.ErrNotFound) {
		logger.Error(err, "logical volume is not found")
		return nil, status.Errorf(codes.NotFound, "logical volume %s is not found", req.GetName())
	}
	if err != nil {
		logger.Error(err, "failed to find volume")
		return nil, status.Error(codes.Internal, err.Error())
	}

	requested := uint64(req.GetSizeBytes())
	current := lv.Size()
	if requested <= current {
		logger.Info("skipping resize: requested size is smaller than current size", "requested", requested, "current", current)
		return &proto.ResizeLVResponse{SizeBytes: int64(current)}, nil
	}

	free, err := pool.Free(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get free bytes: %v", err)
	}

	logger.Info(
		"lvservice request - ResizeLV",
		"requested", requested,
		"current", current,
		"free", free,
	)

	vols, err := pool.ListVolumes(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list volumes: %v", err)
	}
	reserved, free := capacityCheck.reserve(free, req.GetName(), requested, reportedSizes(vols))
	if reserved == nil {
		logger.Error(err, "no enough space left on VG",
			"requested", requested,
			"current", current,
			"free", free,
		)
		return nil, status.Errorf(codes.ResourceExhausted, "no enough space left on VG: free=%d, requested=%d", free, requested-current)
	}
	defer s.reservations.release(reserved)

	err = lv.Resize(ctx, requested)
	if err != nil {
		logger.Error(err, "failed to resize LV",
			"requested", requested,
			"current", current,
			"free", free,
		)
		return nil, status.Error(codes.Internal, err.Error())
	}
	s.notify()

	logger.Info("resized a LV", "requested", requested, "size", lv.Size())

	return &proto.ResizeLVResponse{SizeBytes: int64(lv.Size())}, nil
}
