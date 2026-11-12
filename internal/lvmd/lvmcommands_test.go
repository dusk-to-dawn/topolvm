package lvmd

import (
	"context"
	"os"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/go-logr/logr/testr"
	"github.com/topolvm/topolvm/internal/lvmd/command"
	"github.com/topolvm/topolvm/internal/lvmd/testutils/fakelvm"
	"github.com/topolvm/topolvm/pkg/lvmd/proto"
	lvmdTypes "github.com/topolvm/topolvm/pkg/lvmd/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	ctrl "sigs.k8s.io/controller-runtime"
)

func TestMain(m *testing.M) {
	code := m.Run()
	fakelvm.Cleanup()
	os.Exit(code)
}

const (
	fakeVGName   = "fakevg"
	fakePoolName = "fakepool"
	fakeThickDC  = "thick"
	fakeThinDC   = "thin"
	fakeNoVGDC   = "novg"
	fakeNoPoolDC = "nopool"
	fakeGiB      = uint64(1) << 30
)

type fakeLVMD struct {
	lvm      *fakelvm.Fake
	lv       proto.LVServiceServer
	vg       proto.VGServiceServer
	notified atomic.Int32
}

func startFakeLVMD(t *testing.T, opts fakelvm.Options) *fakeLVMD {
	t.Helper()
	f := &fakeLVMD{}
	f.lvm = fakelvm.Start(t, fakelvm.State{
		VGs: []*fakelvm.VG{{
			Name: fakeVGName,
			Size: 100 * fakeGiB,
			Free: 60 * fakeGiB,
			LVs: []*fakelvm.LV{
				{Name: fakePoolName, Size: 40 * fakeGiB, IsPool: true},
				{Name: "existing-thin", Size: 10 * fakeGiB, Pool: fakePoolName, Minor: 100},
				{Name: "existing-thick", Size: 5 * fakeGiB, Minor: 101},
			},
		}},
		NextMinor: 200,
	}, opts, command.LVMCommandPrefix, command.SetLVMCommandPrefix)

	dcm := NewDeviceClassManager([]*lvmdTypes.DeviceClass{
		{Name: fakeThickDC, VolumeGroup: fakeVGName, Default: true},
		{
			Name:        fakeThinDC,
			VolumeGroup: fakeVGName,
			Type:        lvmdTypes.TypeThin,
			ThinPoolConfig: &lvmdTypes.ThinPoolConfig{
				Name:               fakePoolName,
				OverprovisionRatio: 2,
			},
		},
		{Name: fakeNoVGDC, VolumeGroup: "missingvg"},
		{
			Name:        fakeNoPoolDC,
			VolumeGroup: fakeVGName,
			Type:        lvmdTypes.TypeThin,
			ThinPoolConfig: &lvmdTypes.ThinPoolConfig{
				Name:               "missingpool",
				OverprovisionRatio: 2,
			},
		},
	})
	vgService, notifyWatchers := NewVGService(dcm)
	f.vg = vgService
	f.lv = NewLVService(dcm, NewLvcreateOptionClassManager(nil), func() {
		f.notified.Add(1)
		notifyWatchers()
	})
	return f
}

func testContext(t *testing.T) context.Context {
	return ctrl.LoggerInto(context.Background(), testr.New(t))
}

func expectCommands(t *testing.T, f *fakeLVMD, want []string) {
	t.Helper()
	if got := f.lvm.SubCommands(t); !slices.Equal(got, want) {
		t.Errorf("lvm commands: got %v (%d), want %v (%d)\nfull: %q", got, len(got), want, len(want), f.lvm.Commands(t))
	}
}

func TestLVMCommandsPerCall(t *testing.T) {
	for _, dc := range []string{fakeThinDC, fakeThickDC} {
		t.Run(dc, func(t *testing.T) {
			ctx := testContext(t)
			f := startFakeLVMD(t, fakelvm.Options{})

			t.Run("GetLVList", func(t *testing.T) {
				f.lvm.Reset(t)
				res, err := f.vg.GetLVList(ctx, &proto.GetLVListRequest{DeviceClass: dc})
				if err != nil {
					t.Fatal(err)
				}
				expectCommands(t, f, []string{"fullreport"})
				want := "existing-thick"
				if dc == fakeThinDC {
					want = "existing-thin"
				}
				if !slices.ContainsFunc(res.GetVolumes(), func(v *proto.LogicalVolume) bool { return v.GetName() == want }) {
					t.Errorf("%s not listed: %v", want, res.GetVolumes())
				}
			})

			t.Run("GetFreeBytes", func(t *testing.T) {
				f.lvm.Reset(t)
				if _, err := f.vg.GetFreeBytes(ctx, &proto.GetFreeBytesRequest{DeviceClass: dc}); err != nil {
					t.Fatal(err)
				}
				expectCommands(t, f, []string{"fullreport"})
			})

			t.Run("CreateLV", func(t *testing.T) {
				f.lvm.Reset(t)
				res, err := f.lv.CreateLV(ctx, &proto.CreateLVRequest{
					Name: "new-" + dc, DeviceClass: dc, SizeBytes: int64(fakeGiB),
				})
				if err != nil {
					t.Fatal(err)
				}
				expectCommands(t, f, []string{"fullreport", "lvcreate", "lvs"})
				if res.GetVolume().GetName() != "new-"+dc || res.GetVolume().GetSizeBytes() != int64(fakeGiB) {
					t.Errorf("unexpected volume: %v", res.GetVolume())
				}
				if res.GetVolume().GetDevMajor() != 253 || res.GetVolume().GetDevMinor() == 0 {
					t.Errorf("device numbers not read back: %v", res.GetVolume())
				}
			})

			t.Run("ResizeLV", func(t *testing.T) {
				f.lvm.Reset(t)
				res, err := f.lv.ResizeLV(ctx, &proto.ResizeLVRequest{
					Name: "new-" + dc, DeviceClass: dc, SizeBytes: int64(2 * fakeGiB),
				})
				if err != nil {
					t.Fatal(err)
				}
				expectCommands(t, f, []string{"fullreport", "lvresize", "lvs"})
				if res.GetSizeBytes() != int64(2*fakeGiB) {
					t.Errorf("resize returned %d, want the new size %d", res.GetSizeBytes(), 2*fakeGiB)
				}
			})

			if dc == fakeThinDC {
				t.Run("CreateLVSnapshot", func(t *testing.T) {
					f.lvm.Reset(t)
					res, err := f.lv.CreateLVSnapshot(ctx, &proto.CreateLVSnapshotRequest{
						Name: "snap", DeviceClass: dc, SourceVolume: "existing-thin", AccessType: "ro",
					})
					if err != nil {
						t.Fatal(err)
					}
					expectCommands(t, f, []string{"fullreport", "lvcreate", "lvs", "lvchange"})
					if res.GetSnapshot().GetSizeBytes() != int64(10*fakeGiB) {
						t.Errorf("unexpected snapshot: %v", res.GetSnapshot())
					}
				})
			}

			t.Run("RemoveLV", func(t *testing.T) {
				f.lvm.Reset(t)
				if _, err := f.lv.RemoveLV(ctx, &proto.RemoveLVRequest{Name: "new-" + dc, DeviceClass: dc}); err != nil {
					t.Fatal(err)
				}
				expectCommands(t, f, []string{"lvremove"})
			})
		})
	}
}

// TestLVMCommandsValidation checks that answering a call from one report keeps
// every check the separate vgs and lvs commands used to make.
func TestLVMCommandsValidation(t *testing.T) {
	ctx := testContext(t)
	f := startFakeLVMD(t, fakelvm.Options{})

	tests := []struct {
		name string
		call func() error
		want codes.Code
	}{
		{"create in missing VG", func() error {
			_, err := f.lv.CreateLV(ctx, &proto.CreateLVRequest{Name: "a", DeviceClass: fakeNoVGDC, SizeBytes: int64(fakeGiB)})
			return err
		}, codes.Internal},
		{"create in missing pool", func() error {
			_, err := f.lv.CreateLV(ctx, &proto.CreateLVRequest{Name: "a", DeviceClass: fakeNoPoolDC, SizeBytes: int64(fakeGiB)})
			return err
		}, codes.Internal},
		{"create beyond the overprovision ratio", func() error {
			// 2 x 40 GiB virtual, 10 GiB used: 71 GiB does not fit.
			_, err := f.lv.CreateLV(ctx, &proto.CreateLVRequest{Name: "a", DeviceClass: fakeThinDC, SizeBytes: int64(71 * fakeGiB)})
			return err
		}, codes.ResourceExhausted},
		{"create beyond VG free space", func() error {
			_, err := f.lv.CreateLV(ctx, &proto.CreateLVRequest{Name: "a", DeviceClass: fakeThickDC, SizeBytes: int64(61 * fakeGiB)})
			return err
		}, codes.ResourceExhausted},
		{"unknown device class", func() error {
			_, err := f.lv.CreateLV(ctx, &proto.CreateLVRequest{Name: "a", DeviceClass: "nosuchclass", SizeBytes: int64(fakeGiB)})
			return err
		}, codes.NotFound},
		{"list in missing VG", func() error {
			_, err := f.vg.GetLVList(ctx, &proto.GetLVListRequest{DeviceClass: fakeNoVGDC})
			return err
		}, codes.Internal},
		{"list in missing pool", func() error {
			_, err := f.vg.GetLVList(ctx, &proto.GetLVListRequest{DeviceClass: fakeNoPoolDC})
			return err
		}, codes.Internal},
		{"remove missing LV", func() error {
			_, err := f.lv.RemoveLV(ctx, &proto.RemoveLVRequest{Name: "nosuchlv", DeviceClass: fakeThinDC})
			return err
		}, codes.NotFound},
		{"remove from missing VG", func() error {
			_, err := f.lv.RemoveLV(ctx, &proto.RemoveLVRequest{Name: "a", DeviceClass: fakeNoVGDC})
			return err
		}, codes.NotFound},
		{"resize missing LV", func() error {
			_, err := f.lv.ResizeLV(ctx, &proto.ResizeLVRequest{Name: "nosuchlv", DeviceClass: fakeThinDC, SizeBytes: int64(fakeGiB)})
			return err
		}, codes.NotFound},
		{"resize beyond the overprovision ratio", func() error {
			_, err := f.lv.ResizeLV(ctx, &proto.ResizeLVRequest{Name: "existing-thin", DeviceClass: fakeThinDC, SizeBytes: int64(81 * fakeGiB)})
			return err
		}, codes.ResourceExhausted},
		{"snapshot of missing source", func() error {
			_, err := f.lv.CreateLVSnapshot(ctx, &proto.CreateLVSnapshotRequest{Name: "s", DeviceClass: fakeThinDC, SourceVolume: "nosuchlv", AccessType: "ro"})
			return err
		}, codes.NotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			if got := status.Code(err); got != tt.want {
				t.Errorf("code: got %v (%v), want %v", got, err, tt.want)
			}
		})
	}
	if n := f.notified.Load(); n != 0 {
		t.Errorf("failed calls notified watchers %d times", n)
	}
}
