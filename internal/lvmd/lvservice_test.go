package lvmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/topolvm/topolvm/internal/lvmd/command"
	"github.com/topolvm/topolvm/internal/lvmd/testutils"
	"github.com/topolvm/topolvm/pkg/lvmd/proto"
	lvmdTypes "github.com/topolvm/topolvm/pkg/lvmd/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	lvServiceTestVGName   = "test_lvservice"
	lvServiceTestPoolName = "test_lvservice_pool"
	lvServiceTestThickDC  = lvServiceTestVGName
	lvServiceTestThinDC   = lvServiceTestPoolName
	lvServiceTestTag1     = "testtag1"
	lvServiceTestTag2     = "testtag2"
)

func setupLVService(ctx context.Context, t *testing.T) (
	proto.LVServiceServer,
	*int,
	*command.VolumeGroup,
	*command.ThinPool,
) {
	testutils.RequireRoot(t)

	file := path.Join(t.TempDir(), t.Name())

	loop, err := testutils.MakeLoopbackDevice(ctx, file)
	if err != nil {
		t.Fatal(err)
	}

	err = testutils.MakeLoopbackVG(ctx, lvServiceTestVGName, loop)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = testutils.CleanLoopbackVG(lvServiceTestVGName, []string{loop}, []string{file})
	})

	vg, err := command.FindVolumeGroup(ctx, lvServiceTestVGName)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	notifier := func() {
		count++
	}

	// thinpool details
	overprovisionRatio := float64(10.0)
	poolName := "test_pool"
	poolSize := uint64(1 << 30)
	pool, err := vg.CreatePool(ctx, poolName, poolSize)
	if err != nil {
		t.Fatal(err)
	}

	lvService := NewLVService(
		NewDeviceClassManager(
			[]*lvmdTypes.DeviceClass{
				{
					// volumegroup target
					Name:        lvServiceTestThickDC,
					VolumeGroup: vg.Name(),
				},
				{
					// thinpool target
					Name:        lvServiceTestThinDC,
					VolumeGroup: vg.Name(),
					Type:        lvmdTypes.TypeThin,
					ThinPoolConfig: &lvmdTypes.ThinPoolConfig{
						Name:               poolName,
						OverprovisionRatio: overprovisionRatio,
					},
				},
			},
		),
		NewLvcreateOptionClassManager([]*lvmdTypes.LvcreateOptionClass{}),
		notifier,
	)

	return lvService, &count, vg, pool
}

func TestLVService_ThickLV(t *testing.T) {
	ctx := ctrl.LoggerInto(context.Background(), testr.New(t))
	lvService, count, vg, _ := setupLVService(ctx, t)

	res, err := lvService.CreateLV(context.Background(), &proto.CreateLVRequest{
		Name:        "test1",
		DeviceClass: lvServiceTestThickDC,
		SizeBytes:   1 << 30, // 1 GiB
		Tags:        []string{lvServiceTestTag1, lvServiceTestTag2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if *count != 1 {
		t.Errorf("is not notified: %d", count)
	}
	if res.GetVolume().GetName() != "test1" {
		t.Errorf(`res.Volume.Name != "test1": %s`, res.GetVolume().GetName())
	}
	if res.GetVolume().GetSizeBytes() != 1<<30 {
		t.Errorf(`res.Volume.SizeBytes != %d: %d`, 1<<30, res.GetVolume().GetSizeBytes())
	}
	err = exec.Command("lvs", vg.Name()+"/test1").Run()
	if err != nil {
		t.Error("failed to create logical volume")
	}

	if err := vg.Update(ctx); err != nil {
		t.Fatal(err)
	}

	lv, err := vg.FindVolume(ctx, "test1")
	if err != nil {
		t.Fatal(err)
	}
	if lv.Tags()[0] != lvServiceTestTag1 {
		t.Errorf(`testtag1 not present on volume`)
	}
	if lv.Tags()[1] != lvServiceTestTag2 {
		t.Errorf(`testtag1 not present on volume`)
	}

	_, err = lvService.CreateLV(context.Background(), &proto.CreateLVRequest{
		Name:        "test2",
		DeviceClass: lvServiceTestThickDC,
		SizeBytes:   3 << 30, // 3 GiB
	})
	code := status.Code(err)
	if code != codes.ResourceExhausted {
		t.Errorf(`code is not codes.ResouceExhausted: %s`, code)
	}
	if *count != 1 {
		t.Errorf("unexpected count: %d", count)
	}

	_, err = lvService.ResizeLV(context.Background(), &proto.ResizeLVRequest{
		Name:        "test1",
		DeviceClass: lvServiceTestThickDC,
		SizeBytes:   2 << 30, // 2 GiB
	})
	if err != nil {
		t.Fatal(err)
	}
	if *count != 2 {
		t.Errorf("unexpected count: %d", count)
	}

	if err := vg.Update(ctx); err != nil {
		t.Fatal(err)
	}

	lv, err = vg.FindVolume(ctx, "test1")
	if err != nil {
		t.Fatal(err)
	}
	if lv.Size() != (2 << 30) {
		t.Errorf(`does not match size 2: %d`, lv.Size()>>30)
	}

	_, err = lvService.ResizeLV(context.Background(), &proto.ResizeLVRequest{
		Name:        "test1",
		DeviceClass: lvServiceTestThickDC,
		SizeBytes:   5 << 30, // 5 GiB
	})
	code = status.Code(err)
	if code != codes.ResourceExhausted {
		t.Errorf(`code is not codes.ResouceExhausted: %s`, code)
	}
	if *count != 2 {
		t.Errorf("unexpected count: %d", count)
	}

	_, err = lvService.RemoveLV(context.Background(), &proto.RemoveLVRequest{
		Name:        "test1",
		DeviceClass: lvServiceTestThickDC,
	})
	if err != nil {
		t.Error(err)
	}
	if *count != 3 {
		t.Errorf("unexpected count: %d", count)
	}

	if err := vg.Update(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = vg.FindVolume(ctx, "test1")
	if !errors.Is(err, command.ErrNotFound) {
		t.Error("unexpected error: ", err)
	}
}

func TestLVService_ThinLV(t *testing.T) {
	ctx := ctrl.LoggerInto(context.Background(), testr.New(t))
	lvService, count, vg, pool := setupLVService(ctx, t)

	res, err := lvService.CreateLV(context.Background(), &proto.CreateLVRequest{
		Name:        "testp1",
		DeviceClass: lvServiceTestThinDC,
		SizeBytes:   1 << 30, // 1 GiB
		Tags:        []string{lvServiceTestTag1, lvServiceTestTag2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if *count != 1 {
		t.Errorf("is not notified: %d", count)
	}
	if res.GetVolume().GetName() != "testp1" {
		t.Errorf(`res.Volume.Name != "testp1": %s`, res.GetVolume().GetName())
	}
	if res.GetVolume().GetSizeBytes() != 1<<30 {
		t.Errorf(`res.Volume.SizeBytes != %d: %d`, 1<<30, res.GetVolume().GetSizeBytes())
	}
	err = exec.Command("lvs", vg.Name()+"/testp1").Run()
	if err != nil {
		t.Error("failed to create logical volume")
	}

	if err := vg.Update(ctx); err != nil {
		t.Fatal(err)
	}

	lv, err := pool.FindVolume(ctx, "testp1")
	if err != nil {
		t.Fatal(err)
	}
	if lv.Tags()[0] != lvServiceTestTag1 {
		t.Errorf(`testtag1 not present on volume`)
	}
	if lv.Tags()[1] != lvServiceTestTag2 {
		t.Errorf(`testtag1 not present on volume`)
	}

	// overprovision should work
	_, err = lvService.CreateLV(context.Background(), &proto.CreateLVRequest{
		Name:        "testp2",
		DeviceClass: lvServiceTestThinDC,
		SizeBytes:   3 << 30, // 3 GiB
	})
	if err != nil {
		t.Fatal(err)
	}
	if *count != 2 {
		t.Errorf("unexpected count: %d", count)
	}

	_, err = lvService.ResizeLV(context.Background(), &proto.ResizeLVRequest{
		Name:        "testp1",
		DeviceClass: lvServiceTestThinDC,
		SizeBytes:   2 << 30, // 2 GiB
	})
	if err != nil {
		t.Fatal(err)
	}
	if *count != 3 {
		t.Errorf("unexpected count: %d", count)
	}

	if err := vg.Update(ctx); err != nil {
		t.Fatal(err)
	}

	lv, err = pool.FindVolume(ctx, "testp1")
	if err != nil {
		t.Fatal(err)
	}
	if lv.Size() != (2 << 30) {
		t.Errorf(`does not match size 2: %d`, lv.Size()>>30)
	}

	_, err = lvService.RemoveLV(context.Background(), &proto.RemoveLVRequest{
		Name:        "testp1",
		DeviceClass: lvServiceTestThinDC,
	})
	if err != nil {
		t.Error(err)
	}
	if *count != 4 {
		t.Errorf("unexpected count: %d", count)
	}

	if err := vg.Update(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = pool.FindVolume(ctx, "test1")
	if !errors.Is(err, command.ErrNotFound) {
		t.Error("unexpected error: ", err)
	}

}

func TestLVService_ThinSnapshots(t *testing.T) {
	ctx := ctrl.LoggerInto(context.Background(), testr.New(t))
	lvService, count, vg, pool := setupLVService(ctx, t)

	// create sourceVolume
	var originalSizeBytes int64 = 1 << 30 // 1 GiB
	res, err := lvService.CreateLV(context.Background(), &proto.CreateLVRequest{
		Name:        "sourceVol",
		DeviceClass: lvServiceTestThinDC,
		SizeBytes:   originalSizeBytes,
		Tags:        []string{lvServiceTestTag1, lvServiceTestTag2},
	})
	if err != nil {
		t.Fatal(err)
	}

	// create snapshot of sourceVol
	var snapRes *proto.CreateLVSnapshotResponse
	var snapshotDesiredSizeBytes int64 = 2 << 30 // 2 GiB
	snapRes, err = lvService.CreateLVSnapshot(context.Background(), &proto.CreateLVSnapshotRequest{
		Name:         "snap1",
		DeviceClass:  lvServiceTestThinDC,
		SourceVolume: "sourceVol",
		// use a bigger size here to also simulate resizing to a bigger target than source
		SizeBytes:  snapshotDesiredSizeBytes,
		AccessType: "ro",
		Tags:       []string{"testsnaptag1", "testsnaptag2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if *count != 2 {
		t.Errorf("is not notified: %d", count)
	}
	if snapRes.GetSnapshot().GetName() != "snap1" {
		t.Errorf(`res.Volume.Name != "snap1": %s`, res.GetVolume().GetName())
	}
	if res.GetVolume().GetSizeBytes() != originalSizeBytes {
		t.Errorf(`res.Volume.SizeBytes != %d: %d`, originalSizeBytes, res.GetVolume().GetSizeBytes())
	}
	if snapRes.GetSnapshot().GetSizeBytes() != snapshotDesiredSizeBytes {
		t.Errorf(`snapRes.ThinSnapshot.SizeBytes != %d: %d`, snapshotDesiredSizeBytes, snapRes.GetSnapshot().GetSizeBytes())
	}
	err = exec.Command("lvs", vg.Name()+"/snap1").Run()
	if err != nil {
		t.Error("failed to create logical volume")
	}

	if err := vg.Update(ctx); err != nil {
		t.Fatal(err)
	}

	lv, err := pool.FindVolume(ctx, "snap1")
	if err != nil {
		t.Fatal(err)
	}
	if lv.Tags()[0] != "testsnaptag1" {
		t.Errorf(`testsnaptag1 not present on snapshot`)
	}
	if lv.Tags()[1] != "testsnaptag2" {
		t.Errorf(`testsnaptag1 not present on snapshot`)
	}

	// restore the created snapshot to a new logical volume.

	snapRes, err = lvService.CreateLVSnapshot(context.Background(), &proto.CreateLVSnapshotRequest{
		Name:         "restoredsnap1",
		DeviceClass:  lvServiceTestThinDC,
		SourceVolume: snapRes.GetSnapshot().GetName(),
		AccessType:   "rw",
		Tags:         []string{"testrestoretag1", "testrestoretag2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if *count != 3 {
		t.Errorf("is not notified: %d", count)
	}
	if snapRes.GetSnapshot().GetName() != "restoredsnap1" {
		t.Errorf(`res.Volume.Name != "restoredsnap1": %s`, res.GetVolume().GetName())
	}
	if res.GetVolume().GetSizeBytes() != originalSizeBytes {
		t.Errorf(`res.Volume.SizeBytes != %d: %d`, originalSizeBytes, res.GetVolume().GetSizeBytes())
	}
	if snapRes.GetSnapshot().GetSizeBytes() != snapshotDesiredSizeBytes {
		t.Errorf(`snapRes.ThinSnapshot.SizeBytes != %d: %d`, snapshotDesiredSizeBytes, snapRes.GetSnapshot().GetSizeBytes())
	}
	err = exec.Command("lvs", vg.Name()+"/restoredsnap1").Run()
	if err != nil {
		t.Error("failed to create logical volume")
	}

	if err := vg.Update(ctx); err != nil {
		t.Fatal(err)
	}

	lv, err = pool.FindVolume(ctx, "restoredsnap1")
	if err != nil {
		t.Fatal(err)
	}
	if lv.Tags()[0] != "testrestoretag1" {
		t.Errorf(`testsnaptag1 not present on snapshot`)
	}
	if lv.Tags()[1] != "testrestoretag2" {
		t.Errorf(`testsnaptag1 not present on snapshot`)
	}
}

// TestLVService_IncompleteSnapshotsOnRealLVM checks the snapshot naming with
// real LVM: a snapshot is built under a name of its own and renamed, keeping
// its size, permission and tags; a snapshot of an existing name is refused;
// and the sweep lvmd runs when it starts removes only LVs under a build name.
func TestLVService_IncompleteSnapshotsOnRealLVM(t *testing.T) {
	ctx := ctrl.LoggerInto(context.Background(), testr.New(t))
	lvService, _, vg, _ := setupLVService(ctx, t)

	if _, err := lvService.CreateLV(ctx, &proto.CreateLVRequest{
		Name: "source", DeviceClass: lvServiceTestThinDC, SizeBytes: 1 << 30,
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := lvService.CreateLVSnapshot(ctx, &proto.CreateLVSnapshotRequest{
		Name: "snap", DeviceClass: lvServiceTestThinDC, SourceVolume: "source",
		SizeBytes: 2 << 30, AccessType: "ro", Tags: []string{"snaptag"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if snap.GetSnapshot().GetName() != "snap" || snap.GetSnapshot().GetSizeBytes() != 2<<30 {
		t.Errorf("unexpected snapshot: %v", snap.GetSnapshot())
	}
	out, err := exec.Command("lvs", "--noheadings", "-o", "lv_name,lv_attr,lv_tags", vg.Name()+"/snap").CombinedOutput()
	if err != nil {
		t.Fatalf("lvs: %v: %s", err, out)
	}
	fields := strings.Fields(string(out))
	// As before the build was made inactive: active, read-only, without the
	// activation-skip flag, with its tag.
	if len(fields) != 3 || len(fields[1]) < 10 || fields[1][1] != 'r' || fields[1][4] != 'a' || fields[1][9] == 'k' || fields[2] != "snaptag" {
		t.Errorf("snapshot after the rename: %q, want active, read-only and with its tag", out)
	}

	if _, err := lvService.CreateLVSnapshot(ctx, &proto.CreateLVSnapshotRequest{
		Name: "snap", DeviceClass: lvServiceTestThinDC, SourceVolume: "source", AccessType: "ro",
	}); status.Code(err) != codes.AlreadyExists {
		t.Errorf("a snapshot of an existing name: got %v, want AlreadyExists", err)
	}

	leftover := "snap2" + incompleteSnapshotSuffix + "0123456789abcdef"
	if out, err := exec.Command("lvcreate", "-s", "-k", "n", "-n", leftover, vg.Name()+"/source").CombinedOutput(); err != nil {
		t.Fatalf("lvcreate: %v: %s", err, out)
	}
	vgs, err := command.ListVolumeGroups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	RemoveIncompleteSnapshots(ctx, vgs, []*lvmdTypes.DeviceClass{{
		Name: lvServiceTestThinDC, VolumeGroup: vg.Name(), Type: lvmdTypes.TypeThin,
		ThinPoolConfig: &lvmdTypes.ThinPoolConfig{Name: "test_pool", OverprovisionRatio: 10},
	}})
	out, err = exec.Command("lvs", "--noheadings", "-o", "lv_name", vg.Name()).CombinedOutput()
	if err != nil {
		t.Fatalf("lvs: %v: %s", err, out)
	}
	names := strings.Fields(string(out))
	if slices.Contains(names, leftover) {
		t.Error("the sweep left the incomplete snapshot")
	}
	for _, name := range []string{"source", "snap"} {
		if !slices.Contains(names, name) {
			t.Errorf("the sweep removed %s", name)
		}
	}
	for _, name := range names {
		if incompleteSnapshotNamePattern.MatchString(name) {
			t.Errorf("an LV is left under a build name: %s", name)
		}
	}
}

func lvAttr(t *testing.T, vgName, name string) string {
	t.Helper()
	out, err := exec.Command("lvs", "--noheadings", "-o", "lv_attr", vgName+"/"+name).CombinedOutput()
	if err != nil {
		t.Fatalf("lvs %s: %v: %s", name, err, out)
	}
	return strings.TrimSpace(string(out))
}

func lvNames(t *testing.T, vgName string) []string {
	t.Helper()
	out, err := exec.Command("lvs", "--noheadings", "-o", "lv_name", vgName).CombinedOutput()
	if err != nil {
		t.Fatalf("lvs: %v: %s", err, out)
	}
	return strings.Fields(string(out))
}

// TestLVService_RenameOntoExistingNameOnRealLVM checks, on real LVM, that when
// the requested name appears while a clone is built, lvrename refuses it, both
// LVs stay intact, and the call removes only its own build.
func TestLVService_RenameOntoExistingNameOnRealLVM(t *testing.T) {
	ctx := ctrl.LoggerInto(context.Background(), testr.New(t))
	lvService, _, vg, _ := setupLVService(ctx, t)
	if _, err := lvService.CreateLV(ctx, &proto.CreateLVRequest{
		Name: "source", DeviceClass: lvServiceTestThinDC, SizeBytes: 1 << 30,
	}); err != nil {
		t.Fatal(err)
	}

	beforeSnapshotRename = func(ctx context.Context, vgName, name string) {
		out, err := exec.Command("lvcreate", "-T", vgName+"/test_pool", "-V", "1g", "-n", name, "--addtag", "other").CombinedOutput()
		if err != nil {
			t.Errorf("lvcreate %s: %v: %s", name, err, out)
		}
	}
	t.Cleanup(func() { beforeSnapshotRename = nil })

	_, err := lvService.CreateLVSnapshot(ctx, &proto.CreateLVSnapshotRequest{
		Name: "clone", DeviceClass: lvServiceTestThinDC, SourceVolume: "source",
		SizeBytes: 2 << 30, AccessType: "rw",
	})
	if err == nil {
		t.Fatal("the clone succeeded although its name was taken")
	}
	t.Logf("CreateLVSnapshot: %v", err)
	out, err := exec.Command("lvs", "--noheadings", "-o", "lv_name,lv_size,lv_tags", "--units", "b", "--nosuffix", vg.Name()+"/clone").CombinedOutput()
	if err != nil {
		t.Fatalf("the LV that took the name is gone: %v: %s", err, out)
	}
	if fields := strings.Fields(string(out)); len(fields) != 3 || fields[1] != "1073741824" || fields[2] != "other" {
		t.Errorf("the LV that took the name changed: %q", out)
	}
	names := lvNames(t, vg.Name())
	if !slices.Contains(names, "source") {
		t.Error("the source was removed")
	}
	for _, name := range names {
		if incompleteSnapshotNamePattern.MatchString(name) {
			t.Errorf("the build was left: %s", name)
		}
	}
}

// TestLVService_WritableCloneOnRealLVM checks, on real LVM, that a writable
// clone, extended and renamed, is active and writable under its name, without
// the activation-skip flag, with its device node in place and the build's gone
// when CreateLVSnapshot returns.
func TestLVService_WritableCloneOnRealLVM(t *testing.T) {
	ctx := ctrl.LoggerInto(context.Background(), testr.New(t))
	lvService, _, vg, _ := setupLVService(ctx, t)
	if _, err := lvService.CreateLV(ctx, &proto.CreateLVRequest{
		Name: "source", DeviceClass: lvServiceTestThinDC, SizeBytes: 1 << 30,
	}); err != nil {
		t.Fatal(err)
	}
	res, err := lvService.CreateLVSnapshot(ctx, &proto.CreateLVSnapshotRequest{
		Name: "clone", DeviceClass: lvServiceTestThinDC, SourceVolume: "source",
		SizeBytes: 2 << 30, AccessType: "rw",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.GetSnapshot().GetSizeBytes() != 2<<30 {
		t.Errorf("the clone is %d bytes, want 2 GiB", res.GetSnapshot().GetSizeBytes())
	}
	if _, err := os.Stat(path.Join("/dev", vg.Name(), "clone")); err != nil {
		t.Errorf("no device node under the clone's name when the call returned: %v", err)
	}
	builds, err := filepath.Glob(path.Join("/dev", vg.Name(), "*"+incompleteSnapshotSuffix+"*"))
	if err != nil || len(builds) != 0 {
		t.Errorf("device nodes left under a build name: %v %v", builds, err)
	}
	attr := lvAttr(t, vg.Name(), "clone")
	t.Logf("lv_attr of the clone: %s", attr)
	if len(attr) < 10 || attr[1] != 'w' || attr[4] != 'a' || attr[9] == 'k' {
		t.Errorf("lv_attr %q: want writable, active and without activation skip", attr)
	}
	for _, name := range lvNames(t, vg.Name()) {
		if incompleteSnapshotNamePattern.MatchString(name) {
			t.Errorf("the build was left: %s", name)
		}
	}
}

// suspension is one device-mapper suspend or resume that lvm logged.
type suspension struct {
	at   time.Duration
	what string
	dev  string
}

var suspendLine = regexp.MustCompile(`(Suspending|Resuming) (\S+) \((\d+:\d+)\)`)

// lvmVerbose runs an lvm command with -vvvv and returns the device-mapper
// suspends and resumes it logged, timed from its start.
func lvmVerbose(t *testing.T, args ...string) []suspension {
	t.Helper()
	cmd := exec.Command(args[0], append([]string{"-vvvv"}, args[1:]...)...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var events []suspension
	scanner := bufio.NewScanner(stderr)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		if m := suspendLine.FindStringSubmatch(scanner.Text()); m != nil {
			events = append(events, suspension{at: time.Since(start), what: m[1], dev: m[2] + " (" + m[3] + ")"})
		}
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("%s: %v", strings.Join(args, " "), err)
	}
	return events
}

// describeSuspensions says which devices were suspended, and for how long.
func describeSuspensions(events []suspension) string {
	var b strings.Builder
	since := map[string]time.Duration{}
	for _, e := range events {
		switch e.what {
		case "Suspending":
			since[e.dev] = e.at
		case "Resuming":
			if at, ok := since[e.dev]; ok {
				fmt.Fprintf(&b, "%s suspended %.1f ms; ", e.dev, float64((e.at-at).Microseconds())/1000)
				delete(since, e.dev)
			}
		}
	}
	for dev := range since {
		fmt.Fprintf(&b, "%s suspended, resume not seen; ", dev)
	}
	if b.Len() == 0 {
		return "nothing suspended"
	}
	return b.String()
}

func suspended(events []suspension) []string {
	var devs []string
	for _, e := range events {
		if e.what == "Suspending" {
			devs = append(devs, e.dev)
		}
	}
	return devs
}

// TestLVService_SnapshotStepsSuspendScopeOnRealLVM runs the steps of a
// writable clone larger than its source, and of a read-only snapshot, with
// the lvm commands lvmd runs, on real LVM, next to another active thin volume,
// and reports which device-mapper devices each step suspended: once in the
// order lvmd uses, with the build inactive until it has its name, and once
// with the build active from its creation, as before. It fails only if, in
// lvmd's order, the rename suspends anything, or a step misbehaves.
func TestLVService_SnapshotStepsSuspendScopeOnRealLVM(t *testing.T) {
	ctx := ctrl.LoggerInto(context.Background(), testr.New(t))
	lvService, _, vg, _ := setupLVService(ctx, t)
	for _, name := range []string{"source", "neighbour"} {
		if _, err := lvService.CreateLV(ctx, &proto.CreateLVRequest{
			Name: name, DeviceClass: lvServiceTestThinDC, SizeBytes: 1 << 30,
		}); err != nil {
			t.Fatal(err)
		}
	}
	v := vg.Name()
	var report strings.Builder
	step := func(order, what string, args ...string) []suspension {
		events := lvmVerbose(t, args...)
		fmt.Fprintf(&report, "  %-8s %-36s %s\n", order, what, describeSuspensions(events))
		return events
	}

	// lvmd's order: lvcreate -s -k n -a n, lvresize, lvchange -p r, lvrename,
	// lvchange -k n -a y.
	for _, c := range []struct{ name, access string }{{"clone", "rw"}, {"snap", "ro"}} {
		build := c.name + incompleteSnapshotSuffix + "0123456789abcdef"
		step("inactive", c.name+": lvcreate -s -a n", "lvcreate", "-s", "-k", "n", "-a", "n", "-n", build, v+"/source")
		if c.access == "rw" {
			step("inactive", c.name+": lvresize", "lvresize", "-L", fmt.Sprintf("%db", 2<<30), v+"/"+build)
		} else {
			step("inactive", c.name+": lvchange -p r", "lvchange", "-p", "r", v+"/"+build)
		}
		if devs := suspended(step("inactive", c.name+": lvrename", "lvrename", v, build, c.name)); len(devs) != 0 {
			t.Errorf("renaming the inactive %s suspended %v", c.name, devs)
		}
		step("inactive", c.name+": lvchange -k n -a y", "lvchange", "-k", "n", "-a", "y", v+"/"+c.name)
		attr := lvAttr(t, v, c.name)
		if len(attr) < 10 || attr[4] != 'a' || attr[9] == 'k' || (c.access == "ro") != (attr[1] == 'r') {
			t.Errorf("%s ends as %q", c.name, attr)
		}
	}

	// The previous order: the build is active from lvcreate -s -k n on.
	for _, c := range []struct{ name, access string }{{"old-clone", "rw"}, {"old-snap", "ro"}} {
		build := c.name + incompleteSnapshotSuffix + "0123456789abcdef"
		step("active", c.name+": lvcreate -s", "lvcreate", "-s", "-k", "n", "-n", build, v+"/source")
		if c.access == "rw" {
			step("active", c.name+": lvresize", "lvresize", "-L", fmt.Sprintf("%db", 2<<30), v+"/"+build)
		} else {
			step("active", c.name+": lvchange -p r", "lvchange", "-p", "r", v+"/"+build)
		}
		step("active", c.name+": lvrename", "lvrename", v, build, c.name)
	}
	t.Logf("device-mapper suspends per step, next to another active thin volume:\n%s", report.String())
}
