package lvmd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/topolvm/topolvm/internal/lvmd/command"
	lvmdTypes "github.com/topolvm/topolvm/pkg/lvmd/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// incompleteSnapshotSuffix marks the name a snapshot or clone is built under
// until CreateLVSnapshot renames it to the requested name.
const incompleteSnapshotSuffix = ".creating-"

var incompleteSnapshotNamePattern = regexp.MustCompile(`^.+` + regexp.QuoteMeta(incompleteSnapshotSuffix) + `[0-9a-f]{16}$`)

// incompleteSnapshotName returns a name for building a snapshot that no other
// call uses: the requested name with a random suffix.
func incompleteSnapshotName(name string) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("failed to make a name for the snapshot: %w", err)
	}
	return name + incompleteSnapshotSuffix + hex.EncodeToString(b[:]), nil
}

// builds holds the build names of the snapshots and clones this lvmd is
// making. CreateLVSnapshot adds a name before lvcreate -s can create the LV,
// and removes it when the call has returned, after its own clean-up.
var builds = struct {
	sync.Mutex
	names map[string]struct{}
}{names: map[string]struct{}{}}

func startBuild(name string) {
	builds.Lock()
	defer builds.Unlock()
	builds.names[name] = struct{}{}
}

func endBuild(name string) {
	builds.Lock()
	defer builds.Unlock()
	delete(builds.names, name)
}

func building(name string) bool {
	builds.Lock()
	defer builds.Unlock()
	_, ok := builds.names[name]
	return ok
}

// RemoveIncompleteSnapshots removes the snapshots and clones that an earlier
// lvmd left unfinished in the volume groups of the thin device classes: LVs
// still under the build name of CreateLVSnapshot. No LogicalVolume refers to
// such an LV, because CreateLVSnapshot gives it the requested name only as
// its last step. Call it when lvmd starts, before it serves any call; vgs is
// the volume groups as ListVolumeGroups reports them.
func RemoveIncompleteSnapshots(ctx context.Context, vgs []*command.VolumeGroup, deviceClasses []*lvmdTypes.DeviceClass) {
	removeBuilds(ctx, vgs, deviceClasses, "removed incomplete snapshot volume left by an earlier lvmd")
}

// RemoveLeftoverBuilds removes, while lvmd runs, the LVs under a build name
// that no call of this lvmd is making: builds whose clean-up failed, and
// builds that the lvm process of a cancelled call created after the call had
// cleaned up. vgs is a report taken at any time before: a build that was in
// progress then and has finished since is gone or renamed, and a build that
// started since cannot be in it.
func RemoveLeftoverBuilds(ctx context.Context, vgs []*command.VolumeGroup, deviceClasses []*lvmdTypes.DeviceClass) {
	removeBuilds(ctx, vgs, deviceClasses, "removed leftover snapshot build volume")
}

func removeBuilds(ctx context.Context, vgs []*command.VolumeGroup, deviceClasses []*lvmdTypes.DeviceClass, removed string) {
	logger := log.FromContext(ctx)
	done := map[string]bool{}
	for _, dc := range deviceClasses {
		if dc.Type != lvmdTypes.TypeThin || done[dc.VolumeGroup] {
			continue
		}
		done[dc.VolumeGroup] = true
		vg, err := command.SearchVolumeGroupList(vgs, dc.VolumeGroup)
		if err != nil {
			continue
		}
		lvs, err := vg.ListVolumes(ctx)
		if err != nil {
			logger.Error(err, "failed to list volumes for incomplete snapshots", "volume_group", dc.VolumeGroup)
			continue
		}
		for name := range lvs {
			if !incompleteSnapshotNamePattern.MatchString(name) || building(name) {
				continue
			}
			err := command.RemoveVolume(ctx, vg.Name(), name)
			switch {
			case errors.Is(err, command.ErrNotFound):
				// Renamed or removed by its call since the report.
			case err != nil:
				logger.Error(err, "failed to remove incomplete snapshot volume", "volume_group", dc.VolumeGroup, "name", name)
			default:
				logger.Info(removed, "volume_group", dc.VolumeGroup, "name", name)
			}
		}
	}
}
