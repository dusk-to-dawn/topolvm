package lvmd

import (
	"context"

	internalLvmd "github.com/topolvm/topolvm/internal/lvmd"
	internalLvmdCommand "github.com/topolvm/topolvm/internal/lvmd/command"
	"github.com/topolvm/topolvm/pkg/lvmd/proto"
	lvmdTypes "github.com/topolvm/topolvm/pkg/lvmd/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func NewEmbeddedServiceClients(
	ctx context.Context,
	deviceClasses []*lvmdTypes.DeviceClass,
	LvcreateOptionClasses []*lvmdTypes.LvcreateOptionClass,
) (
	proto.LVServiceClient,
	proto.VGServiceClient,
) {
	// Snapshots that an earlier run left unfinished are removed before any
	// call is served.
	if vgs, err := internalLvmdCommand.ListVolumeGroups(ctx); err != nil {
		log.FromContext(ctx).Error(err, "failed to list volume groups for incomplete snapshots")
	} else {
		internalLvmd.RemoveIncompleteSnapshots(ctx, vgs, deviceClasses)
	}

	dcManager := internalLvmd.NewDeviceClassManager(deviceClasses)
	lvOptionClassManager := internalLvmd.NewLvcreateOptionClassManager(LvcreateOptionClasses)

	return internalLvmd.NewEmbeddedServiceClients(ctx, dcManager, lvOptionClassManager)
}
