package controller

import "time"

const (
	// cacheCatchUpTimeout bounds how long a pass that patched its
	// LogicalVolume waits for the cache to show the status it then wrote.
	cacheCatchUpTimeout = time.Second

	// conflictRequeueDelay is when a pass whose status update conflicted
	// looks at the LogicalVolume again, if no event has brought it back first.
	conflictRequeueDelay = 100 * time.Millisecond

	// keySelectedNode is a PVC resource indexing key for the controller
	keySelectedNode = "metadata.annotations.selected-node"

	// keyLogicalVolumeNode is a Logical Volume resource indexing key for the controller
	keyLogicalVolumeNode = "spec.nodeName"

	// AnnSelectedNode annotation is added to a PVC that has been triggered by scheduler to
	// be dynamically provisioned. Its value is the name of the selected node.
	// https://github.com/kubernetes/kubernetes/blob/9bae1bc56804db4905abebcd408e0f02e199ab93/pkg/controller/volume/persistentvolume/util/util.go#L53
	AnnSelectedNode = "volume.kubernetes.io/selected-node"
)
