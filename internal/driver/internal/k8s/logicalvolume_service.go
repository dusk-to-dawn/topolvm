package k8s

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/topolvm/topolvm"
	topolvmlegacyv1 "github.com/topolvm/topolvm/api/legacy/v1"
	topolvmv1 "github.com/topolvm/topolvm/api/v1"
	clientwrapper "github.com/topolvm/topolvm/internal/client"
	"github.com/topolvm/topolvm/internal/getter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// ErrVolumeNotFound represents the specified volume is not found.
var ErrVolumeNotFound = errors.New("VolumeID is not found")

// LogicalVolumeService represents service for LogicalVolume.
// This is not concurrent safe, must take lock on caller.
type LogicalVolumeService struct {
	writer interface {
		client.Writer
		client.StatusClient
	}
	getter       getter.Interface
	volumeGetter *volumeGetter
	// changes wakes up callers waiting for a LogicalVolume to change.
	changes *changeNotifier
}

// changeNotifier lets callers wait for events on a LogicalVolume, delivered
// by the informer whose cache the service reads from. The informer updates
// its cache before it delivers an event, so a woken caller reads the change.
type changeNotifier struct {
	mu      sync.Mutex
	waiters map[string]map[chan struct{}]struct{}
}

func newChangeNotifier() *changeNotifier {
	return &changeNotifier{waiters: map[string]map[chan struct{}]struct{}{}}
}

// watch returns a channel that receives a value after the named
// LogicalVolume changes, and a function to stop watching.
func (n *changeNotifier) watch(name string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.waiters[name] == nil {
		n.waiters[name] = map[chan struct{}]struct{}{}
	}
	n.waiters[name][ch] = struct{}{}
	return ch, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		delete(n.waiters[name], ch)
		if len(n.waiters[name]) == 0 {
			delete(n.waiters, name)
		}
	}
}

func (n *changeNotifier) notify(obj any) {
	if tombstone, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	o, err := meta.Accessor(obj)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	for ch := range n.waiters[o.GetName()] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// fallbackPoll is how often a waiter checks the LogicalVolume when no event
// arrives. Events normally end a wait first; this only bounds how long a
// missed event could delay it, as the polling did before events were used.
var fallbackPoll = wait.Backoff{
	Duration: 1 * time.Second,
	Factor:   2,
	Jitter:   0.1,
	Steps:    math.MaxInt,
	Cap:      10 * time.Second,
}

// waitForChange calls condition now and again after every change of the
// named LogicalVolume, until it reports done or an error, or ctx ends.
// Conditions read through the service's cache-backed getter, so waiting
// costs no requests to the API server beyond the ones a condition makes
// when the cache does not have the object.
func (s *LogicalVolumeService) waitForChange(ctx context.Context, name string, fallback wait.Backoff, condition func(ctx context.Context) (bool, error)) error {
	changed, stop := s.changes.watch(name)
	defer stop()
	delay := fallback.DelayFunc()
	for {
		done, err := condition(ctx)
		if err != nil || done {
			return err
		}
		timer := time.NewTimer(delay())
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-changed:
			timer.Stop()
		case <-timer.C:
		}
	}
}

const (
	indexFieldVolumeID = "status.volumeID"
)

var (
	logger = ctrl.Log.WithName("LogicalVolume")
)

type retryMissingGetter struct {
	cacheReader client.Reader
	apiReader   client.Reader
	getter      getter.Interface
}

func newRetryMissingGetter(cacheReader client.Reader, apiReader client.Reader) *retryMissingGetter {
	return &retryMissingGetter{
		cacheReader: cacheReader,
		apiReader:   apiReader,
		getter:      getter.NewRetryMissingGetter(cacheReader, apiReader),
	}
}

func (r *retryMissingGetter) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	var lv *topolvmv1.LogicalVolume
	var ok bool
	if lv, ok = obj.(*topolvmv1.LogicalVolume); !ok {
		return r.getter.Get(ctx, key, obj)
	}

	err := r.cacheReader.Get(ctx, key, lv)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	return r.apiReader.Get(ctx, key, lv)
}

// This type is a safe guard to prohibit calling List from LogicalVolumeService directly.
type volumeGetter struct {
	cacheReader client.Reader
	apiReader   client.Reader
}

// Get returns LogicalVolume by volume ID.
// This ensures read-after-create consistency.
func (v *volumeGetter) Get(ctx context.Context, volumeID string) (*topolvmv1.LogicalVolume, error) {
	lvList := new(topolvmv1.LogicalVolumeList)
	err := v.cacheReader.List(ctx, lvList, client.MatchingFields{indexFieldVolumeID: volumeID})
	if err != nil {
		return nil, err
	}

	if len(lvList.Items) > 1 {
		return nil, fmt.Errorf("multiple LogicalVolume is found for VolumeID %s", volumeID)
	} else if len(lvList.Items) != 0 {
		return &lvList.Items[0], nil
	}

	// not found. try direct reader.
	err = v.apiReader.List(ctx, lvList)
	if err != nil {
		return nil, err
	}

	count := 0
	var foundLv *topolvmv1.LogicalVolume
	for _, lv := range lvList.Items {
		if lv.Status.VolumeID == volumeID {
			count++
			lv := lv
			foundLv = &lv
		}
	}
	if count > 1 {
		return nil, fmt.Errorf("multiple LogicalVolume is found for VolumeID %s", volumeID)
	}
	if foundLv == nil {
		return nil, ErrVolumeNotFound
	}
	return foundLv, nil
}

//+kubebuilder:rbac:groups=topolvm.io,resources=logicalvolumes,verbs=get;list;watch;create;delete
//+kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch

// NewLogicalVolumeService returns LogicalVolumeService.
func NewLogicalVolumeService(mgr manager.Manager) (*LogicalVolumeService, error) {
	ctx := context.Background()
	var lvObject client.Object = &topolvmv1.LogicalVolume{}
	if topolvm.UseLegacy() {
		lvObject = &topolvmlegacyv1.LogicalVolume{}
	}
	changes := newChangeNotifier()
	informer, err := mgr.GetCache().GetInformer(ctx, lvObject)
	if err != nil {
		return nil, err
	}
	if _, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    changes.notify,
		UpdateFunc: func(_, obj any) { changes.notify(obj) },
		DeleteFunc: changes.notify,
	}); err != nil {
		return nil, err
	}

	if topolvm.UseLegacy() {
		err := mgr.GetFieldIndexer().IndexField(ctx, &topolvmlegacyv1.LogicalVolume{}, indexFieldVolumeID, func(o client.Object) []string {
			return []string{o.(*topolvmlegacyv1.LogicalVolume).Status.VolumeID}
		})
		if err != nil {
			return nil, err
		}
	} else {
		err := mgr.GetFieldIndexer().IndexField(ctx, &topolvmv1.LogicalVolume{}, indexFieldVolumeID, func(o client.Object) []string {
			return []string{o.(*topolvmv1.LogicalVolume).Status.VolumeID}
		})
		if err != nil {
			return nil, err
		}
	}

	client := clientwrapper.NewWrappedClient(mgr.GetClient())
	apiReader := clientwrapper.NewWrappedReader(mgr.GetAPIReader(), mgr.GetClient().Scheme())
	return &LogicalVolumeService{
		writer:       client,
		getter:       newRetryMissingGetter(client, apiReader),
		volumeGetter: &volumeGetter{cacheReader: client, apiReader: apiReader},
		changes:      changes,
	}, nil
}

// CreateVolume creates volume
func (s *LogicalVolumeService) CreateVolume(ctx context.Context, node, dc, oc, name, sourceName string, requestBytes int64) (*topolvmv1.LogicalVolume, error) {
	logger.Info("k8s.CreateVolume called", "name", name, "node", node, "size", requestBytes, "sourceName", sourceName)
	var lv *topolvmv1.LogicalVolume
	// if the create volume request has no source, proceed with regular lv creation.
	if sourceName == "" {
		lv = &topolvmv1.LogicalVolume{
			ObjectMeta: newLogicalVolumeMeta(name),
			Spec: topolvmv1.LogicalVolumeSpec{
				Name:                name,
				NodeName:            node,
				DeviceClass:         dc,
				LvcreateOptionClass: oc,
				Size:                *resource.NewQuantity(requestBytes, resource.BinarySI),
			},
		}

	} else {
		// On the other hand, if a volume has a datasource, create a thin snapshot of the source volume with READ-WRITE access for volume cloning.
		lv = &topolvmv1.LogicalVolume{
			ObjectMeta: newLogicalVolumeMeta(name),
			Spec: topolvmv1.LogicalVolumeSpec{
				Name:                name,
				NodeName:            node,
				DeviceClass:         dc,
				LvcreateOptionClass: oc,
				Size:                *resource.NewQuantity(requestBytes, resource.BinarySI),
				Source:              sourceName,
				AccessType:          "rw",
			},
		}
	}

	return s.createAndWait(ctx, lv)
}

// newLogicalVolumeMeta returns the metadata of a new LogicalVolume. It
// carries the finalizer and label that topolvm-node would otherwise add with
// a patch before creating the volume, so that topolvm-node can create it on
// its first pass. topolvm-node still adds them if they are missing.
func newLogicalVolumeMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:       name,
		Finalizers: []string{topolvm.GetLogicalVolumeFinalizer()},
		Labels:     map[string]string{topolvm.CreatedbyLabelKey: topolvm.CreatedbyLabelValue},
	}
}

// DeleteVolume deletes volume
func (s *LogicalVolumeService) DeleteVolume(ctx context.Context, volumeID string) error {
	logger.Info("k8s.DeleteVolume called", "volumeID", volumeID)

	lv, err := s.GetVolume(ctx, volumeID)
	if err != nil {
		if errors.Is(err, ErrVolumeNotFound) {
			logger.Info("volume is not found", "volume_id", volumeID)
			return nil
		}
		return err
	}

	err = s.writer.Delete(ctx, lv)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	// wait until delete the target volume
	fallback := fallbackPoll
	fallback.Duration = 100 * time.Millisecond
	return s.waitForChange(ctx, lv.Name, fallback, func(ctx context.Context) (bool, error) {
		if err := s.getter.Get(ctx, client.ObjectKey{Name: lv.Name}, new(topolvmv1.LogicalVolume)); err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			logger.Error(err, "failed to get LogicalVolume", "name", lv.Name)
			return false, err
		}
		logger.Info("waiting for LogicalVolume to be deleted", "name", lv.Name)
		return false, nil
	})
}

// CreateSnapshot creates a snapshot of existing volume.
func (s *LogicalVolumeService) CreateSnapshot(ctx context.Context, node, dc, sourceVol, sname, accessType string, snapSize resource.Quantity) (*topolvmv1.LogicalVolume, error) {
	logger.Info("CreateSnapshot called", "name", sname)
	snapshotLV := &topolvmv1.LogicalVolume{
		ObjectMeta: newLogicalVolumeMeta(sname),
		Spec: topolvmv1.LogicalVolumeSpec{
			Name:        sname,
			NodeName:    node,
			DeviceClass: dc,
			Size:        snapSize,
			Source:      sourceVol,
			AccessType:  accessType,
		},
	}

	return s.createAndWait(ctx, snapshotLV)
}

// ExpandVolume expands volume
func (s *LogicalVolumeService) ExpandVolume(ctx context.Context, volumeID string, requestBytes int64) (*topolvmv1.LogicalVolume, error) {
	logger := logger.WithValues("volume_id", volumeID, "size", requestBytes)
	logger.Info("k8s.ExpandVolume called")
	request := resource.NewQuantity(requestBytes, resource.BinarySI)

	lv, err := s.GetVolume(ctx, volumeID)
	if err != nil {
		return nil, err
	}

	err = s.updateSpecSize(ctx, volumeID, request)
	if err != nil {
		return nil, err
	}

	var changedLV topolvmv1.LogicalVolume
	return &changedLV, s.waitForChange(ctx, lv.Name, fallbackPoll, func(ctx context.Context) (bool, error) {
		if err := s.getter.Get(ctx, client.ObjectKey{Name: lv.Name}, &changedLV); err != nil {
			logger.Error(err, "failed to get LogicalVolume", "name", lv.Name)
			return false, err
		}

		if changedLV.Status.Code != codes.OK {
			return false, status.Error(changedLV.Status.Code, changedLV.Status.Message)
		}

		if changedLV.Status.CurrentSize == nil {
			logger.Info("waiting for update of 'status.currentSize' "+
				"to be filled initially", "name", lv.Name)
			// WA: since Status.CurrentSize is added in v0.4.0. it may be missing.
			// if the expansion is completed, it is filled, so wait for that.
			return false, nil
		}

		if changedLV.Status.CurrentSize.Cmp(*request) < 0 {
			logger.Info("waiting for update of 'status.currentSize' to be at least the requested size",
				"name", lv.Name,
				"status.currentSize", changedLV.Status.CurrentSize,
				"spec.size", changedLV.Spec.Size,
				"request", request,
			)
			return false, nil
		}

		logger.Info("LogicalVolume successfully expanded")
		return true, nil
	})
}

// GetVolume returns LogicalVolume by volume ID.
func (s *LogicalVolumeService) GetVolume(ctx context.Context, volumeID string) (*topolvmv1.LogicalVolume, error) {
	return s.volumeGetter.Get(ctx, volumeID)
}

// updateSpecSize updates .Spec.Size of LogicalVolume.
func (s *LogicalVolumeService) updateSpecSize(ctx context.Context, volumeID string, size *resource.Quantity) error {
	return wait.ExponentialBackoffWithContext(ctx,
		retry.DefaultBackoff,
		func(ctx context.Context) (bool, error) {
			lv, err := s.GetVolume(ctx, volumeID)
			if err != nil {
				return false, err
			}
			lv.Spec.Size = *size
			if lv.Annotations == nil {
				lv.Annotations = make(map[string]string)
			}
			lv.Annotations[topolvm.GetResizeRequestedAtKey()] = time.Now().UTC().String()

			if err := s.writer.Update(ctx, lv); err != nil {
				if apierrors.IsConflict(err) {
					logger.Info("detected conflict when trying to update LogicalVolume spec", "name", lv.Name)
					return false, nil
				} else {
					logger.Error(err, "failed to update LogicalVolume spec", "name", lv.Name)
					return false, err
				}
			}
			return true, nil
		})
}

// createAndWait creates a new LogicalVolume resource and wait until it is fully provisioned
func (s *LogicalVolumeService) createAndWait(ctx context.Context, lv *topolvmv1.LogicalVolume) (*topolvmv1.LogicalVolume, error) {
	if err := s.create(ctx, lv); err != nil {
		return nil, err
	}

	return s.waitForVolumeProvisioning(ctx, lv.Name)
}

// create creates a new LogicalVolume or verifies compatibility with existing one
func (s *LogicalVolumeService) create(ctx context.Context, lv *topolvmv1.LogicalVolume) error {
	existingLV := new(topolvmv1.LogicalVolume)
	err := s.getter.Get(ctx, client.ObjectKey{Name: lv.Name}, existingLV)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}

		if err := s.writer.Create(ctx, lv); err != nil {
			return err
		}
		logger.Info("created LogicalVolume CR", "name", lv.Name, "source", lv.Spec.Source, "accessType", lv.Spec.AccessType)
		return nil
	}

	if !existingLV.IsCompatibleWith(lv) {
		return status.Error(codes.AlreadyExists, "Incompatible LogicalVolume already exists")
	}

	return nil
}

// waitForVolumeProvisioning waits until the volume is fully provisioned or fails
func (s *LogicalVolumeService) waitForVolumeProvisioning(ctx context.Context, lvName string) (*topolvmv1.LogicalVolume, error) {
	var newLV topolvmv1.LogicalVolume
	return &newLV, s.waitForChange(ctx, lvName, fallbackPoll, func(ctx context.Context) (bool, error) {
		if err := s.getter.Get(ctx, client.ObjectKey{Name: lvName}, &newLV); err != nil {
			logger.Error(err, "failed to get LogicalVolume", "name", lvName)
			return false, err
		}

		if newLV.Status.VolumeID != "" {
			logger.Info("LogicalVolume successfully provisioned", "volume_id", newLV.Status.VolumeID)
			return true, nil
		}

		if newLV.Status.Code != codes.OK {
			if err := s.writer.Delete(ctx, &newLV); err != nil {
				// log this error but do not return this error, because newLV.Status.Message is more important
				logger.Error(err, "failed to delete LogicalVolume")
			}
			return false, status.Error(newLV.Status.Code, newLV.Status.Message)
		}

		return false, nil
	})
}
