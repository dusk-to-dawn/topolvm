package controller

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/topolvm/topolvm"
	topolvmv1 "github.com/topolvm/topolvm/api/v1"
	"github.com/topolvm/topolvm/pkg/lvmd/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	storegev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// mockLVMD holds the state shared by the mock lvmd clients below.
var mockLVMD = struct {
	sync.Mutex
	volumes      []*proto.LogicalVolume
	getLVListN   int
	createLVHook func(in *proto.CreateLVRequest) error
}{}

func mockLVMDGetLVListCalls() int {
	mockLVMD.Lock()
	defer mockLVMD.Unlock()
	return mockLVMD.getLVListN
}

func setMockCreateLVHook(hook func(in *proto.CreateLVRequest) error) {
	mockLVMD.Lock()
	defer mockLVMD.Unlock()
	mockLVMD.createLVHook = hook
}

type MockVGServiceClient struct {
}

// GetFreeBytes implements proto.VGServiceClient.
func (MockVGServiceClient) GetFreeBytes(ctx context.Context, in *proto.GetFreeBytesRequest, opts ...grpc.CallOption) (*proto.GetFreeBytesResponse, error) {
	panic("unimplemented")
}

// GetLVList implements proto.VGServiceClient.
func (c MockVGServiceClient) GetLVList(ctx context.Context, in *proto.GetLVListRequest, opts ...grpc.CallOption) (*proto.GetLVListResponse, error) {
	mockLVMD.Lock()
	defer mockLVMD.Unlock()
	mockLVMD.getLVListN++
	return &proto.GetLVListResponse{
		Volumes: slices.Clone(mockLVMD.volumes),
	}, nil
}

// Watch implements proto.VGServiceClient.
func (MockVGServiceClient) Watch(ctx context.Context, in *proto.Empty, opts ...grpc.CallOption) (proto.VGService_WatchClient, error) {
	panic("unimplemented")
}

type MockLVServiceClient struct {
}

// CreateLV implements proto.LVServiceClient.
func (c MockLVServiceClient) CreateLV(ctx context.Context, in *proto.CreateLVRequest, opts ...grpc.CallOption) (*proto.CreateLVResponse, error) {
	mockLVMD.Lock()
	defer mockLVMD.Unlock()
	lv := proto.LogicalVolume{
		Name:      in.Name,
		SizeBytes: in.SizeBytes,
	}
	mockLVMD.volumes = append(mockLVMD.volumes, &lv)
	if mockLVMD.createLVHook != nil {
		if err := mockLVMD.createLVHook(in); err != nil {
			return nil, err
		}
	}
	createResponse := proto.CreateLVResponse{
		Volume: &lv,
	}
	return &createResponse, nil
}

// CreateLVSnapshot implements proto.LVServiceClient.
func (MockLVServiceClient) CreateLVSnapshot(ctx context.Context, in *proto.CreateLVSnapshotRequest, opts ...grpc.CallOption) (*proto.CreateLVSnapshotResponse, error) {
	panic("unimplemented")
}

// RemoveLV implements proto.LVServiceClient.
func (MockLVServiceClient) RemoveLV(ctx context.Context, in *proto.RemoveLVRequest, opts ...grpc.CallOption) (*proto.Empty, error) {
	panic("unimplemented")
}

// ResizeLV implements proto.LVServiceClient.
func (MockLVServiceClient) ResizeLV(ctx context.Context, in *proto.ResizeLVRequest, opts ...grpc.CallOption) (*proto.ResizeLVResponse, error) {
	return &proto.ResizeLVResponse{SizeBytes: in.SizeBytes}, nil
}

var _ = Describe("LogicalVolume controller", func() {
	ctx := context.Background()
	var stopFunc func()
	errCh := make(chan error)
	var vgService MockVGServiceClient
	var lvService MockLVServiceClient

	startReconcilerWithWorkers := func(suffix string, workers int) {
		skipNameValidation := true
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme: scheme,
			Controller: config.Controller{
				SkipNameValidation:      &skipNameValidation,
				MaxConcurrentReconciles: workers,
			},
			Metrics: server.Options{
				BindAddress: "0", // disable metrics
			},
		})
		Expect(err).ToNot(HaveOccurred())

		vgService = MockVGServiceClient{}
		lvService = MockLVServiceClient{}

		reconciler := NewLogicalVolumeReconcilerWithServices(mgr.GetClient(), nodeNameBase+suffix, vgService, lvService)
		err = reconciler.SetupWithManager(mgr)
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(ctx)
		stopFunc = cancel
		go func() {
			errCh <- mgr.Start(ctx)
		}()
		time.Sleep(100 * time.Millisecond)
	}
	startReconciler := func(suffix string) {
		startReconcilerWithWorkers(suffix, 1)
	}

	AfterEach(func() {
		stopFunc()
		Expect(<-errCh).NotTo(HaveOccurred())
	})

	setupResources := func(ctx context.Context, suffix string) topolvmv1.LogicalVolume {
		node := corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: nodeNameBase + suffix,
				Finalizers: []string{
					topolvm.GetNodeFinalizer(),
				},
			},
		}
		err := k8sClient.Create(ctx, &node)
		Expect(err).NotTo(HaveOccurred())

		sc := storegev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: storageClassNameBase + suffix,
			},
			Provisioner: topolvm.GetPluginName(),
		}
		err = k8sClient.Create(ctx, &sc)
		Expect(err).NotTo(HaveOccurred())

		ns := createNamespace()
		pvc := corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcNameBase + suffix,
				Namespace: ns,
				Annotations: map[string]string{
					AnnSelectedNode: node.Name,
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &sc.Name,
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: *resource.NewQuantity(1, resource.BinarySI),
					},
				},
			},
		}
		err = k8sClient.Create(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		lv := topolvmv1.LogicalVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: logicalVolumeNameBase + suffix,
			},
			Spec: topolvmv1.LogicalVolumeSpec{
				NodeName: node.Name,
			},
		}
		err = k8sClient.Create(ctx, &lv)
		Expect(err).NotTo(HaveOccurred())

		return lv
	}

	It("should add finalizer to LogicalVolume", func() {
		startReconciler("-add-finalizer")

		ctx := context.Background()

		// Setup
		lv := setupResources(ctx, "-add-finalizer")

		// Verify
		// ensure LV has finalizer
		Eventually(func(g Gomega) bool {
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(&lv), &lv)
			if err != nil {
				return false
			}
			return controllerutil.ContainsFinalizer(&lv, topolvm.GetLogicalVolumeFinalizer())
		}).Should(BeTrue())
	})

	It("should not add finalizer to LogicalVolume when volume has pendingdeletion annotation", func() {
		startReconciler("-pendingdeletion")

		ctx := context.Background()

		// Setup
		lv := setupResources(ctx, "-pendingdeletion")

		// ensure LV gets finalizer
		Eventually(func(g Gomega) bool {
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(&lv), &lv)
			if err != nil {
				return false
			}
			return controllerutil.ContainsFinalizer(&lv, topolvm.GetLogicalVolumeFinalizer())
		}).Should(BeTrue())

		// Add pending deletion key & remove finalizer
		lv2 := lv.DeepCopy()
		lv2.Annotations = map[string]string{
			topolvm.GetLVPendingDeletionKey(): "true",
		}
		controllerutil.RemoveFinalizer(lv2, topolvm.GetLogicalVolumeFinalizer())

		patch := client.MergeFrom(&lv)
		err := k8sClient.Patch(ctx, lv2, patch)
		Expect(err).NotTo(HaveOccurred())

		// ensure LV finalizer is removed
		Consistently(func(g Gomega) bool {
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(&lv), &lv)
			if err != nil {
				return false
			}
			return !controllerutil.ContainsFinalizer(&lv, topolvm.GetLogicalVolumeFinalizer())
		}, "2s").Should(BeTrue())
	})
	waitForVolumeID := func(ctx context.Context, lv *topolvmv1.LogicalVolume) {
		GinkgoHelper()
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(lv), lv)).To(Succeed())
			g.Expect(lv.Status.VolumeID).To(Equal(string(lv.UID)))
			g.Expect(lv.Status.Code).To(Equal(codes.OK))
		}).Should(Succeed())
	}

	It("should create LV without listing the volumes first", func() {
		setMockCreateLVHook(nil)
		startReconciler("-create")
		ctx := context.Background()
		before := mockLVMDGetLVListCalls()

		lv := setupResources(ctx, "-create")
		waitForVolumeID(ctx, &lv)
		Expect(lv.Status.CurrentSize).NotTo(BeNil())
		Expect(mockLVMDGetLVListCalls()).To(Equal(before))
	})

	It("should adopt an existing LV that lvmd reports as AlreadyExists", func() {
		setMockCreateLVHook(func(in *proto.CreateLVRequest) error {
			return status.Error(codes.AlreadyExists, "exists")
		})
		DeferCleanup(func() { setMockCreateLVHook(nil) })
		startReconciler("-already-exists")
		ctx := context.Background()

		lv := setupResources(ctx, "-already-exists")
		waitForVolumeID(ctx, &lv)
	})

	It("should adopt an existing LV when an older lvmd fails to create it", func() {
		setMockCreateLVHook(func(in *proto.CreateLVRequest) error {
			return status.Error(codes.Internal, "Logical Volume already exists in volume group")
		})
		DeferCleanup(func() { setMockCreateLVHook(nil) })
		startReconciler("-old-lvmd")
		ctx := context.Background()

		lv := setupResources(ctx, "-old-lvmd")
		waitForVolumeID(ctx, &lv)
	})

	It("should record the error when creation fails and the LV does not exist", func() {
		setMockCreateLVHook(func(in *proto.CreateLVRequest) error {
			mockLVMD.volumes = slices.DeleteFunc(mockLVMD.volumes, func(v *proto.LogicalVolume) bool { return v.Name == in.Name })
			return status.Error(codes.ResourceExhausted, "no space")
		})
		DeferCleanup(func() { setMockCreateLVHook(nil) })
		startReconciler("-fail")
		ctx := context.Background()

		lv := setupResources(ctx, "-fail")
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(&lv), &lv)).To(Succeed())
			g.Expect(lv.Status.Code).To(Equal(codes.ResourceExhausted))
			g.Expect(lv.Status.VolumeID).To(BeEmpty())
		}).Should(Succeed())
	})
	It("should create LVs concurrently with several workers", func() {
		const workers = 4
		var mu sync.Mutex
		inFlight, peak := 0, 0
		allIn := make(chan struct{})
		setMockCreateLVHook(func(in *proto.CreateLVRequest) error {
			// Called with the mock's lock held: release it while waiting.
			mockLVMD.Unlock()
			defer mockLVMD.Lock()
			mu.Lock()
			inFlight++
			peak = max(peak, inFlight)
			if inFlight == workers {
				close(allIn)
			}
			mu.Unlock()
			defer func() {
				mu.Lock()
				inFlight--
				mu.Unlock()
			}()
			select {
			case <-allIn:
				return nil
			case <-time.After(20 * time.Second):
				return status.Error(codes.DeadlineExceeded, "creations did not overlap")
			}
		})
		DeferCleanup(func() { setMockCreateLVHook(nil) })
		startReconcilerWithWorkers("-concurrent", workers)
		ctx := context.Background()

		first := setupResources(ctx, "-concurrent")
		lvs := []topolvmv1.LogicalVolume{first}
		for i := 1; i < workers; i++ {
			lv := topolvmv1.LogicalVolume{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%d", first.Name, i)},
				Spec:       topolvmv1.LogicalVolumeSpec{NodeName: first.Spec.NodeName},
			}
			Expect(k8sClient.Create(ctx, &lv)).To(Succeed())
			lvs = append(lvs, lv)
		}
		for i := range lvs {
			waitForVolumeID(ctx, &lvs[i])
		}
		mu.Lock()
		defer mu.Unlock()
		Expect(peak).To(Equal(workers))
	})
	It("should add the finalizer and label before creating the LV, in the same pass", func() {
		var mu sync.Mutex
		var problems []string
		setMockCreateLVHook(func(in *proto.CreateLVRequest) error {
			// The hook runs with the mock's lock held; the API read below does not need it.
			var list topolvmv1.LogicalVolumeList
			if err := k8sClient.List(context.Background(), &list); err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			for _, lv := range list.Items {
				if string(lv.UID) != in.Name {
					continue
				}
				if !controllerutil.ContainsFinalizer(&lv, topolvm.GetLogicalVolumeFinalizer()) {
					problems = append(problems, "no finalizer when CreateLV was called")
				}
				if lv.Labels[topolvm.CreatedbyLabelKey] != topolvm.CreatedbyLabelValue {
					problems = append(problems, "no label when CreateLV was called")
				}
			}
			return nil
		})
		DeferCleanup(func() { setMockCreateLVHook(nil) })
		startReconciler("-one-pass")
		ctx := context.Background()

		lv := setupResources(ctx, "-one-pass")
		waitForVolumeID(ctx, &lv)
		Expect(controllerutil.ContainsFinalizer(&lv, topolvm.GetLogicalVolumeFinalizer())).To(BeTrue())
		Expect(lv.Labels).To(HaveKeyWithValue(topolvm.CreatedbyLabelKey, topolvm.CreatedbyLabelValue))
		mu.Lock()
		defer mu.Unlock()
		Expect(problems).To(BeEmpty())
	})
})

// fakeLVM stands in for the LVM state lvmd reports for one node. It serves the
// logical volumes that already exist there and records the resizes it is asked for.
type fakeLVM struct {
	MockVGServiceClient
	MockLVServiceClient

	volumes     []*proto.LogicalVolume
	resizeCalls int
	createCalls int
	resizeErr   error
}

// CreateLV implements proto.LVServiceClient. Like lvmd, it refuses a name that exists.
func (f *fakeLVM) CreateLV(ctx context.Context, in *proto.CreateLVRequest, opts ...grpc.CallOption) (*proto.CreateLVResponse, error) {
	f.createCalls++
	for _, v := range f.volumes {
		if v.Name == in.Name {
			return nil, status.Errorf(codes.AlreadyExists, "logical volume %s already exists", in.Name)
		}
	}
	v := &proto.LogicalVolume{Name: in.Name, SizeBytes: in.SizeBytes}
	f.volumes = append(f.volumes, v)
	return &proto.CreateLVResponse{Volume: v}, nil
}

// GetLVList implements proto.VGServiceClient.
func (f *fakeLVM) GetLVList(ctx context.Context, in *proto.GetLVListRequest, opts ...grpc.CallOption) (*proto.GetLVListResponse, error) {
	return &proto.GetLVListResponse{
		Volumes: f.volumes,
	}, nil
}

// ResizeLV implements proto.LVServiceClient.
func (f *fakeLVM) ResizeLV(ctx context.Context, in *proto.ResizeLVRequest, opts ...grpc.CallOption) (*proto.ResizeLVResponse, error) {
	f.resizeCalls++
	if f.resizeErr != nil {
		return nil, f.resizeErr
	}
	for _, v := range f.volumes {
		if v.Name == in.Name {
			v.SizeBytes = in.SizeBytes
			return &proto.ResizeLVResponse{SizeBytes: v.SizeBytes}, nil
		}
	}
	return nil, fmt.Errorf("no such volume: %s", in.Name)
}

var _ = Describe("LogicalVolume controller createLV", func() {
	It("should adopt an existing LVM LV at its actual size without resizing it", func() {
		ctx := context.Background()

		// LVM rounds the LV up to an extent boundary, so the two differ.
		const (
			specSizeBytes   = 1 << 30
			actualSizeBytes = 1<<30 + 4<<20
		)

		lv := topolvmv1.LogicalVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: logicalVolumeNameBase + "-existing-lvm-lv",
			},
			Spec: topolvmv1.LogicalVolumeSpec{
				NodeName: nodeNameBase + "-existing-lvm-lv",
				Size:     *resource.NewQuantity(specSizeBytes, resource.BinarySI),
			},
		}
		err := k8sClient.Create(ctx, &lv)
		Expect(err).NotTo(HaveOccurred())

		lvm := &fakeLVM{
			volumes: []*proto.LogicalVolume{
				{Name: string(lv.UID), SizeBytes: actualSizeBytes},
			},
		}
		reconciler := NewLogicalVolumeReconcilerWithServices(k8sClient, lv.Spec.NodeName, lvm, lvm)

		readBack := func() topolvmv1.LogicalVolume {
			var updated topolvmv1.LogicalVolume
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(&lv), &updated)).NotTo(HaveOccurred())
			return updated
		}

		By("recording the size of the LVM LV rather than spec.size")
		err = reconciler.createLV(ctx, GinkgoLogr, &lv)
		Expect(err).NotTo(HaveOccurred())

		adopted := readBack()
		Expect(adopted.Status.VolumeID).To(Equal(string(lv.UID)))
		Expect(adopted.Status.CurrentSize).NotTo(BeNil())
		Expect(adopted.Status.CurrentSize.Value()).To(Equal(int64(actualSizeBytes)))
		Expect(lvm.resizeCalls).To(Equal(0))

		By("leaving the LV alone because it already covers spec.size")
		err = reconciler.expandLV(ctx, GinkgoLogr, &lv)
		Expect(err).NotTo(HaveOccurred())

		Expect(readBack().Status.CurrentSize.Value()).To(Equal(int64(actualSizeBytes)))
		Expect(lvm.resizeCalls).To(Equal(0))
	})

	It("should keep currentSize after adopting an LV smaller than spec.size when the resize fails", func() {
		ctx := context.Background()

		// The LV exists at a smaller size, as after a crash between creating it and
		// recording it while spec.size was raised.
		const (
			specSizeBytes   = 2 << 30
			actualSizeBytes = 1 << 30
		)
		lv := topolvmv1.LogicalVolume{
			ObjectMeta: metav1.ObjectMeta{Name: logicalVolumeNameBase + "-adopt-then-resize-fails"},
			Spec: topolvmv1.LogicalVolumeSpec{
				NodeName: nodeNameBase + "-adopt-then-resize-fails",
				Size:     *resource.NewQuantity(specSizeBytes, resource.BinarySI),
			},
		}
		Expect(k8sClient.Create(ctx, &lv)).To(Succeed())

		lvm := &fakeLVM{
			volumes:   []*proto.LogicalVolume{{Name: string(lv.UID), SizeBytes: actualSizeBytes}},
			resizeErr: status.Error(codes.ResourceExhausted, "no enough space left on VG"),
		}
		reconciler := NewLogicalVolumeReconcilerWithServices(k8sClient, lv.Spec.NodeName, lvm, lvm)

		By("adopting the LV after lvmd reports AlreadyExists, with its real size")
		Expect(reconciler.createLV(ctx, GinkgoLogr, &lv)).To(Succeed())
		Expect(lvm.createCalls).To(Equal(1))
		var updated topolvmv1.LogicalVolume
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(&lv), &updated)).To(Succeed())
		Expect(updated.Status.VolumeID).To(Equal(string(lv.UID)))
		Expect(updated.Status.CurrentSize).NotTo(BeNil())
		Expect(updated.Status.CurrentSize.Value()).To(Equal(int64(actualSizeBytes)))

		By("recording the failed resize without losing currentSize")
		Expect(reconciler.expandLV(ctx, GinkgoLogr, &updated)).NotTo(Succeed())
		Expect(lvm.resizeCalls).To(Equal(1))
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(&lv), &updated)).To(Succeed())
		Expect(updated.Status.Code).To(Equal(codes.ResourceExhausted))
		Expect(updated.Status.CurrentSize).NotTo(BeNil())
		Expect(updated.Status.CurrentSize.Value()).To(Equal(int64(actualSizeBytes)))
	})

	It("should look a snapshot up before creating it and not adopt one after a failure", func() {
		ctx := context.Background()

		source := topolvmv1.LogicalVolume{
			ObjectMeta: metav1.ObjectMeta{Name: logicalVolumeNameBase + "-snap-source"},
			Spec: topolvmv1.LogicalVolumeSpec{
				NodeName: nodeNameBase + "-snap-source",
				Size:     *resource.NewQuantity(1<<30, resource.BinarySI),
			},
		}
		Expect(k8sClient.Create(ctx, &source)).To(Succeed())
		source.Status.VolumeID = string(source.UID)
		source.Status.CurrentSize = resource.NewQuantity(1<<30, resource.BinarySI)
		Expect(k8sClient.Status().Update(ctx, &source)).To(Succeed())

		snap := topolvmv1.LogicalVolume{
			ObjectMeta: metav1.ObjectMeta{Name: logicalVolumeNameBase + "-snap-half-done"},
			Spec: topolvmv1.LogicalVolumeSpec{
				NodeName:   source.Spec.NodeName,
				Size:       *resource.NewQuantity(2<<30, resource.BinarySI),
				Source:     source.Name,
				AccessType: "rw",
			},
		}
		Expect(k8sClient.Create(ctx, &snap)).To(Succeed())

		// lvmd created the clone at the source's size, then failed to extend it.
		lvm := &snapshotFailingLVM{fakeLVM: fakeLVM{}, leaveSize: 1 << 30}
		reconciler := NewLogicalVolumeReconcilerWithServices(k8sClient, snap.Spec.NodeName, lvm, lvm)

		Expect(reconciler.createLV(ctx, GinkgoLogr, &snap)).NotTo(Succeed())
		var updated topolvmv1.LogicalVolume
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(&snap), &updated)).To(Succeed())
		Expect(updated.Status.Code).To(Equal(codes.Internal))
		Expect(updated.Status.VolumeID).To(BeEmpty())
		// One look-up before the attempt, none after it.
		Expect(lvm.listCalls).To(Equal(1))
	})
})

// snapshotFailingLVM fails CreateLVSnapshot after leaving a partial LV behind,
// as lvmd did before it cleaned up after every failed step.
type snapshotFailingLVM struct {
	fakeLVM
	leaveSize int64
	listCalls int
}

// GetLVList implements proto.VGServiceClient.
func (f *snapshotFailingLVM) GetLVList(ctx context.Context, in *proto.GetLVListRequest, opts ...grpc.CallOption) (*proto.GetLVListResponse, error) {
	f.listCalls++
	return f.fakeLVM.GetLVList(ctx, in, opts...)
}

// CreateLVSnapshot implements proto.LVServiceClient.
func (f *snapshotFailingLVM) CreateLVSnapshot(ctx context.Context, in *proto.CreateLVSnapshotRequest, opts ...grpc.CallOption) (*proto.CreateLVSnapshotResponse, error) {
	f.volumes = append(f.volumes, &proto.LogicalVolume{Name: in.Name, SizeBytes: f.leaveSize})
	return nil, status.Error(codes.Internal, "failed to resize snapshot volume")
}

var _ = Describe("LogicalVolume controller createLV with a source", func() {
	It("should fail without panicking when the source has no currentSize yet", func() {
		ctx := context.Background()

		source := topolvmv1.LogicalVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: logicalVolumeNameBase + "-source-without-currentsize",
			},
			Spec: topolvmv1.LogicalVolumeSpec{
				NodeName: nodeNameBase + "-source-without-currentsize",
				Size:     *resource.NewQuantity(1<<30, resource.BinarySI),
			},
		}
		err := k8sClient.Create(ctx, &source)
		Expect(err).NotTo(HaveOccurred())

		// topolvm-node adopted an existing LVM LV, so volumeID is set while currentSize is not.
		source.Status.VolumeID = string(source.UID)
		err = k8sClient.Status().Update(ctx, &source)
		Expect(err).NotTo(HaveOccurred())

		lv := topolvmv1.LogicalVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: logicalVolumeNameBase + "-snapshot-of-sizeless-source",
			},
			Spec: topolvmv1.LogicalVolumeSpec{
				NodeName:   source.Spec.NodeName,
				Size:       *resource.NewQuantity(1<<30, resource.BinarySI),
				Source:     source.Name,
				AccessType: "ro",
			},
		}
		err = k8sClient.Create(ctx, &lv)
		Expect(err).NotTo(HaveOccurred())

		reconciler := NewLogicalVolumeReconcilerWithServices(
			k8sClient, lv.Spec.NodeName, &fakeLVM{}, &fakeLVM{})

		err = reconciler.createLV(ctx, GinkgoLogr, &lv)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("status.currentSize"))
	})
})
