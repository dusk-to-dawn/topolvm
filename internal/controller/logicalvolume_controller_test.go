package controller

import (
	"context"
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

	startReconciler := func(suffix string) {
		skipNameValidation := true
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme: scheme,
			Controller: config.Controller{
				SkipNameValidation: &skipNameValidation,
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
})
