package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/topolvm/topolvm"
	topolvmv1 "github.com/topolvm/topolvm/api/v1"
	"github.com/topolvm/topolvm/pkg/lvmd/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/config"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// slowLVM stands in for lvmd during a burst: CreateLV takes a while, as
// lvcreate does, and refuses a name that exists, as lvmd does.
type slowLVM struct {
	MockVGServiceClient
	MockLVServiceClient

	mu            sync.Mutex
	volumes       map[string]*proto.LogicalVolume
	createCalls   int
	alreadyExists int
}

func (f *slowLVM) CreateLV(ctx context.Context, in *proto.CreateLVRequest, opts ...grpc.CallOption) (*proto.CreateLVResponse, error) {
	f.mu.Lock()
	f.createCalls++
	if _, ok := f.volumes[in.Name]; ok {
		f.alreadyExists++
		f.mu.Unlock()
		return nil, status.Errorf(codes.AlreadyExists, "logical volume %s already exists", in.Name)
	}
	f.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
	v := &proto.LogicalVolume{Name: in.Name, SizeBytes: in.SizeBytes}
	f.mu.Lock()
	f.volumes[in.Name] = v
	f.mu.Unlock()
	return &proto.CreateLVResponse{Volume: v}, nil
}

func (f *slowLVM) GetLVList(ctx context.Context, in *proto.GetLVListRequest, opts ...grpc.CallOption) (*proto.GetLVListResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	res := &proto.GetLVListResponse{}
	for _, v := range f.volumes {
		res.Volumes = append(res.Volumes, v)
	}
	return res, nil
}

func (f *slowLVM) counts() (creates, alreadyExists int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.createCalls, f.alreadyExists
}

// errorRecorder is a logr sink that keeps the messages logged at error level.
type errorRecorder struct {
	mu   *sync.Mutex
	errs *[]string
}

func (s errorRecorder) Init(logr.RuntimeInfo)          {}
func (s errorRecorder) Enabled(int) bool               { return true }
func (s errorRecorder) Info(int, string, ...any)       {}
func (s errorRecorder) WithValues(...any) logr.LogSink { return s }
func (s errorRecorder) WithName(string) logr.LogSink   { return s }
func (s errorRecorder) Error(err error, msg string, _ ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	*s.errs = append(*s.errs, fmt.Sprintf("%s: %v", msg, err))
}

var _ = Describe("LogicalVolume controller in a burst", func() {
	// runBurst starts the reconciler with 8 workers, creates 12 LogicalVolumes
	// at once, as the controller does for a burst of claims, and waits until
	// each has its volume ID. It returns the fake lvmd and the error-level
	// messages logged meanwhile.
	runBurst := func(suffix string, withFinalizerAndLabel bool) (*slowLVM, []string) {
		var mu sync.Mutex
		var errs []string
		skipNameValidation := true
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme: scheme,
			Logger: logr.New(errorRecorder{mu: &mu, errs: &errs}),
			Controller: config.Controller{
				SkipNameValidation:      &skipNameValidation,
				MaxConcurrentReconciles: 8,
			},
			Metrics: server.Options{BindAddress: "0"},
		})
		Expect(err).NotTo(HaveOccurred())
		lvm := &slowLVM{volumes: map[string]*proto.LogicalVolume{}}
		nodeName := nodeNameBase + suffix
		Expect(NewLogicalVolumeReconcilerWithServices(mgr.GetClient(), nodeName, lvm, lvm).SetupWithManager(mgr)).To(Succeed())
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error)
		go func() { done <- mgr.Start(ctx) }()
		DeferCleanup(func() {
			cancel()
			Expect(<-done).NotTo(HaveOccurred())
		})
		Expect(mgr.GetCache().WaitForCacheSync(ctx)).To(BeTrue())

		lvs := make([]topolvmv1.LogicalVolume, 12)
		for i := range lvs {
			lvs[i] = topolvmv1.LogicalVolume{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s%s-%d", logicalVolumeNameBase, suffix, i)},
				Spec: topolvmv1.LogicalVolumeSpec{
					NodeName: nodeName,
					Size:     *resource.NewQuantity(1<<30, resource.BinarySI),
				},
			}
			if withFinalizerAndLabel {
				lvs[i].Finalizers = []string{topolvm.GetLogicalVolumeFinalizer()}
				lvs[i].Labels = map[string]string{topolvm.CreatedbyLabelKey: topolvm.CreatedbyLabelValue}
			}
			Expect(k8sClient.Create(ctx, &lvs[i])).To(Succeed())
		}
		for i := range lvs {
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(&lvs[i]), &lvs[i])).To(Succeed())
				g.Expect(lvs[i].Status.VolumeID).To(Equal(string(lvs[i].UID)))
			}).Should(Succeed())
		}
		// Let any pass still queued for these objects run.
		time.Sleep(500 * time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
		return lvm, append([]string(nil), errs...)
	}

	// The upstream controller creates LogicalVolumes without the finalizer and
	// the label, so the node patches them in the pass that creates the LV.
	It("should ask lvmd once per volume when it adds the finalizer and label itself", func() {
		lvm, errs := runBurst("-burst-patched", false)
		creates, exists := lvm.counts()
		GinkgoWriter.Printf("objects patched by the node: %d CreateLV calls for 12 volumes, %d AlreadyExists, %d error lines\n", creates, exists, len(errs))
		Expect(exists).To(BeZero())
		Expect(creates).To(Equal(12))
		Expect(errs).To(BeEmpty())
	})

	// This fork's controller creates them with both, so the node does not patch.
	It("should ask lvmd once per volume when the controller set the finalizer and label", func() {
		lvm, errs := runBurst("-burst-preset", true)
		creates, exists := lvm.counts()
		GinkgoWriter.Printf("objects created with finalizer and label: %d CreateLV calls for 12 volumes, %d AlreadyExists, %d error lines\n", creates, exists, len(errs))
		Expect(exists).To(BeZero())
		Expect(creates).To(Equal(12))
		Expect(errs).To(BeEmpty())
	})

	// newPresetLV creates a LogicalVolume as this fork's controller does.
	newPresetLV := func(ctx context.Context, suffix string) topolvmv1.LogicalVolume {
		lv := topolvmv1.LogicalVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:       logicalVolumeNameBase + suffix,
				Finalizers: []string{topolvm.GetLogicalVolumeFinalizer()},
				Labels:     map[string]string{topolvm.CreatedbyLabelKey: topolvm.CreatedbyLabelValue},
			},
			Spec: topolvmv1.LogicalVolumeSpec{
				NodeName: nodeNameBase + suffix,
				Size:     *resource.NewQuantity(1<<30, resource.BinarySI),
			},
		}
		Expect(k8sClient.Create(ctx, &lv)).To(Succeed())
		return lv
	}

	It("should requeue without an error when its status update conflicts", func() {
		ctx := context.Background()
		lv := newPresetLV(ctx, "-status-conflict")
		conflicted := false
		direct, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
		Expect(err).NotTo(HaveOccurred())
		c := interceptor.NewClient(direct, interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if !conflicted {
					conflicted = true
					return apierrs.NewConflict(schema.GroupResource{Group: "topolvm.io", Resource: "logicalvolumes"},
						obj.GetName(), errors.New("the object has been modified"))
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		})
		lvm := &slowLVM{volumes: map[string]*proto.LogicalVolume{}}
		r := NewLogicalVolumeReconcilerWithServices(c, lv.Spec.NodeName, lvm, lvm)
		var mu sync.Mutex
		var errs []string
		ctx = crlog.IntoContext(ctx, logr.New(errorRecorder{mu: &mu, errs: &errs}))
		req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(&lv)}

		res, err := r.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))

		// The next pass finds the LV the first one created, and adopts it.
		_, err = r.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		creates, exists := lvm.counts()
		Expect(creates).To(Equal(2))
		Expect(exists).To(Equal(1))
		Expect(k8sClient.Get(ctx, req.NamespacedName, &lv)).To(Succeed())
		Expect(lv.Status.VolumeID).To(Equal(string(lv.UID)))
		Expect(lv.Status.Code).To(Equal(codes.OK))
		mu.Lock()
		defer mu.Unlock()
		Expect(errs).To(BeEmpty())
	})

	It("should fail loudly when lvmd reports AlreadyExists for a volume it does not list", func() {
		ctx := context.Background()
		lv := newPresetLV(ctx, "-exists-unlisted")
		lvm := &unlistedLVM{}
		r := NewLogicalVolumeReconcilerWithServices(k8sClient, lv.Spec.NodeName, lvm, lvm)
		var mu sync.Mutex
		var errs []string
		ctx = crlog.IntoContext(ctx, logr.New(errorRecorder{mu: &mu, errs: &errs}))

		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(&lv)})
		Expect(status.Code(err)).To(Equal(codes.AlreadyExists))
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(&lv), &lv)).To(Succeed())
		Expect(lv.Status.Code).To(Equal(codes.AlreadyExists))
		Expect(lv.Status.VolumeID).To(BeEmpty())
		mu.Lock()
		defer mu.Unlock()
		Expect(errs).To(ContainElement(ContainSubstring("failed to create LV")))
	})
})

// unlistedLVM reports every name as existing, and lists none.
type unlistedLVM struct {
	MockVGServiceClient
	MockLVServiceClient
}

func (unlistedLVM) CreateLV(ctx context.Context, in *proto.CreateLVRequest, opts ...grpc.CallOption) (*proto.CreateLVResponse, error) {
	return nil, status.Errorf(codes.AlreadyExists, "logical volume %s already exists", in.Name)
}

func (unlistedLVM) GetLVList(ctx context.Context, in *proto.GetLVListRequest, opts ...grpc.CallOption) (*proto.GetLVListResponse, error) {
	return &proto.GetLVListResponse{}, nil
}
