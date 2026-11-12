package k8s

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/topolvm/topolvm"
	topolvmlegacyv1 "github.com/topolvm/topolvm/api/legacy/v1"
	topolvmv1 "github.com/topolvm/topolvm/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// promptly is how soon after the LogicalVolume changes a wait must return.
// The polling it replaces waited up to several seconds.
const promptly = 500 * time.Millisecond

type lvTestEnv struct {
	svc *LogicalVolumeService
	// k8s writes like topolvm-node would.
	k8s client.Client
	// lvGets counts GET requests for LogicalVolumes made by the service.
	lvGets atomic.Int64
}

type countingTransport struct {
	next  http.RoundTripper
	count func(*http.Request)
}

func (t countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.count(req)
	return t.next.RoundTrip(req)
}

func startLVTestEnv(t *testing.T) *lvTestEnv {
	t.Helper()
	testEnv := &envtest.Environment{
		CRDDirectoryPaths:           []string{filepath.Join("..", "..", "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing:       true,
		DownloadBinaryAssets:        true,
		DownloadBinaryAssetsVersion: "v" + os.Getenv("ENVTEST_KUBERNETES_VERSION"),
		BinaryAssetsDirectory:       os.Getenv("ENVTEST_ASSETS_DIR"),
	}
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testEnv.Stop() })

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, topolvmv1.AddToScheme, topolvmlegacyv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}

	e := &lvTestEnv{}
	svcCfg := rest.CopyConfig(cfg)
	svcCfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return countingTransport{next: rt, count: func(req *http.Request) {
			// Single-object reads; list and watch requests of the informer have no name.
			if req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/logicalvolumes/") {
				e.lvGets.Add(1)
			}
		}}
	})
	mgr, err := ctrl.NewManager(svcCfg, ctrl.Options{Scheme: scheme, Metrics: server.Options{BindAddress: "0"}})
	if err != nil {
		t.Fatal(err)
	}
	e.svc, err = NewLogicalVolumeService(mgr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("cache did not sync")
	}
	e.k8s, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// waitForLV waits until the named LogicalVolume exists and returns it.
func (e *lvTestEnv) waitForLV(t *testing.T, name string) *topolvmv1.LogicalVolume {
	t.Helper()
	lv := &topolvmv1.LogicalVolume{}
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := e.k8s.Get(context.Background(), client.ObjectKey{Name: name}, lv)
		if err == nil {
			return lv
		}
		if time.Now().After(deadline) {
			t.Fatalf("LogicalVolume %s was not created: %v", name, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type result struct {
	at  time.Time
	err error
}

func TestLogicalVolumeServiceWaits(t *testing.T) {
	if os.Getenv("ENVTEST_KUBERNETES_VERSION") == "" {
		t.Skip("ENVTEST_KUBERNETES_VERSION is not set")
	}
	e := startLVTestEnv(t)
	ctx := context.Background()

	t.Run("CreateVolume returns promptly once the volume is created", func(t *testing.T) {
		done := make(chan result, 1)
		go func() {
			_, err := e.svc.CreateVolume(ctx, "node1", "dc", "", "create-ok", "", 1<<30)
			done <- result{time.Now(), err}
		}()
		lv := e.waitForLV(t, "create-ok")
		if !controllerutil.ContainsFinalizer(lv, topolvm.GetLogicalVolumeFinalizer()) {
			t.Error("LogicalVolume was created without its finalizer")
		}
		if lv.Labels[topolvm.CreatedbyLabelKey] != topolvm.CreatedbyLabelValue {
			t.Error("LogicalVolume was created without its label")
		}

		// Finish between two of the old polls (at 1 s and 3 s).
		time.Sleep(1500 * time.Millisecond)
		gets := e.lvGets.Load()
		lv.Status.VolumeID = "vol-create-ok"
		lv.Status.CurrentSize = resource.NewQuantity(1<<30, resource.BinarySI)
		if err := e.k8s.Status().Update(ctx, lv); err != nil {
			t.Fatal(err)
		}
		updated := time.Now()

		res := <-done
		if res.err != nil {
			t.Fatal(res.err)
		}
		d := res.at.Sub(updated)
		t.Logf("CreateVolume returned %v after the volume was created", d)
		if d > promptly {
			t.Errorf("CreateVolume returned %v after the volume was created", d)
		}
		if n := e.lvGets.Load() - gets; n > 0 {
			t.Errorf("waiting made %d GET requests to the API server", n)
		}
	})

	t.Run("CreateVolume returns the node's error promptly", func(t *testing.T) {
		done := make(chan result, 1)
		go func() {
			_, err := e.svc.CreateVolume(ctx, "node1", "dc", "", "create-fail", "", 1<<30)
			done <- result{time.Now(), err}
		}()
		lv := e.waitForLV(t, "create-fail")
		time.Sleep(1500 * time.Millisecond)
		lv.Status.Code = codes.ResourceExhausted
		lv.Status.Message = "no space"
		if err := e.k8s.Status().Update(ctx, lv); err != nil {
			t.Fatal(err)
		}
		updated := time.Now()

		res := <-done
		if status.Code(res.err) != codes.ResourceExhausted {
			t.Fatalf("got %v, want ResourceExhausted", res.err)
		}
		d := res.at.Sub(updated)
		t.Logf("CreateVolume returned %v after the node reported the error", d)
		if d > promptly {
			t.Errorf("CreateVolume returned %v after the node reported the error", d)
		}
	})

	t.Run("CreateVolume still times out with its context", func(t *testing.T) {
		tctx, cancel := context.WithTimeout(ctx, 700*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := e.svc.CreateVolume(tctx, "node1", "dc", "", "create-timeout", "", 1<<30)
		if err == nil {
			t.Fatal("CreateVolume succeeded without the node")
		}
		if d := time.Since(start); d > 700*time.Millisecond+promptly {
			t.Errorf("CreateVolume took %v to honour a 700ms deadline", d)
		}
	})

	t.Run("ExpandVolume returns promptly once the volume is resized", func(t *testing.T) {
		lv := e.waitForLV(t, "create-ok")
		done := make(chan result, 1)
		go func() {
			_, err := e.svc.ExpandVolume(ctx, "vol-create-ok", 2<<30)
			done <- result{time.Now(), err}
		}()
		deadline := time.Now().Add(10 * time.Second)
		for lv.Spec.Size.Value() != 2<<30 {
			if time.Now().After(deadline) {
				t.Fatal("spec.size was not updated")
			}
			time.Sleep(20 * time.Millisecond)
			if err := e.k8s.Get(ctx, client.ObjectKeyFromObject(lv), lv); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(1500 * time.Millisecond)
		lv.Status.CurrentSize = resource.NewQuantity(2<<30, resource.BinarySI)
		if err := e.k8s.Status().Update(ctx, lv); err != nil {
			t.Fatal(err)
		}
		updated := time.Now()

		res := <-done
		if res.err != nil {
			t.Fatal(res.err)
		}
		d := res.at.Sub(updated)
		t.Logf("ExpandVolume returned %v after the volume was resized", d)
		if d > promptly {
			t.Errorf("ExpandVolume returned %v after the volume was resized", d)
		}
	})

	t.Run("DeleteVolume returns promptly once the volume is removed", func(t *testing.T) {
		lv := &topolvmv1.LogicalVolume{}
		lv.Name = "delete-me"
		lv.Spec = topolvmv1.LogicalVolumeSpec{Name: "delete-me", NodeName: "node1", Size: *resource.NewQuantity(1<<30, resource.BinarySI)}
		controllerutil.AddFinalizer(lv, topolvm.GetLogicalVolumeFinalizer())
		if err := e.k8s.Create(ctx, lv); err != nil {
			t.Fatal(err)
		}
		lv.Status.VolumeID = "vol-delete-me"
		if err := e.k8s.Status().Update(ctx, lv); err != nil {
			t.Fatal(err)
		}
		// Let the service's cache see the volume ID.
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := e.svc.GetVolume(ctx, "vol-delete-me"); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("volume ID not found")
			}
			time.Sleep(20 * time.Millisecond)
		}

		done := make(chan result, 1)
		go func() {
			err := e.svc.DeleteVolume(ctx, "vol-delete-me")
			done <- result{time.Now(), err}
		}()
		// Finish between two of the old polls (at 1.5 s and 3.1 s).
		time.Sleep(1700 * time.Millisecond)
		select {
		case res := <-done:
			t.Fatalf("DeleteVolume returned before the volume was removed: %v", res.err)
		default:
		}
		if err := e.k8s.Get(ctx, client.ObjectKeyFromObject(lv), lv); err != nil {
			t.Fatal(err)
		}
		if lv.DeletionTimestamp == nil {
			t.Fatal("LogicalVolume was not marked for deletion")
		}
		controllerutil.RemoveFinalizer(lv, topolvm.GetLogicalVolumeFinalizer())
		if err := e.k8s.Update(ctx, lv); err != nil {
			t.Fatal(err)
		}
		updated := time.Now()

		res := <-done
		if res.err != nil {
			t.Fatal(res.err)
		}
		d := res.at.Sub(updated)
		t.Logf("DeleteVolume returned %v after the volume was removed", d)
		if d > promptly {
			t.Errorf("DeleteVolume returned %v after the volume was removed", d)
		}
	})
}
