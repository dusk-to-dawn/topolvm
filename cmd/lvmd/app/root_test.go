package app

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/topolvm/topolvm/internal/lvmd/command"
	"github.com/topolvm/topolvm/internal/lvmd/testutils/fakelvm"
	"sigs.k8s.io/yaml"
)

const (
	helperConfigEnv = "TOPOLVM_LVMD_HELPER_CONFIG"
	testGiB         = uint64(1) << 30
)

// TestLVMDProcessHelper runs lvmd with the configuration named by
// helperConfigEnv until it receives SIGTERM. It runs only as a subprocess of
// TestOneLVMDPerSocket.
func TestLVMDProcessHelper(t *testing.T) {
	path := os.Getenv(helperConfigEnv)
	if path == "" {
		t.Skip("helper process only")
	}
	cfgFilePath = path
	metricsBindAddress = ""
	if err := subMain(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "lvmd:", err)
		os.Exit(1)
	}
	os.Exit(0)
}

type lvmdProcess struct {
	cmd *exec.Cmd
	log string
}

func startLVMD(t *testing.T, cfg string) *lvmdProcess {
	t.Helper()
	p := &lvmdProcess{log: filepath.Join(t.TempDir(), "lvmd.log")}
	logFile, err := os.Create(p.log)
	if err != nil {
		t.Fatal(err)
	}
	p.cmd = exec.Command(os.Args[0], "-test.run=^TestLVMDProcessHelper$")
	p.cmd.Env = append(os.Environ(), helperConfigEnv+"="+cfg)
	p.cmd.Stdout = logFile
	p.cmd.Stderr = logFile
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = p.cmd.Process.Kill()
		_ = p.cmd.Wait()
		_ = logFile.Close()
	})
	return p
}

func (p *lvmdProcess) logText(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(p.log)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// stop sends SIGTERM, as kubelet does, and waits for the process to exit.
func (p *lvmdProcess) stop(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("lvmd exited with %v:\n%s", err, p.logText(t))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("lvmd did not exit after SIGTERM")
	}
}

func socketInode(sock string) (uint64, bool) {
	fi, err := os.Stat(sock)
	if err != nil {
		return 0, false
	}
	return fi.Sys().(*syscall.Stat_t).Ino, true
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestOneLVMDPerSocket runs two lvmd processes for one socket, as two
// DaemonSets on one node would during a hand-over. The second must not sweep
// or serve anything until the first has exited.
func TestOneLVMDPerSocket(t *testing.T) {
	const vgName, poolName = "fakevg", "fakepool"
	leftover := func(n string) string { return n + ".creating-0123456789abcdef" }
	var prefix []string
	f := fakelvm.Start(t, fakelvm.State{
		VGs: []*fakelvm.VG{{
			Name: vgName,
			Size: 100 * testGiB,
			Free: 60 * testGiB,
			LVs: []*fakelvm.LV{
				{Name: poolName, Size: 40 * testGiB, IsPool: true},
				{Name: leftover("first"), Size: testGiB, Pool: poolName, Minor: 100},
			},
		}},
		NextMinor: 200,
	}, fakelvm.Options{}, func() []string { return nil }, func(p []string) { prefix = p })
	lvExists := func(name string) bool {
		for _, vg := range f.State(t).VGs {
			for _, lv := range vg.LVs {
				if lv.Name == name {
					return true
				}
			}
		}
		return false
	}

	dir := t.TempDir()
	sock := filepath.Join(dir, "lvmd.sock")
	cfg, err := yaml.Marshal(map[string]any{
		"socket-name":        sock,
		"lvm-command-prefix": prefix,
		"device-classes": []map[string]any{{
			"name": "thin", "volume-group": vgName, "default": true, "type": "thin",
			"thin-pool": map[string]any{"name": poolName, "overprovision-ratio": 2.0},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "lvmd.yaml")
	if err := os.WriteFile(cfgPath, cfg, 0o600); err != nil {
		t.Fatal(err)
	}

	first := startLVMD(t, cfgPath)
	waitFor(t, "the first lvmd to serve", func() bool { _, ok := socketInode(sock); return ok })
	firstInode, _ := socketInode(sock)
	if lvExists(leftover("first")) {
		t.Fatal("the first lvmd did not sweep the build left before it started")
	}

	// A build appears that the second lvmd's sweep would remove.
	prev := command.LVMCommandPrefix()
	command.SetLVMCommandPrefix(prefix)
	t.Cleanup(func() { command.SetLVMCommandPrefix(prev) })
	ctx := context.Background()
	vgs, err := command.ListVolumeGroups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	vg, err := command.SearchVolumeGroupList(vgs, vgName)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := vg.FindPool(ctx, poolName)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.CreateVolume(ctx, leftover("second"), testGiB, nil, 0, "", nil); err != nil {
		t.Fatal(err)
	}

	second := startLVMD(t, cfgPath)
	const waiting = "waiting for the lvmd that holds the lock"
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(second.logText(t), waiting) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(time.Second)
	if !strings.Contains(second.logText(t), waiting) {
		t.Errorf("the second lvmd did not say that it waits:\n%s", second.logText(t))
	}
	if !lvExists(leftover("second")) {
		t.Error("the second lvmd swept while the first was still running")
	}
	if inode, ok := socketInode(sock); !ok || inode != firstInode {
		t.Error("the second lvmd replaced the first one's socket while the first was still running")
	}

	first.stop(t)
	waitFor(t, "the second lvmd to serve", func() bool {
		inode, ok := socketInode(sock)
		return ok && inode != firstInode
	})
	if lvExists(leftover("second")) {
		t.Error("the second lvmd did not sweep once it had the lock")
	}
	if !strings.Contains(second.logText(t), "took the lvmd lock") {
		t.Errorf("the second lvmd did not log taking the lock:\n%s", second.logText(t))
	}
	second.stop(t)
}
