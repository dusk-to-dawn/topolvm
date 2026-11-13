package lvmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// lockAsync takes the lock for sock in a goroutine and reports the result.
func lockAsync(ctx context.Context, sock string) <-chan func() {
	got := make(chan func(), 1)
	go func() {
		release, err := LockProcess(ctx, sock)
		if err != nil {
			close(got)
			return
		}
		got <- release
	}()
	return got
}

func TestLockProcessWaitsForTheHolder(t *testing.T) {
	interval := lockWaitLogInterval
	lockWaitLogInterval = 100 * time.Millisecond
	t.Cleanup(func() { lockWaitLogInterval = interval })

	sock := filepath.Join(t.TempDir(), "lvmd.sock")
	first, err := LockProcess(context.Background(), sock)
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var lines []string
	logger := funcr.New(func(prefix, args string) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, args)
	}, funcr.Options{})
	ctx, cancel := context.WithCancel(log.IntoContext(context.Background(), logger))
	defer cancel()
	second := lockAsync(ctx, sock)

	select {
	case <-second:
		t.Fatal("a second lvmd took the lock while the first held it")
	case <-time.After(700 * time.Millisecond):
	}
	mu.Lock()
	waitLines := 0
	for _, l := range lines {
		if strings.Contains(l, "waiting for the lvmd that holds the lock") && strings.Contains(l, fmt.Sprintf("pid %d", os.Getpid())) {
			waitLines++
		}
	}
	mu.Unlock()
	if waitLines < 2 {
		t.Errorf("the waiting lvmd logged %d lines naming the holder, want one every interval: %q", waitLines, lines)
	}

	first()
	select {
	case release, ok := <-second:
		if !ok {
			t.Fatal("the second lvmd failed to take the lock")
		}
		release()
	case <-time.After(2 * time.Second):
		t.Fatal("the second lvmd did not take the lock after the first released it")
	}
}

func TestLockProcessGivesUpOnlyWhenCancelled(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "lvmd.sock")
	first, err := LockProcess(context.Background(), sock)
	if err != nil {
		t.Fatal(err)
	}
	defer first()

	ctx, cancel := context.WithTimeout(logr.NewContext(context.Background(), logr.Discard()), 500*time.Millisecond)
	defer cancel()
	_, err = LockProcess(ctx, sock)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v, want the context's error", err)
	}
}

// TestLockProcessHelper takes the lock and holds it until it is killed. It
// runs only as the subprocess of TestLockProcessReleasedOnCrash.
func TestLockProcessHelper(t *testing.T) {
	sock := os.Getenv("TOPOLVM_LOCK_HELPER_SOCKET")
	if sock == "" {
		t.Skip("helper process only")
	}
	if _, err := LockProcess(context.Background(), sock); err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
	fmt.Println("locked")
	time.Sleep(time.Minute)
	os.Exit(0)
}

func TestLockProcessReleasedOnCrash(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "lvmd.sock")
	cmd := exec.Command(os.Args[0], "-test.run=^TestLockProcessHelper$")
	cmd.Env = append(os.Environ(), "TOPOLVM_LOCK_HELPER_SOCKET="+sock)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "locked" {
		t.Fatalf("helper: %q, %v", line, err)
	}

	ctx, cancel := context.WithCancel(logr.NewContext(context.Background(), logr.Discard()))
	defer cancel()
	got := lockAsync(ctx, sock)
	select {
	case <-got:
		t.Fatal("took the lock while another process held it")
	case <-time.After(500 * time.Millisecond):
	}

	// A crash: the kernel drops the lock with the process's descriptors.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case release, ok := <-got:
		if !ok {
			t.Fatal("failed to take the lock")
		}
		release()
	case <-time.After(5 * time.Second):
		t.Fatal("the lock was not released when its holder was killed")
	}
}
