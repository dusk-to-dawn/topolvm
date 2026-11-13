package lvmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	// lockPollInterval is how often a waiting lvmd tries the lock again.
	lockPollInterval = 200 * time.Millisecond
	// lockWaitLogInterval is how often a waiting lvmd says what it waits for.
	lockWaitLogInterval = 5 * time.Second
)

// ProcessLockPath returns the lock file of the lvmd that serves socketPath.
func ProcessLockPath(socketPath string) string {
	return socketPath + ".lock"
}

// LockProcess takes the lock that makes this process the only lvmd for
// socketPath on the host, and holds it until release is called or the process
// exits, whichever way it exits: the kernel drops the lock with the last
// descriptor of the lock file.
//
// The lock is an exclusive flock on the file next to the socket, so lvmd
// processes in different containers contend for it when they mount the
// socket's directory from the host, as topolvm-node does to reach lvmd.
// What one lvmd knows of the changes in progress is its own: two lvmd
// processes on one volume group would not count each other's reservations,
// and one's start-up sweep could remove the other's snapshot builds. So an
// lvmd waits here, before it touches LVM or its socket, for as long as another
// holds the lock, and says every lockWaitLogInterval what it waits for. It
// gives up only when ctx ends. An lvmd older than this one does not take the
// lock, so it can still run next to this one.
func LockProcess(ctx context.Context, socketPath string) (release func(), err error) {
	logger := log.FromContext(ctx)
	path := ProcessLockPath(socketPath)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to open the lvmd lock %s: %w", path, err)
	}
	start := time.Now()
	var lastLog time.Time
	for {
		err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			_ = f.Close()
			return nil, fmt.Errorf("failed to lock %s: %w", path, err)
		}
		if time.Since(lastLog) >= lockWaitLogInterval {
			holder, _ := os.ReadFile(path)
			logger.Info("waiting for the lvmd that holds the lock to exit; this lvmd serves nothing until then",
				"lock", path, "holder", strings.TrimSpace(string(holder)), "waited", time.Since(start).Round(time.Second).String())
			lastLog = time.Now()
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, ctx.Err()
		case <-time.After(lockPollInterval):
		}
	}

	// Say who holds the lock, for the log of an lvmd that waits for it.
	host, _ := os.Hostname()
	holder := fmt.Sprintf("%s pid %d since %s\n", host, os.Getpid(), time.Now().UTC().Format(time.RFC3339))
	if err := f.Truncate(0); err == nil {
		_, _ = f.WriteAt([]byte(holder), 0)
	}
	if !lastLog.IsZero() {
		logger.Info("took the lvmd lock", "lock", path, "waited", time.Since(start).Round(time.Millisecond).String())
	}
	return func() { _ = f.Close() }, nil
}
