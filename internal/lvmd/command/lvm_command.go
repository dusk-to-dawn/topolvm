package command

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	verbosityLVMStateUpdate   = 1
	verbosityLVMStateNoUpdate = 4
)

// DefaultMaxConcurrentCommands is the default bound on the number of LVM
// commands run at once.
const DefaultMaxConcurrentCommands = 16

var (
	lvmCommandPrefix = []string{"/usr/bin/nsenter", "-m", "-u", "-i", "-n", "-p", "-t", "1", "/sbin/lvm"}

	// commandSlots holds one token per LVM command that may run at once.
	commandSlots = make(chan struct{}, DefaultMaxConcurrentCommands)
)

// SetMaxConcurrentCommands bounds the number of LVM commands run at once, so
// that a burst of requests cannot start an unbounded number of processes that
// all contend for the same volume group lock. Calls beyond the bound wait for
// a running command to finish, or for their context to end. A value of zero
// or less removes the bound. It must be called before any command is run.
func SetMaxConcurrentCommands(n int) {
	if n <= 0 {
		commandSlots = nil
		return
	}
	commandSlots = make(chan struct{}, n)
}

// acquireCommandSlot waits for a free command slot and returns the function
// that frees it.
func acquireCommandSlot(ctx context.Context) (func(), error) {
	slots := commandSlots
	if slots == nil {
		return func() {}, nil
	}
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("waiting to run an lvm command: %w", context.Cause(ctx))
	}
	var once sync.Once
	return func() { once.Do(func() { <-slots }) }, nil
}

// SetLVMPath sets the path to the lvm command. Note that this function must not
// be called together with SetLVMCommandPrefix.
func SetLVMPath(path string) {
	if path != "" {
		lvmCommandPrefix[len(lvmCommandPrefix)-1] = path
	}
}

// SetLVMCommandPrefix sets a list of strings necessary to run a LVM command.
// For example, if it's X, `/sbin/lvm lvcreate ...` will be run as `X /sbin/lvm
// lvcreate ...`.  This function must not be called together with SetLVMPath.
func SetLVMCommandPrefix(prefix []string) {
	lvmCommandPrefix = prefix
}

// LVMCommandPrefix returns a copy of the prefix used to run LVM commands.
func LVMCommandPrefix() []string {
	return slices.Clone(lvmCommandPrefix)
}

// callLVM calls lvm sub-commands and prints the output to the log.
func callLVM(ctx context.Context, args ...string) error {
	return callLVMInto(ctx, nil, verbosityLVMStateUpdate, args...)
}

// callLVMInto calls lvm sub-commands and decodes the output via JSON into the provided struct pointer.
// if the struct pointer is nil, the output will be printed to the log instead.
func callLVMInto(ctx context.Context, into any, logVerbosity int, args ...string) error {
	output, err := callLVMStreamed(ctx, logVerbosity, args...)
	if err != nil {
		return fmt.Errorf("failed to execute command: %v", err)
	}

	// if we don't decode the output into a struct, we can still log the command results from stdout.
	if into == nil {
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			log.FromContext(ctx).Info(strings.TrimSpace(scanner.Text()))
		}
		err = scanner.Err()
	} else {
		err = json.NewDecoder(output).Decode(&into)
	}
	closeErr := output.Close()

	return errors.Join(closeErr, err)
}

// callLVMStreamed calls lvm sub-commands and returns the output as a ReadCloser.
// The caller is responsible for closing the ReadCloser, which will cause the command to complete.
// Not calling close on this method will result in a resource leak.
func callLVMStreamed(ctx context.Context, logVerbosity int, args ...string) (io.ReadCloser, error) {
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithCallDepth(1))
	wholeCommand := slices.Concat(lvmCommandPrefix, args)
	// Use CommandContext so kubelet timing out on an RPC (default 2 min
	// csiTimeout) cancels the underlying vgs/lvs/... subprocess instead
	// of leaving it running on the node. exec.Command ignores ctx and
	// accumulates orphans under repeated timeout scenarios (#1163).
	cmd := exec.CommandContext(ctx, wholeCommand[0], wholeCommand[1:]...)
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "LC_ALL=C")

	release, err := acquireCommandSlot(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := runCommand(ctx, logVerbosity, cmd)
	if err != nil {
		release()
		return nil, err
	}
	return releasingReadCloser{ReadCloser: stream, release: release}, nil
}

// releasingReadCloser frees the command's slot once the command is closed.
type releasingReadCloser struct {
	io.ReadCloser
	release func()
}

func (r releasingReadCloser) Close() error {
	defer r.release()
	return r.ReadCloser.Close()
}

// runCommand runs the command and returns the stdout as a ReadCloser that also Waits for the command to finish.
// After the Close command is called the cmd is closed and the resources are released.
// Not calling close on this method will result in a resource leak.
func runCommand(ctx context.Context, logVerbosity int, cmd *exec.Cmd) (io.ReadCloser, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdout.Close()
		return nil, err
	}

	log.FromContext(ctx).V(logVerbosity).Info("invoking command", "args", cmd.Args)
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, err
	}
	// Return a read closer that will wait for the command to finish when closed to release all resources.
	return commandReadCloser{cmd: cmd, ReadCloser: stdout, stderr: stderr}, nil
}

// commandReadCloser is a ReadCloser that calls the Wait function of the command when Close is called.
// This is used to wait for the command the pipe before waiting for the command to finish.
type commandReadCloser struct {
	cmd *exec.Cmd
	io.ReadCloser
	stderr io.ReadCloser
}

// Close drains any unread stdout, reads stderr, and waits for the command to exit.
func (p commandReadCloser) Close() error {
	// Drain stdout before Wait() to prevent blocking when the caller stopped
	// reading early and the child has more output than the pipe buffer.
	_, _ = io.Copy(io.Discard, p.ReadCloser)

	// Read the stderr output after the read has finished since we are sure by then the command must have run.
	stderr, err := io.ReadAll(p.stderr)
	if err != nil {
		return err
	}

	if err := p.cmd.Wait(); err != nil {
		// wait can result in an exit code error
		return &lvmErr{
			err:    err,
			stderr: stderr,
		}
	}
	return nil
}
