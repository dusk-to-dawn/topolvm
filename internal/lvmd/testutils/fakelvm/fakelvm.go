// Package fakelvm provides a stand-in for the lvm command so that lvmd's
// handling of LVM commands can be tested without root privileges or real
// volume groups.
//
// Start builds the small fakelvm command (once per test binary) and points
// lvmd's command prefix at it. Every invocation is recorded, so tests can
// assert exactly which lvm commands a call ran. State lives in a JSON file
// guarded by a file lock, which reports take shared and modifying commands
// take exclusively, like LVM's per-VG lock.
package fakelvm

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	envState = "TOPOLVM_FAKE_LVM_STATE"
	envLog   = "TOPOLVM_FAKE_LVM_LOG"
	// envDelay is a delay applied to every command before it takes the lock,
	// standing in for process start-up and device scanning.
	envDelay = "TOPOLVM_FAKE_LVM_DELAY"
	// envLockedDelay is a delay applied to commands that modify state while
	// they hold the lock, standing in for metadata writes and activation.
	envLockedDelay = "TOPOLVM_FAKE_LVM_LOCKED_DELAY"

	// keyReport is the top-level key of LVM's JSON reports.
	keyReport = "report"

	exitNotFound = 5
	exitUsage    = 3

	attrThinPool   = "twi-aotz--"
	attrThinVolume = "Vwi-a-tz--"
	attrThick      = "-wi-a-----"
)

// State is the content of the fake LVM.
type State struct {
	VGs       []*VG  `json:"vgs"`
	NextMinor uint32 `json:"nextMinor"`
}

// VG is a fake volume group.
type VG struct {
	Name string `json:"name"`
	Size uint64 `json:"size"`
	Free uint64 `json:"free"`
	LVs  []*LV  `json:"lvs"`
}

// LV is a fake logical volume, thin pool or thin volume.
type LV struct {
	Name   string   `json:"name"`
	Size   uint64   `json:"size"`
	Pool   string   `json:"pool,omitempty"`
	Origin string   `json:"origin,omitempty"`
	IsPool bool     `json:"isPool,omitempty"`
	Tags   []string `json:"tags,omitempty"`
	Minor  uint32   `json:"minor"`
}

var build struct {
	once sync.Once
	dir  string
	bin  string
	err  error
}

// binary builds the fakelvm command once and returns its path.
func binary() (string, error) {
	build.once.Do(func() {
		build.dir, build.err = os.MkdirTemp("", "fakelvm")
		if build.err != nil {
			return
		}
		build.bin = filepath.Join(build.dir, "lvm")
		goCmd, err := exec.LookPath("go")
		if err != nil {
			build.err = err
			return
		}
		out, err := exec.Command(goCmd, "build", "-o", build.bin,
			"github.com/topolvm/topolvm/internal/lvmd/testutils/fakelvm/cmd/fakelvm").CombinedOutput()
		if err != nil {
			build.err = fmt.Errorf("building fakelvm: %w: %s", err, out)
		}
	})
	return build.bin, build.err
}

// Cleanup removes the fakelvm command built by Start. Call it from TestMain
// after the tests have run.
func Cleanup() {
	if build.dir != "" {
		_ = os.RemoveAll(build.dir)
	}
}

// Fake is a handle on a fake LVM started by Start.
type Fake struct {
	statePath string
	logPath   string
}

// Options tune the fake's timing.
type Options struct {
	// Delay is applied to every command outside the lock.
	Delay time.Duration
	// LockedDelay is applied to modifying commands while they hold the lock.
	LockedDelay time.Duration
}

// Start writes the initial state, makes lvm commands started by this test
// process run the fake, and returns a handle to inspect it. setPrefix must be
// the function that sets lvmd's command prefix; the previous prefix is
// restored when the test ends.
func Start(t testing.TB, initial State, opts Options, getPrefix func() []string, setPrefix func([]string)) *Fake {
	t.Helper()
	bin, err := binary()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	f := &Fake{
		statePath: filepath.Join(dir, "state.json"),
		logPath:   filepath.Join(dir, "commands.log"),
	}
	if err := writeState(f.statePath, &initial); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envState, f.statePath)
	t.Setenv(envLog, f.logPath)
	t.Setenv(envDelay, opts.Delay.String())
	t.Setenv(envLockedDelay, opts.LockedDelay.String())

	prev := getPrefix()
	setPrefix([]string{bin})
	t.Cleanup(func() { setPrefix(prev) })
	return f
}

// Invocation is one recorded run of the fake lvm.
type Invocation struct {
	Start, End time.Time
	// Args is the sub-command and its arguments joined by spaces.
	Args string
}

// Invocations returns the lvm commands that have finished so far, in the
// order they finished.
func (f *Fake) Invocations(t testing.TB) []Invocation {
	t.Helper()
	data, err := os.ReadFile(f.logPath)
	if err != nil {
		t.Fatal(err)
	}
	var invs []Invocation
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		fields := strings.SplitN(sc.Text(), " ", 3)
		if len(fields) != 3 {
			continue
		}
		start, err1 := strconv.ParseInt(fields[0], 10, 64)
		end, err2 := strconv.ParseInt(fields[1], 10, 64)
		if err1 != nil || err2 != nil {
			t.Fatalf("bad fake lvm log line %q", sc.Text())
		}
		invs = append(invs, Invocation{Start: time.Unix(0, start), End: time.Unix(0, end), Args: fields[2]})
	}
	return invs
}

// Commands returns the lvm commands run so far, one entry per process,
// each with its arguments joined by spaces.
func (f *Fake) Commands(t testing.TB) []string {
	t.Helper()
	invs := f.Invocations(t)
	cmds := make([]string, 0, len(invs))
	for _, inv := range invs {
		cmds = append(cmds, inv.Args)
	}
	return cmds
}

// MaxConcurrent returns the largest number of recorded commands whose
// sub-command is one of names (all if empty) that were running at once.
func (f *Fake) MaxConcurrent(t testing.TB, names ...string) int {
	t.Helper()
	type edge struct {
		at    time.Time
		delta int
	}
	var edges []edge
	for _, inv := range f.Invocations(t) {
		name := strings.SplitN(inv.Args, " ", 2)[0]
		if len(names) > 0 && !slices.Contains(names, name) {
			continue
		}
		edges = append(edges, edge{inv.Start, 1}, edge{inv.End, -1})
	}
	slices.SortFunc(edges, func(a, b edge) int {
		if c := a.at.Compare(b.at); c != 0 {
			return c
		}
		return a.delta - b.delta // ends before starts at the same instant
	})
	running, peak := 0, 0
	for _, e := range edges {
		running += e.delta
		peak = max(peak, running)
	}
	return peak
}

// SubCommands returns only the sub-command names of Commands.
func (f *Fake) SubCommands(t testing.TB) []string {
	t.Helper()
	cmds := f.Commands(t)
	names := make([]string, 0, len(cmds))
	for _, c := range cmds {
		names = append(names, strings.SplitN(c, " ", 2)[0])
	}
	return names
}

// Reset forgets the commands recorded so far.
func (f *Fake) Reset(t testing.TB) {
	t.Helper()
	if err := os.WriteFile(f.logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

// State returns the current state of the fake.
func (f *Fake) State(t testing.TB) *State {
	t.Helper()
	s, err := readState(f.statePath)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// Main runs one fake lvm command with args (without the program name) and
// returns its exit code.
func Main(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "no command given")
		return exitUsage
	}
	start := time.Now()
	defer func() {
		// Recorded at exit, with the start and end times, so tests can see overlap.
		line := fmt.Sprintf("%d %d %s", start.UnixNano(), time.Now().UnixNano(), strings.Join(args, " "))
		if err := appendLog(os.Getenv(envLog), line); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}()
	sleepEnv(envDelay)

	cmd, opts, pos := parse(args)

	// Like LVM's per-VG lock: reports share it, modifying commands hold it exclusively.
	how := syscall.LOCK_SH
	switch cmd {
	case "lvcreate", "lvremove", "lvresize", "lvchange":
		how = syscall.LOCK_EX
	}
	statePath := os.Getenv(envState)
	unlock, err := lock(statePath+".lock", how)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitUsage
	}
	defer unlock()

	state, err := readState(statePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitUsage
	}

	var code int
	modified := false
	switch cmd {
	case "vgs":
		code = vgs(os.Stdout, state, pos)
	case "lvs":
		code = lvs(os.Stdout, state, pos)
	case "fullreport":
		code = fullreport(os.Stdout, state, pos)
	case "lvcreate":
		code = lvcreate(state, opts, pos)
		modified = code == 0
	case "lvremove":
		code = lvremove(state, pos)
		modified = code == 0
	case "lvresize":
		code = lvresize(state, opts, pos)
		modified = code == 0
	case "lvchange":
		code = 0
	default:
		fmt.Fprintf(os.Stderr, "fake lvm does not implement %q\n", cmd)
		return exitUsage
	}
	if modified {
		sleepEnv(envLockedDelay)
		if err := writeState(statePath, state); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return exitUsage
		}
	}
	return code
}

// options that take a value in the commands lvmd runs.
var valueOptions = map[string]bool{
	"-o": true, "--units": true, "--reportformat": true, "--configreport": true,
	"-n": true, "-V": true, "-L": true, "-T": true, "-W": true, "-k": true,
	"-i": true, "-I": true, "--addtag": true, "-p": true, "-a": true, "--size": true,
}

func parse(args []string) (string, map[string][]string, []string) {
	cmd := args[0]
	opts := map[string][]string{}
	var pos []string
	for i := 1; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "-") {
			if valueOptions[a] && i+1 < len(args) {
				opts[a] = append(opts[a], args[i+1])
				i++
				continue
			}
			opts[a] = append(opts[a], "")
			continue
		}
		pos = append(pos, a)
	}
	return cmd, opts, pos
}

func first(opts map[string][]string, key string) string {
	if v := opts[key]; len(v) > 0 {
		return v[0]
	}
	return ""
}

func parseSize(s string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSuffix(strings.TrimSuffix(s, "b"), "B"), 10, 64)
}

func (s *State) vg(name string) *VG {
	for _, vg := range s.VGs {
		if vg.Name == name {
			return vg
		}
	}
	return nil
}

func (vg *VG) lv(name string) *LV {
	for _, lv := range vg.LVs {
		if lv.Name == name {
			return lv
		}
	}
	return nil
}

func vgNotFound(name string) int {
	fmt.Fprintf(os.Stderr, "  Volume group \"%s\" not found\n  Cannot process volume group %s\n", name, name)
	return exitNotFound
}

func lvNotFound(full string) int {
	fmt.Fprintf(os.Stderr, "  Failed to find logical volume \"%s\"\n", full)
	return exitNotFound
}

// splitTarget splits "vg" or "vg/lv".
func splitTarget(t string) (string, string) {
	vg, lv, _ := strings.Cut(t, "/")
	return vg, lv
}

func vgRow(vg *VG) map[string]string {
	return map[string]string{
		"vg_uuid": "uuid-" + vg.Name,
		"vg_name": vg.Name,
		"vg_size": strconv.FormatUint(vg.Size, 10),
		"vg_free": strconv.FormatUint(vg.Free, 10),
	}
}

func lvRow(vg *VG, lv *LV) map[string]string {
	attr := attrThick
	switch {
	case lv.IsPool:
		attr = attrThinPool
	case lv.Pool != "":
		attr = attrThinVolume
	}
	originSize := ""
	if lv.Origin != "" {
		if o := vg.lv(lv.Origin); o != nil {
			originSize = strconv.FormatUint(o.Size, 10)
		}
	}
	dataPercent, metaPercent := "", ""
	if lv.IsPool {
		dataPercent, metaPercent = "1.00", "2.00"
	}
	return map[string]string{
		"lv_uuid":          "uuid-" + vg.Name + "-" + lv.Name,
		"lv_name":          lv.Name,
		"lv_full_name":     vg.Name + "/" + lv.Name,
		"lv_path":          "/dev/" + vg.Name + "/" + lv.Name,
		"lv_size":          strconv.FormatUint(lv.Size, 10),
		"lv_kernel_major":  "253",
		"lv_kernel_minor":  strconv.FormatUint(uint64(lv.Minor), 10),
		"origin":           lv.Origin,
		"origin_size":      originSize,
		"pool_lv":          lv.Pool,
		"lv_tags":          strings.Join(lv.Tags, ","),
		"lv_attr":          attr,
		"vg_name":          vg.Name,
		"data_percent":     dataPercent,
		"metadata_percent": metaPercent,
	}
}

func writeJSON(w io.Writer, v any) {
	_ = json.NewEncoder(w).Encode(v)
}

func vgs(w io.Writer, s *State, pos []string) int {
	rows := []map[string]string{}
	if len(pos) == 0 {
		for _, vg := range s.VGs {
			rows = append(rows, vgRow(vg))
		}
	}
	for _, name := range pos {
		vg := s.vg(name)
		if vg == nil {
			writeJSON(w, map[string]any{keyReport: []any{map[string]any{"vg": rows}}})
			return vgNotFound(name)
		}
		rows = append(rows, vgRow(vg))
	}
	writeJSON(w, map[string]any{keyReport: []any{map[string]any{"vg": rows}}})
	return 0
}

func lvs(w io.Writer, s *State, pos []string) int {
	rows := []map[string]string{}
	for _, target := range pos {
		vgName, lvName := splitTarget(target)
		vg := s.vg(vgName)
		if vg == nil {
			writeJSON(w, map[string]any{keyReport: []any{map[string]any{"lv": rows}}})
			return vgNotFound(vgName)
		}
		if lvName == "" {
			for _, lv := range vg.LVs {
				rows = append(rows, lvRow(vg, lv))
			}
			continue
		}
		lv := vg.lv(lvName)
		if lv == nil {
			writeJSON(w, map[string]any{keyReport: []any{map[string]any{"lv": rows}}})
			return lvNotFound(target)
		}
		rows = append(rows, lvRow(vg, lv))
	}
	writeJSON(w, map[string]any{keyReport: []any{map[string]any{"lv": rows}}})
	return 0
}

func fullreport(w io.Writer, s *State, pos []string) int {
	selected := s.VGs
	if len(pos) > 0 {
		selected = nil
		for _, name := range pos {
			vg := s.vg(name)
			if vg == nil {
				writeJSON(w, map[string]any{keyReport: []any{}})
				return vgNotFound(name)
			}
			selected = append(selected, vg)
		}
	}
	reports := []any{}
	for _, vg := range selected {
		lvRows := []map[string]string{}
		for _, lv := range vg.LVs {
			lvRows = append(lvRows, lvRow(vg, lv))
		}
		reports = append(reports, map[string]any{
			"vg":    []map[string]string{vgRow(vg)},
			"pv":    []any{},
			"lv":    lvRows,
			"pvseg": []any{},
			"seg":   []any{},
		})
	}
	writeJSON(w, map[string]any{keyReport: reports})
	return 0
}

func lvcreate(s *State, opts map[string][]string, pos []string) int {
	name := first(opts, "-n")
	var vgName, pool, origin string
	var size uint64
	var err error
	switch {
	case first(opts, "-T") != "":
		vgName, pool = splitTarget(first(opts, "-T"))
		if first(opts, "-V") == "" {
			fmt.Fprintln(os.Stderr, "fake lvm only creates thin volumes with -T")
			return exitUsage
		}
		size, err = parseSize(first(opts, "-V"))
	case len(opts["-s"]) > 0:
		if len(pos) != 1 {
			return exitUsage
		}
		vgName, origin = splitTarget(pos[0])
	default:
		if len(pos) != 1 {
			return exitUsage
		}
		vgName = pos[0]
		size, err = parseSize(first(opts, "-L"))
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitUsage
	}
	vg := s.vg(vgName)
	if vg == nil {
		return vgNotFound(vgName)
	}
	if vg.lv(name) != nil {
		fmt.Fprintf(os.Stderr, "  Logical Volume \"%s\" already exists in volume group \"%s\"\n", name, vgName)
		return exitNotFound
	}
	lv := &LV{Name: name, Tags: opts["--addtag"]}
	switch {
	case pool != "":
		p := vg.lv(pool)
		if p == nil || !p.IsPool {
			return lvNotFound(vgName + "/" + pool)
		}
		lv.Pool, lv.Size = pool, size
	case origin != "":
		o := vg.lv(origin)
		if o == nil {
			return lvNotFound(vgName + "/" + origin)
		}
		lv.Pool, lv.Origin, lv.Size = o.Pool, origin, o.Size
	default:
		if size > vg.Free {
			fmt.Fprintf(os.Stderr, "  Volume group \"%s\" has insufficient free space\n", vgName)
			return exitNotFound
		}
		vg.Free -= size
		lv.Size = size
	}
	s.NextMinor++
	lv.Minor = s.NextMinor
	vg.LVs = append(vg.LVs, lv)
	fmt.Printf("  Logical volume \"%s\" created.\n", name)
	return 0
}

func lvremove(s *State, pos []string) int {
	if len(pos) != 1 {
		return exitUsage
	}
	vgName, lvName := splitTarget(pos[0])
	vg := s.vg(vgName)
	if vg == nil {
		return vgNotFound(vgName)
	}
	for i, lv := range vg.LVs {
		if lv.Name != lvName {
			continue
		}
		if lv.Pool == "" && !lv.IsPool {
			vg.Free += lv.Size
		}
		vg.LVs = append(vg.LVs[:i], vg.LVs[i+1:]...)
		fmt.Printf("  Logical volume \"%s\" successfully removed.\n", lvName)
		return 0
	}
	return lvNotFound(pos[0])
}

func lvresize(s *State, opts map[string][]string, pos []string) int {
	if len(pos) != 1 {
		return exitUsage
	}
	size, err := parseSize(first(opts, "-L"))
	if err != nil {
		return exitUsage
	}
	vgName, lvName := splitTarget(pos[0])
	vg := s.vg(vgName)
	if vg == nil {
		return vgNotFound(vgName)
	}
	lv := vg.lv(lvName)
	if lv == nil {
		return lvNotFound(pos[0])
	}
	if lv.Pool == "" && size > lv.Size {
		if size-lv.Size > vg.Free {
			fmt.Fprintf(os.Stderr, "  Insufficient free space\n")
			return exitNotFound
		}
		vg.Free -= size - lv.Size
	}
	lv.Size = size
	return 0
}

func sleepEnv(key string) {
	d, err := time.ParseDuration(os.Getenv(key))
	if err == nil && d > 0 {
		time.Sleep(d)
	}
}

func appendLog(path, line string) error {
	if path == "" {
		return errors.New("fake lvm log path is not set")
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = f.WriteString(line + "\n")
	return errors.Join(err, f.Close())
}

func lock(path string, how int) (func(), error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), how); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}

func readState(path string) (*State, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func writeState(path string, s *State) error {
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
