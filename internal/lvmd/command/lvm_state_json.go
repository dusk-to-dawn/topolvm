package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

func parseFullReportResult(data io.ReadCloser) ([]vg, []lv, error) {
	type fullReportResult struct {
		Report []struct {
			VG []vg `json:"vg"`
			LV []lv `json:"lv"`
		} `json:"report"`
	}

	var result fullReportResult
	if err := json.NewDecoder(data).Decode(&result); err != nil {
		return nil, nil, err
	}

	var vgs []vg
	var lvs []lv
	for _, report := range result.Report {
		vgs = append(vgs, report.VG...)
		lvs = append(lvs, report.LV...)
	}
	return vgs, lvs, nil
}

// fullReportArgs are the options of the fullreport command that retrieves
// volume groups and their logical volumes in one call.
var fullReportArgs = []string{
	argReportFormat, formatJSON,
	argUnits, "b", argNoSuffix,
	argConfigReport, "vg", "-o", "vg_name,vg_uuid,vg_size,vg_free",
	argConfigReport, "lv", "-o", "lv_uuid,lv_name,lv_full_name,lv_path,lv_size," +
		"lv_kernel_major,lv_kernel_minor,origin,origin_size,pool_lv,lv_tags," +
		"lv_attr,vg_name,data_percent,metadata_percent,pool_lv",
	// fullreport doesn't have an option to omit an entire section, so we
	// omit all fields instead.
	argConfigReport, "pv", argNoOptions,
	argConfigReport, "pvseg", argNoOptions,
	argConfigReport, "seg", argNoOptions,
}

// getVGState retrieves one volume group and all of its logical volumes with a
// single fullreport command scoped to that volume group. Scoping it to one
// volume group means LVM takes only that volume group's lock.
func getVGState(ctx context.Context, name string) (vg, []lv, error) {
	type fullReportResult struct {
		Report []struct {
			VG []vg `json:"vg"`
			LV []lv `json:"lv"`
		} `json:"report"`
	}
	res := new(fullReportResult)
	args := append([]string{"fullreport", name}, fullReportArgs...)
	err := callLVMInto(ctx, res, verbosityLVMStateNoUpdate, args...)
	if IsLVMNotFound(err) {
		return vg{}, nil, errors.Join(ErrNotFound, err)
	}
	if err != nil {
		return vg{}, nil, err
	}

	for _, report := range res.Report {
		for _, v := range report.VG {
			if v.name == name {
				return v, report.LV, nil
			}
		}
	}
	return vg{}, nil, ErrNotFound
}

// Issue single lvm command that retrieves everything we need in one call and get the output as JSON
func getLVMState(ctx context.Context) ([]vg, []lv, error) {
	streamed, err := callLVMStreamed(ctx, verbosityLVMStateNoUpdate, append([]string{"fullreport"}, fullReportArgs...)...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to execute command: %v", err)
	}
	defer func() {
		// this will wait for the process to be released.
		if err := streamed.Close(); err != nil {
			log.FromContext(ctx).Error(err, "failed to run command")
		}
	}()
	return parseFullReportResult(streamed)
}
