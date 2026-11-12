#!/usr/bin/env python3
"""Summarise one burst round from the logs collected by run.sh."""
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

EVENT = re.compile(r'^(\S+) .*"Event occurred" object="bench/([^"]+)".*reason="([A-Za-z]+)"')


def ts(value):
    """Parse an RFC 3339 timestamp with up to nanoseconds into epoch seconds."""
    value = value.rstrip("Z")
    if "." in value:
        head, frac = value.split(".", 1)
        frac = (frac + "000000")[:6]
        value = f"{head}.{frac}"
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp()


def lvm_subcommand(args):
    for i, a in enumerate(args):
        if a == "lvm" or a.endswith("/lvm"):
            return args[i + 1] if i + 1 < len(args) else "?"
    return args[0] if args else "?"


def lvmd_commands(path, start, end):
    counts = {}
    for line in Path(path).read_text().splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if entry.get("msg") != "invoking command":
            continue
        t = entry.get("ts")
        t = ts(t) if isinstance(t, str) else float(t)
        if not start <= t <= end:
            continue
        sub = lvm_subcommand(entry.get("args", []))
        counts[sub] = counts.get(sub, 0) + 1
    return counts


def lvmd_times(path, message):
    out = []
    for line in Path(path).read_text().splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if entry.get("msg") == message:
            t = entry.get("ts")
            out.append(ts(t) if isinstance(t, str) else float(t))
    return out


def main(rdir):
    rdir = Path(rdir)
    times = json.loads((rdir / "times.json").read_text())
    t0, size, round_name = times["t0"], times["size"], times["round"]
    prefix = round_name + "-"

    starts, succeeded, failed = {}, {}, 0
    for line in (rdir / "provisioner.log").read_text().splitlines():
        m = EVENT.match(line)
        if not m or not m.group(2).startswith(prefix):
            continue
        t, claim, reason = ts(m.group(1)), m.group(2), m.group(3)
        if reason == "Provisioning":
            starts.setdefault(claim, t)
        elif reason == "ProvisioningSucceeded":
            succeeded[claim] = t
        elif reason == "ProvisioningFailed":
            failed += 1

    node_created = []
    for line in (rdir / "node.log").read_text().splitlines():
        if '"created new LV"' in line:
            node_created.append(ts(line.split(" ", 1)[0]))
    node_created = [t for t in node_created if t >= t0]
    lvmd_created = [t for t in lvmd_times(rdir / "lvmd.log", "created a new LV") if t >= t0]

    last_node = max(node_created) if node_created else None
    # Commands are counted up to shortly after lvmd reports the last creation.
    # In "pods" rounds this window also holds the reports that
    # NodePublishVolume and NodeGetVolumeStats run for volumes created earlier
    # in the round; "pvc" rounds have no such calls.
    last_lvmd = max(lvmd_created) if lvmd_created else None
    window_end = (last_lvmd or last_node or times["t_bound"]) + 0.5
    durations = sorted(succeeded[c] - starts[c] for c in succeeded if c in starts)
    create_cmds = lvmd_commands(rdir / "lvmd.log", t0, window_end)
    delete_cmds = lvmd_commands(rdir / "lvmd-delete.log", times["t_del"], times["t_deleted"] + 2.0)
    removed = [t for t in lvmd_times(rdir / "lvmd-delete.log", "removed a LV") if t >= times["t_del"]]

    def per_volume(counts, n, include_full):
        total = sum(v for k, v in counts.items() if include_full or k != "fullreport")
        return round(total / n, 2) if n else None

    def rel(t):
        return round(t - t0, 3) if t is not None else None

    result = {
        "round": round_name,
        "mode": times.get("mode"),
        "volumes": size,
        "succeeded": len(succeeded),
        "provisioning_failed": failed,
        "create_spread_s": round(times["t_created"] - t0, 3),
        "first_provisioning_start_s": rel(min(starts.values())) if starts else None,
        "last_provisioning_start_s": rel(max(starts.values())) if starts else None,
        "last_node_created_s": rel(last_node),
        "last_lvmd_created_s": rel(max(lvmd_created)) if lvmd_created else None,
        "last_succeeded_s": rel(max(succeeded.values())) if succeeded else None,
        "claim_start_to_succeeded_s_min": round(durations[0], 3) if durations else None,
        "claim_start_to_succeeded_s_median": round(durations[len(durations) // 2], 3) if durations else None,
        "claim_start_to_succeeded_s_max": round(durations[-1], 3) if durations else None,
        "all_bound_seen_s": rel(times["t_bound"]),
        "lvm_commands_create_window": create_cmds,
        "lvm_per_volume_excl_fullreport": per_volume(create_cmds, len(lvmd_created), False),
        "fullreports_create_window": create_cmds.get("fullreport", 0),
        "delete_all_lvs_gone_s": round(times["t_deleted"] - times["t_del"], 3),
        "last_lvmd_removed_s": round(max(removed) - times["t_del"], 3) if removed else None,
        "lvm_commands_delete_window": delete_cmds,
        "lvm_per_removal_excl_fullreport": per_volume(delete_cmds, len(removed), False),
    }
    print(json.dumps(result, sort_keys=False))


if __name__ == "__main__":
    main(sys.argv[1])
