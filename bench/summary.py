#!/usr/bin/env python3
"""Print a markdown table of all rounds of one configuration, and a TSV file."""
import json
import sys
from pathlib import Path

COLUMNS = [
    ("round", "round"),
    ("volumes", "volumes"),
    ("succeeded", "ok"),
    ("provisioning_failed", "failed"),
    ("last_provisioning_start_s", "last start s"),
    ("last_node_created_s", "last node-side create s"),
    ("last_succeeded_s", "last ProvisioningSucceeded s"),
    ("claim_start_to_succeeded_s_median", "claim median s"),
    ("claim_start_to_succeeded_s_max", "claim max s"),
    ("lvm_per_volume_excl_fullreport", "lvm per create"),
    ("fullreports_create_window", "fullreports"),
    ("delete_all_lvs_gone_s", "all LVs gone s"),
    ("lvm_per_removal_excl_fullreport", "lvm per removal"),
]


def main(name, out):
    rows = []
    for result in sorted(Path(out).glob("*/result.json")):
        try:
            rows.append(json.loads(result.read_text()))
        except ValueError:
            continue
    print(f"#### {name}\n")
    print("| " + " | ".join(h for _, h in COLUMNS) + " | create commands |")
    print("|" + "---|" * (len(COLUMNS) + 1))
    tsv = ["\t".join(["config"] + [k for k, _ in COLUMNS] + ["lvm_commands_create_window"])]
    for r in rows:
        cmds = json.dumps(r.get("lvm_commands_create_window", {}), sort_keys=True)
        print("| " + " | ".join(str(r.get(k)) for k, _ in COLUMNS) + f" | `{cmds}` |")
        tsv.append("\t".join([name] + [str(r.get(k)) for k, _ in COLUMNS] + [cmds]))
    Path(out, "summary.tsv").write_text("\n".join(tsv) + "\n")
    env = Path(out, "env.txt")
    if env.exists():
        print("\n```\n" + env.read_text() + "```\n")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
