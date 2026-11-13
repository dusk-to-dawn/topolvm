# CI of this fork

This fork runs CI only when someone asks for it: once per candidate commit, the
cheap checks first. Nothing runs on push, pull request or schedule. GitHub
disabled the workflows of this fork once, after a day in which the workflows
inherited from upstream ran on every push to a pull request: 27 runs and about
1,900 job-minutes.

## Branches and workflows

- **`fast-provisioning-v0.42.1` is the default branch.** Its last commits are
  fork-only: they delete every workflow inherited from upstream and add the two
  below. Workflows that can be dispatched must be on the default branch.
- **`chart-v17.3.1-fast`** holds the chart and `fork-chart.yaml`; **`bench`**
  holds `bench.yaml`. Their fork-only commits delete upstream's workflows too.
- **`base-v0.42.1` and `base-chart-v17.3.1`** are copies of upstream's release
  tags and keep upstream's workflows. Nothing makes them run: no pull request is
  open against them, schedules run only on the default branch, and nobody pushes
  to them.
- **There is no `main`.** It was deleted on 2026-11-13, so that "Sync fork" has
  no branch to push upstream's commits to. Do not push a branch named `main` to
  this fork: its tree would carry upstream's push-on-main workflows and run
  them. Rebase onto upstream locally instead.

| workflow | file, branch | runs on | does |
|---|---|---|---|
| Fork CI | `fork-ci.yaml`, default branch | dispatch only, inputs `ref` (full commit SHA) and `scope` (`smoke` or `full`) | stage 1: lint, `check-uncommitted`, unit tests as root with real LVM; stage 2, only after stage 1: e2e |
| Fork image | `fork-image.yaml`, default branch | tags `image-<version>` | builds and pushes `topolvm-with-sidecar:<version>`, once, if a successful `full` Fork CI round exists for the tagged commit |
| Fork chart | `fork-chart.yaml`, `chart-v17.3.1-fast` | tags `topolvm-chart-v<version>` | packages and pushes the chart, if a successful `full` Fork CI round exists for the commit of the image the chart pins (tag `image-<appVersion>`) |
| Bench | `bench.yaml`, `bench` | dispatch only | burst provisioning on kind: the candidate with 8 workers and this fork's controller; with `mixed`, also with upstream's controller |

**Fork CI scopes.**
- `smoke` (default): the newest Kubernetes version of upstream's matrix, with
  the variants this fork changes: the default configuration, eight reconcile
  workers, and lvmd in the cluster as a DaemonSet and embedded.
- `full`: every Kubernetes version and every variant, legacy included. If a
  `smoke` round of the same commit succeeded, its jobs and stage 1 are not run
  again. Upstream's snapshot, restore and clone specs run in every e2e job.

## Cost

Job-minutes, from the runs of 2026-11-12 on GitHub-hosted runners.

| entry point | jobs | job-minutes |
|---|---|---|
| Fork CI `smoke` | plan, stage 1 (~10), e2e image (~2), 2 kind e2e (~20 each), 2 in-cluster e2e (~18 each) | ~90 |
| Fork CI `full` after a `smoke` of the same commit | plan, e2e image, 13 kind e2e, 4 in-cluster e2e | ~330 |
| Fork CI `full` alone | plan, stage 1, e2e image, 15 kind e2e, 6 in-cluster e2e | ~420 |
| Bench | plan, 1 configuration (~10 to 12) | ~12 |
| Bench with `mixed` | plan, 2 configurations | ~24 |
| Fork image | 1 | ~8 |
| Fork chart | 1 | ~1 |

## Rules

1. Before any dispatch, the local checks pass: `go test -race ./...` with
   envtest, and golangci-lint.
2. One `smoke` round per candidate commit.
3. For a release: one `full` round, one bench, one image tag and one chart tag,
   for the same commit.
4. Nothing else. A failed round is fixed locally and the fix becomes a new
   candidate.

**Budget: about 700 job-minutes per release.** A release as above costs about
90 + 330 + 24 + 8 + 1 = 453; two candidates before it add 90 each.

## Releasing

1. Push the candidate. Nothing runs.
2. Dispatch Fork CI with `ref` = the full SHA and `scope` = `smoke`.
3. If it succeeds, dispatch Fork CI with the same `ref` and `scope` = `full`.
4. If that succeeds, dispatch Bench with `build_ref` = the SHA (and `mixed` if
   needed).
5. Push the tag `image-<version>` on the SHA. The image workflow checks the
   `full` round and publishes once.
6. Pin the chart to the new image digest on `chart-v17.3.1-fast`, push it, and
   push the tag `topolvm-chart-v<chart version>`.

## Pending

- **A queued re-run of an old chart run, 43327449947**, was requested while
  workflows were disabled. GitHub would not cancel or delete it then. If it ran,
  it would push chart `17.3.1-fast.3` again with a new digest. Cancel it as soon
  as workflows are enabled again, and only then enable the `Fork chart`
  workflow, which is disabled for this reason.
- **Upstream's workflows** `Main`, `e2e-k8s`, `e2e-k8s-incluster-lvmd` and `pr`
  show as deleted, since the default branch no longer has them, and an old
  `Fork image (dry run)` has no file on any branch. A push of a branch named
  `main` would bring upstream's back and run them.

## Upstream's numbers

The bench of 2026-11-12 measured upstream v0.42.1 with its chart 17.3.1 (one
worker, default arguments) on the same runners as each configuration, medians
of two rounds. They are not measured again for each candidate.

| upstream v0.42.1 | result |
|---|---|
| 1 volume, from claim to ProvisioningSucceeded | 1.1 to 1.2 s with a Pod; 3.2 to 3.9 s for a claim alone, behind the provisioner's client limits |
| 12 claims at once | last provisioned after 4.4 to 7.8 s |
| 40 claims at once | last provisioned after 14.9 to 21.1 s, with 26 to 46 ProvisioningFailed on the way |
| claims 150 ms apart | median 1.9 to 8.8 s from claim to provisioned |
| 40 claims of 3 GiB against a 100 GiB thin pool limit | 33 provisioned (99 GiB), 7 refused |
| `lvm` processes | 8 per creation (9 to 10.5 with Pods), 2 per removal |

## Rebasing onto a newer upstream

Rebase the fork's commits locally onto the upstream release, with the fork-only
commits last. Where upstream changed a workflow file that the fork deletes,
resolve the conflict by deleting the file again. Take the Kubernetes versions
of the e2e matrix from upstream's `e2e-k8s-workflow.yaml` into
`fork-ci.yaml`'s plan step.
