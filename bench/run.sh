#!/usr/bin/env bash
# Burst provisioning check for TopoLVM on a kind cluster with lvmd running on
# the runner host (the layout of upstream's e2e suite).
#
# Environment:
#   SRC          checkout of the TopoLVM ref under test (lvmd and e2e tooling)
#   CHART        checkout holding charts/topolvm to install
#   IMAGE_MODE   "pull" (use IMAGE) or "build" (build hypertopolvm from SRC)
#   IMAGE        image reference for IMAGE_MODE=pull, e.g. repo:tag
#   SIDECAR_IMAGE image to copy CSI sidecar binaries from in build mode
#   NODE_ARGS    space-separated extra topolvm-node arguments
#   LVMD_ARGS    space-separated extra lvmd arguments
#   PROVISIONER_ARGS space-separated extra csi-provisioner arguments
#                (needs a chart with controller.csiProvisioner.args)
#   SIZES        volume counts per round, e.g. "1 12 40"
#   MODES        any of "pods", "pvc" and "stagger" (see the round loop)
#   STAGGER_MS   interval between claims in "stagger" rounds and the limit round
#   LIMIT_CLAIMS claims of LIMIT_SIZE in the over-capacity round (0: skip it)
#   LIMIT_SIZE   size of those claims; their sum must exceed the pool's limit
#   REPEATS      rounds per size
#   OUT          output directory
set -euo pipefail

: "${SRC:?}" "${CHART:?}" "${IMAGE_MODE:?}" "${OUT:?}"
SIZES=${SIZES:-"1 12 40"}
MODES=${MODES:-"pods pvc stagger"}
STAGGER_MS=${STAGGER_MS:-150}
LIMIT_CLAIMS=${LIMIT_CLAIMS:-40}
LIMIT_SIZE=${LIMIT_SIZE:-3Gi}
REPEATS=${REPEATS:-2}
NODE_ARGS=${NODE_ARGS:-}
LVMD_ARGS=${LVMD_ARGS:-}
PROVISIONER_ARGS=${PROVISIONER_ARGS:-}
HERE=$(cd "$(dirname "$0")" && pwd)
WORK=/tmp/bench
E2E="$SRC/test/e2e"
KUBERNETES_VERSION=$(sed -n 's/^KUBERNETES_VERSION ?= //p' "$SRC/versions.mk")
CERT_MANAGER_VERSION=$(sed -n 's/^CERT_MANAGER_VERSION := //p' "$SRC/versions.mk")
KUBECTL="$E2E/bin/kubectl-$KUBERNETES_VERSION"
HELM="$SRC/bin/helm"
KIND="$SRC/bin/kind"
NODE=topolvm-e2e-worker
NS=bench
PAUSE_IMAGE=registry.k8s.io/pause:3.10
mkdir -p "$WORK" "$OUT"
T_START=$(date +%s)

log() { printf '%s %s\n' "$(date -u +%H:%M:%S.%3N)" "$*" >&2; }

log "tools"
make -C "$E2E" setup >/dev/null

log "lvmd and volume groups"
(cd "$SRC" && CGO_ENABLED=0 go build -o "$WORK/lvmd" ./cmd/lvmd)
for i in 1 2 3; do
  mkdir -p "/tmp/topolvm/worker$i" "/tmp/topolvm/lvmd$i"
  truncate --size=12G "$WORK/backing$i"
  sudo losetup -f "$WORK/backing$i"
  dev=$(sudo losetup -j "$WORK/backing$i" | cut -d: -f1)
  sudo vgcreate -y "node$i-bench" "$dev" >/dev/null
  sudo lvcreate -y -T -n pool0 -L 10G "node$i-bench" >/dev/null
  cat > "$WORK/lvmd$i.yaml" <<EOF
socket-name: /tmp/topolvm/lvmd$i/lvmd.sock
device-classes:
  - name: "bench-thin"
    volume-group: "node$i-bench"
    default: true
    type: thin
    thin-pool:
      name: "pool0"
      overprovision-ratio: 10.0
EOF
  sudo systemctl reset-failed "lvmd$i.service" 2>/dev/null || true
  # shellcheck disable=SC2086
  sudo systemd-run --unit="lvmd$i.service" "$WORK/lvmd" --config="$WORK/lvmd$i.yaml" \
    --zap-log-level=4 --zap-time-encoding=rfc3339nano $LVMD_ARGS
done
sleep 2
for i in 1 2 3; do systemctl is-active -q "lvmd$i.service"; done

log "image"
if [ "$IMAGE_MODE" = build ]; then
  : "${SIDECAR_IMAGE:?}"
  rm -rf "$E2E/tmpbin" && mkdir -p "$E2E/tmpbin"
  (cd "$SRC" && CGO_ENABLED=0 go build -o "$E2E/tmpbin/hypertopolvm" \
    -ldflags "-X github.com/topolvm/topolvm.Version=bench-$(git -C "$SRC" rev-parse --short HEAD)" ./cmd/hypertopolvm)
  docker pull -q "$SIDECAR_IMAGE" >/dev/null
  cid=$(docker create "$SIDECAR_IMAGE")
  for b in csi-provisioner csi-node-driver-registrar csi-resizer csi-snapshotter livenessprobe; do
    docker cp "$cid:/$b" "$E2E/tmpbin/$b"
  done
  docker rm "$cid" >/dev/null
  docker build -q -f "$E2E/Dockerfile" -t topolvm:bench "$E2E" >/dev/null
  IMAGE=topolvm:bench
  PULL_POLICY=Never
else
  : "${IMAGE:?}"
  docker pull -q "$IMAGE" >/dev/null
  PULL_POLICY=IfNotPresent
fi
docker pull -q "$PAUSE_IMAGE" >/dev/null

log "kind"
make -C "$E2E" launch-kind TEST_SCHEDULER_EXTENDER_TYPE=none KUBERNETES_VERSION="$KUBERNETES_VERSION" >/dev/null
"$KIND" load docker-image --name topolvm-e2e "$IMAGE" "$PAUSE_IMAGE" >/dev/null

log "install chart from $CHART"
repo=${IMAGE%:*}
ref=${IMAGE##*:}
{
  echo "image:"
  echo "  repository: \"$repo\""
  echo "  reference: \"$ref\""
  echo "  pullPolicy: $PULL_POLICY"
  echo "storageClasses:"
  echo "  - name: bench-thin"
  echo "    storageClass:"
  echo "      isDefaultClass: false"
  echo "      volumeBindingMode: WaitForFirstConsumer"
  echo "      additionalParameters:"
  echo "        '{{ include \"topolvm.pluginName\" . }}/device-class': \"bench-thin\""
  echo "node:"
  echo "  args:"
  for a in $NODE_ARGS; do echo "    - \"$a\""; done
  [ -z "$NODE_ARGS" ] && echo "    []"
  echo "controller:"
  echo "  csiProvisioner:"
  echo "    args:"
  for a in $PROVISIONER_ARGS; do echo "      - \"$a\""; done
  [ -z "$PROVISIONER_ARGS" ] && echo "      []"
} > "$WORK/values.yaml"
cp "$WORK/values.yaml" "$OUT/values.yaml"
"$KUBECTL" apply -f "https://github.com/cert-manager/cert-manager/releases/download/$CERT_MANAGER_VERSION/cert-manager.crds.yaml" >/dev/null
"$KUBECTL" create namespace topolvm-system >/dev/null
"$HELM" repo add jetstack https://charts.jetstack.io >/dev/null
"$HELM" repo update >/dev/null
"$HELM" dependency build "$CHART/charts/topolvm/" >/dev/null
"$HELM" install --namespace=topolvm-system topolvm "$CHART/charts/topolvm/" \
  -f "$E2E/manifests/values/base.yaml" -f "$E2E/manifests/values/systemd-service-lvmd.yaml" \
  -f "$WORK/values.yaml" >/dev/null
"$HELM" get manifest -n topolvm-system topolvm > "$OUT/manifest.yaml"
"$KUBECTL" wait --for=condition=available --timeout=180s -n topolvm-system deployments/topolvm-controller >/dev/null
"$KUBECTL" wait --for=condition=ready --timeout=180s -n topolvm-system -l app.kubernetes.io/component=node pod >/dev/null
"$KUBECTL" create namespace "$NS" >/dev/null
NODE_POD=$("$KUBECTL" get pod -n topolvm-system -l app.kubernetes.io/component=node \
  --field-selector "spec.nodeName=$NODE" -o jsonpath='{.items[0].metadata.name}')
# Wait until topolvm-node has published the device class capacity on the node.
for _ in $(seq 60); do
  if "$KUBECTL" get node "$NODE" -o jsonpath='{.metadata.annotations}' | grep -q 'bench-thin'; then break; fi
  sleep 1
done
{
  echo "kernel=$(uname -r)"
  echo "lvm=$(sudo lvm version | head -1 | xargs)"
  echo "nproc=$(nproc)"
  echo "image=$IMAGE"
  echo "node_args=$NODE_ARGS"
  echo "lvmd_args=$LVMD_ARGS"
  echo "provisioner_args=$PROVISIONER_ARGS"
  echo "provisioner_container_args=$("$KUBECTL" get deploy -n topolvm-system topolvm-controller \
    -o jsonpath='{.spec.template.spec.containers[?(@.name=="csi-provisioner")].args}')"
  echo "node_container_args=$("$KUBECTL" get ds -n topolvm-system topolvm-node \
    -o jsonpath='{.spec.template.spec.containers[?(@.name=="topolvm-node")].args}')"
} > "$OUT/env.txt"
cat "$OUT/env.txt" >&2

object_manifest() { # mode round index [size]
  local mode=$1 round=$2 i=$3 size=${4:-1Gi}
  if [ "$mode" != pods ]; then
    cat <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $round-$i
  namespace: $NS
  labels:
    bench: "$round"
  annotations:
    volume.kubernetes.io/selected-node: $NODE
spec:
  accessModes: ["ReadWriteOnce"]
  volumeMode: Block
  storageClassName: bench-thin
  resources:
    requests:
      storage: $size
EOF
  else
    cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $round-$i
  namespace: $NS
  labels:
    bench: "$round"
spec:
  nodeSelector:
    kubernetes.io/hostname: $NODE
  terminationGracePeriodSeconds: 0
  containers:
    - name: pause
      image: $PAUSE_IMAGE
      imagePullPolicy: IfNotPresent
      volumeDevices:
        - name: v
          devicePath: /dev/v
  volumes:
    - name: v
      ephemeral:
        volumeClaimTemplate:
          spec:
            accessModes: ["ReadWriteOnce"]
            volumeMode: Block
            storageClassName: bench-thin
            resources:
              requests:
                storage: 1Gi
EOF
  fi
}

lv_count() { "$KUBECTL" get logicalvolumes.topolvm.io --no-headers 2>/dev/null | wc -l; }

# create_staggered rdir round count size: creates count claims, one every
# STAGGER_MS, each by its own kubectl process.
create_staggered() {
  local rdir=$1 round=$2 count=$3 size=$4 i pids="" pause
  pause=$(awk -v ms="$STAGGER_MS" 'BEGIN { printf "%.3f", ms / 1000 }')
  mkdir -p "$rdir/staggered"
  for i in $(seq -w 1 "$count"); do
    object_manifest stagger "$round" "$i" "$size" > "$rdir/staggered/$i.yaml"
  done
  for i in $(seq -w 1 "$count"); do
    "$KUBECTL" create -f "$rdir/staggered/$i.yaml" >/dev/null &
    pids="$pids $!"
    sleep "$pause"
  done
  # shellcheck disable=SC2086
  wait $pids
}

# thin_sum: bytes of all thin volumes in node 1's pool, as LVM reports them.
thin_sum() {
  sudo lvs --noheadings --units b --nosuffix -o lv_size -S 'pool_lv=pool0' node1-bench 2>/dev/null \
    | awk '{ s += $1 } END { printf "%d\n", s }'
}

# Modes: "pods" creates Pods with a generic ephemeral block volume each, pinned
# to one node (the claims pass through the ephemeral volume controller and the
# scheduler); "pvc" creates claims that already carry the selected-node
# annotation the scheduler would set, so provisioning starts at once and no
# Pod, kubelet or NodePublishVolume call is involved; "stagger" creates the
# same claims one at a time, STAGGER_MS apart, as the claims of a CI burst
# arrive.
for mode in $MODES; do
  for size in $SIZES; do
    for rep in $(seq "$REPEATS"); do
      round="$mode-n$size-r$rep"
      rdir="$OUT/$round"
      mkdir -p "$rdir/objects"
      # Four kubectl processes, each creating a quarter of the objects: more
      # processes than CPUs make the requests arrive later, not sooner.
      n=0
      for i in $(seq -w 1 "$size"); do
        { echo "---"; object_manifest "$mode" "$round" "$i"; } >> "$rdir/objects/chunk$((n % 4)).yaml"
        n=$((n + 1))
      done
      sleep 5
      log "round $round: create $size objects"
      t0=$(date -u +%s.%N)
      pids=""
      if [ "$mode" = stagger ]; then
        create_staggered "$rdir" "$round" "$size" 1Gi
      else
        for f in "$rdir"/objects/chunk*.yaml; do
          "$KUBECTL" create -f "$f" >/dev/null &
          pids="$pids $!"
        done
        # shellcheck disable=SC2086
        wait $pids
      fi
      t_created=$(date -u +%s.%N)
      deadline=$(( $(date +%s) + 300 ))
      while :; do
        bound=$("$KUBECTL" get pvc -n "$NS" -o jsonpath='{range .items[*]}{.metadata.name} {.status.phase}{"\n"}{end}' \
          | awk -v r="$round-" 'index($1, r) == 1 && $2 == "Bound"' | wc -l)
        [ "$bound" -ge "$size" ] && break
        if [ "$(date +%s)" -gt "$deadline" ]; then log "round $round: timeout with $bound bound"; break; fi
        sleep 0.5
      done
      t_bound=$(date -u +%s.%N)
      sleep 3
      since=$(date -u -d "@${t0%.*}" +%Y-%m-%dT%H:%M:%SZ)
      "$KUBECTL" logs -n topolvm-system deploy/topolvm-controller -c csi-provisioner --timestamps --since-time="$since" > "$rdir/provisioner.log"
      "$KUBECTL" logs -n topolvm-system deploy/topolvm-controller -c topolvm-controller --timestamps --since-time="$since" > "$rdir/controller.log"
      "$KUBECTL" logs -n topolvm-system "$NODE_POD" -c topolvm-node --timestamps --since-time="$since" > "$rdir/node.log"
      sudo journalctl -u lvmd1.service -o cat --since "@${t0%.*}" > "$rdir/lvmd.log"
      log "round $round: delete"
      t_del=$(date -u +%s.%N)
      if [ "$mode" != pods ]; then
        "$KUBECTL" delete pvc -n "$NS" -l "bench=$round" --wait=false >/dev/null
      else
        "$KUBECTL" delete pod -n "$NS" -l "bench=$round" --wait=false >/dev/null
      fi
      deadline=$(( $(date +%s) + 300 ))
      while [ "$(lv_count)" -gt 0 ]; do
        if [ "$(date +%s)" -gt "$deadline" ]; then log "round $round: deletion timeout"; break; fi
        sleep 0.5
      done
      t_deleted=$(date -u +%s.%N)
      sleep 3
      sudo journalctl -u lvmd1.service -o cat --since "@${t_del%.*}" > "$rdir/lvmd-delete.log"
      cat > "$rdir/times.json" <<EOF
{"round": "$round", "mode": "$mode", "size": $size, "t0": $t0, "t_created": $t_created, "t_bound": $t_bound, "t_del": $t_del, "t_deleted": $t_deleted}
EOF
      python3 "$HERE/parse.py" "$rdir" | tee "$rdir/result.json" >&2
    done
  done
done

# Over-capacity round: staggered claims that together ask for more than the
# thin pool's limit (pool size times the overprovision ratio of 10). A
# sampler records the largest sum of thin volume sizes while they arrive; it
# must never exceed the limit.
LIMIT_FAILED=0
if [ "$LIMIT_CLAIMS" -gt 0 ]; then
  round="limit-n$LIMIT_CLAIMS"
  rdir="$OUT/$round"
  mkdir -p "$rdir"
  pool_bytes=$(sudo lvs --noheadings --units b --nosuffix -o lv_size node1-bench/pool0 | tr -d ' ')
  limit_bytes=$((pool_bytes * 10))
  log "round $round: $LIMIT_CLAIMS claims of $LIMIT_SIZE against a limit of $limit_bytes bytes"
  ( while :; do thin_sum; sleep 0.2; done ) > "$rdir/samples.txt" &
  sampler=$!
  t0=$(date -u +%s.%N)
  create_staggered "$rdir" "$round" "$LIMIT_CLAIMS" "$LIMIT_SIZE"
  # Wait until no LogicalVolume has been added for 15 s.
  last=-1
  stable_since=$(date +%s)
  deadline=$(( $(date +%s) + 240 ))
  while :; do
    n=$(lv_count)
    if [ "$n" != "$last" ]; then last=$n; stable_since=$(date +%s); fi
    [ $(( $(date +%s) - stable_since )) -ge 15 ] && break
    [ "$(date +%s)" -gt "$deadline" ] && break
    sleep 1
  done
  sleep 3
  kill "$sampler" 2>/dev/null || true
  wait "$sampler" 2>/dev/null || true
  thin_sum >> "$rdir/samples.txt"
  created=$(sudo lvs --noheadings -o lv_name -S 'pool_lv=pool0' node1-bench | wc -l)
  max_bytes=$(sort -n "$rdir/samples.txt" | tail -n 1)
  since=$(date -u -d "@${t0%.*}" +%Y-%m-%dT%H:%M:%SZ)
  exhausted=$("$KUBECTL" logs -n topolvm-system deploy/topolvm-controller -c csi-provisioner --since-time="$since" \
    | grep -c 'ResourceExhausted' || true)
  overcommit=false
  if [ "$max_bytes" -gt "$limit_bytes" ]; then overcommit=true; fi
  printf '{"round": "%s", "claims": %d, "claim_size": "%s", "stagger_ms": %d, "pool_bytes": %d, "limit_bytes": %d, "max_thin_bytes": %d, "samples": %d, "volumes_created": %d, "resource_exhausted_log_lines": %d, "overcommit": %s}\n' \
    "$round" "$LIMIT_CLAIMS" "$LIMIT_SIZE" "$STAGGER_MS" "$pool_bytes" "$limit_bytes" "$max_bytes" \
    "$(wc -l < "$rdir/samples.txt")" "$created" "$exhausted" "$overcommit" > "$rdir/limit.json"
  cat "$rdir/limit.json" >&2
  "$KUBECTL" delete pvc -n "$NS" -l "bench=$round" --wait=false >/dev/null
  deadline=$(( $(date +%s) + 300 ))
  while [ "$(lv_count)" -gt 0 ]; do
    if [ "$(date +%s)" -gt "$deadline" ]; then log "round $round: deletion timeout"; break; fi
    sleep 0.5
  done
  if [ "$overcommit" = true ]; then
    log "round $round: OVERCOMMIT: thin volumes reached $max_bytes bytes, limit $limit_bytes"
    LIMIT_FAILED=1
  fi
fi

sudo journalctl -u lvmd1.service -o cat --since "@${T_START}" > "$OUT/lvmd1-full.log" || true
"$KUBECTL" logs -n topolvm-system "$NODE_POD" -c topolvm-node --timestamps > "$OUT/node-full.log" || true

# Leave the runner as it was found, so that another configuration can run in
# the same job.
log "teardown"
make -C "$E2E" shutdown-kind >/dev/null 2>&1 || true
for i in 1 2 3; do
  sudo systemctl stop "lvmd$i.service" || true
  sudo vgremove -ffy "node$i-bench" >/dev/null
  dev=$(sudo losetup -j "$WORK/backing$i" | cut -d: -f1)
  sudo pvremove -ffy "$dev" >/dev/null
  sudo losetup -d "$dev"
  rm -f "$WORK/backing$i"
done

if [ "$LIMIT_FAILED" = 1 ]; then
  exit 1
fi
