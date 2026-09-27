#!/usr/bin/env bash
# Phase 0 exit test on the cluster, with fault injection.
#   RUN=<unique> EVENTS=1000 KILL=resonate|bridge|worker|none ./exactly-once.sh
# Publishes EVENTS CloudEvents x3 (deliberate duplicates) plus 20 invocations
# x2 through Dapr, optionally force-kills one pod of KILL a few seconds in, and
# checks that every trigger ran exactly once (waiting past a 60 s task lease).
set -euo pipefail
RUN="${RUN:?}" EVENTS="${EVENTS:-200}" KILL="${KILL:-none}"
job="check-$RUN"
kubectl -n loam apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata: { name: $job, namespace: loam }
spec:
  backoffLimit: 0
  template:
    metadata:
      annotations:
        dapr.io/enabled: "true"
        dapr.io/app-id: trigger-publisher
        dapr.io/config: loam
        dapr.io/sidecar-memory-limit: 256Mi
    spec:
      restartPolicy: Never
      containers:
        - name: check
          image: localhost/loam/dapr-bridge:tikv-dapr
          command: ["/usr/local/bin/trigger-check"]
          env:
            - { name: CHECK_RUN, value: "$RUN" }
            - { name: CHECK_EVENTS, value: "$EVENTS" }
            - { name: CHECK_DUPLICATES, value: "3" }
            - { name: CHECK_INVOKES, value: "20" }
            - { name: CHECK_SETTLE_SECS, value: "90" }
            - { name: CHECK_TIMEOUT_SECS, value: "400" }
            - { name: CHECK_STRICT, value: "${STRICT:-1}" }
EOF
if [ "$KILL" != none ]; then
  until kubectl -n loam get pod -l job-name="$job" -o name 2>/dev/null | grep -q pod; do sleep 1; done
  kubectl -n loam wait --for=condition=Ready pod -l job-name="$job" --timeout=120s >/dev/null || true
  sleep 1
  victim=$(kubectl -n loam get pod -l app="$KILL" -o name | head -1)
  echo "killing $victim"
  kubectl -n loam delete "$victim" --grace-period=0 --force >/dev/null 2>&1
fi
# Poll: `kubectl wait` with two --for conditions waits for both.
for _ in $(seq 1 700); do
  st=$(kubectl -n loam get job "$job" -o jsonpath='{.status.succeeded}{.status.failed}')
  [ -n "$st" ] && break
  sleep 1
done
kubectl -n loam logs "job/$job" -c check | tail -1
