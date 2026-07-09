# Multi-tenant quota, priority & saturation

A runnable guide for the [Async Processor](../../../charts/async-processor) that gives three teams
their own **per-team quota**, **priority tier**, and **saturation-aware back-off**, observed through
either **self-hosted Prometheus + Grafana** or **GCP Cloud Monitoring**.

The scenario is the point; the **message queue is a pluggable backend**. It runs unchanged on:

- **Redis SortedSet** (`redis-sortedset`) — portable, no cloud dependency; queues are scored by
  request `deadline`, so within a team the processor dispatches **earliest-deadline-first**.
- **GCP Pub/Sub** (`gcp-pubsub-gated`) — managed queue on GCP.

The gate configuration, worker pools, observability, and the scenario walkthroughs below are
identical across both; only the queue wiring and how you publish differ.

![Animated architecture: per-team messages flow through quota + saturation gates; under load the batch gate closes and its backlog grows while premium keeps flowing](diagram/architecture.gif)

> The loop illustrates the priority-under-saturation story (drawn with the Pub/Sub backend and the
> earlier per-pool model; the current guide expresses priority with the tier-priority merge policy —
> reserved/overflow × tier lanes — over one shared pool, and Redis SortedSet behaves identically).
> Source + regeneration: [`diagram/`](diagram/) (`architecture.html` is the editable animated SVG).

## What it shows

Each team has a **tier** and a **reserved quota** (its org-guaranteed capacity). All three teams
feed **one shared worker pool**, and the [**tier-priority merge policy**](https://github.com/llm-d-incubation/llm-d-async/pull/294)
orders that pool's requests by two per-request dimensions:

| Team | Queue | Tier | Reserved quota | `inference_objective` |
| :-- | :-- | :-- | :-- | :-- |
| **premium** | `team-premium-requests` | `interactive` (highest) | concurrency **2** | `latency` |
| **standard** | `team-standard-requests` | `async` | concurrency **2** | (default) |
| **batch** | `team-batch-requests` | `batch` (lowest) | concurrency **1** | `throughput` |

- **Reservation classification** — set by the per-team **`redis-quota`** gate in **`classifying`**
  mode (keyed on the `team` key in the request **`metadata`**, queue-agnostic): requests within a
  team's quota are `reserved` (its guaranteed lane); requests beyond it are `overflow`. Unlike the
  default *blocking* mode, overflow is **not** nacked — it is admitted and simply deprioritized.
- **Tier** — a per-queue `tier` label: `interactive` (premium) > `async` (standard) > `batch`.

The merge policy buckets every request into one of **6 strict lanes** by `(classification, tier)`,
dispatches them in that order, and stamps **`x-gateway-priority`** (0 = highest … 5 = lowest) for the
inference gateway:

| Lane | `x-gateway-priority` | Who |
| :-- | :-- | :-- |
| reserved + interactive | 0 | premium within quota |
| reserved + async | 1 | standard within quota |
| reserved + batch | 2 | batch within quota |
| overflow + interactive | 3 | premium over quota |
| overflow + async | 4 | standard over quota |
| overflow + batch | 5 | batch over quota |

So **all reserved traffic drains before any overflow** (org priority), and within each class the tier
order still holds — overflow is prioritized among itself (interactive over batch). Within a lane the
policy round-robins across teams and, on Redis SortedSet, dispatches earliest-deadline-first.

**Saturation (Scenario C)** adds a pool-level `wait-on-refuse(prometheus-query)` gate on the shared
pool: `D = clamp(1 - sum(vllm:num_requests_running)/N, 0, 1)`. On saturation the pool **parks**
(`ActionWait`, in-memory — no broker churn); as capacity frees, the merge policy drains the highest
lanes first. (For a *tier-aware* saturation gate that treats lanes differently — wait `reserved`,
429 `interactive`+`overflow`, refuse `async`/`batch`+`overflow` — see the `tier-priority-admission`
gate from [#294]; these overlays use the simpler `wait-on-refuse` guard.)

> `x-gateway-priority` carries the **lane index (lower = higher priority)**. Confirm your inference
> gateway / EPP interprets it that way — the header is how per-team priority reaches the scheduler.

### Two things to know up front

1. **Over-quota is deprioritized, not dropped.** In `classifying` mode, requests beyond a team's
   reserved quota become `overflow` and are dispatched after all `reserved` traffic (and behind
   higher tiers), rather than nacked/redelivered. To hard-throttle instead, use the gate's default
   `blocking` mode (over-quota returns to the queue; backlog grows).
2. **Two observability options** (pick one):
   - **B — self-hosted Prometheus + Grafana** (any cluster, either queue): uses the chart's bundled
     `PodMonitor` / `PrometheusRule` / Grafana dashboard; real-time. **The path for Redis SortedSet.**
   - **A — GCP Cloud Monitoring** (GKE-native, GMP): for the Pub/Sub backend on GKE. Gates query the
     GMP query frontend; adds a native Pub/Sub-backlog panel.

## Layout

```
values/
  redis/
    quota-only.yaml            # Scenarios A + B on Redis SortedSet
    saturation-prometheus.yaml # Scenario C on Redis SortedSet (self-hosted Prometheus)
  pubsub/
    quota-only.yaml            # Scenarios A + B on GCP Pub/Sub
    saturation-gmp.yaml        # Scenario C via GMP query frontend (option A)
    saturation-prometheus.yaml # Scenario C on Pub/Sub with self-hosted Prometheus (option B)
  kube-prometheus-stack.yaml   # lean Prometheus + Grafana stack (option B)
manifests/
  redis.yaml                   # ephemeral Redis: request queues (SortedSet) + quota counters
  prometheus-vllm-podmonitor.yaml  # Prometheus Operator scrape of vLLM (option B)
  gmp-podmonitoring.yaml       # GMP scrape of AP metrics -> Cloud Monitoring (option A)
  gmp-frontend.yaml            # GMP query frontend / PromQL endpoint (option A)
dashboards/
  cloud-monitoring.json        # Cloud Monitoring dashboard (option A)
scripts/
  gcp-setup.sh / gcp-teardown.sh   # Pub/Sub option only: topics, subscriptions, SA + IAM
```

## Prerequisites

- A Kubernetes cluster running an **llm-d stack**: inference gateway + EPP + an `InferencePool` +
  vLLM model server. See the [e2e-deploy guide](../e2e-deploy.md) to bring one up.
- `kubectl` and `helm` (v3). **For the Pub/Sub backend only:** a GCP project with the Pub/Sub (+
  Monitoring, for option A) APIs enabled and `gcloud` authenticated.
- The async-processor chart **v0.7.2+**. This guide installs the in-repo chart at
  `../../../charts/async-processor`.

Replace these placeholders throughout `values/` and the commands below:

| Placeholder | Meaning |
| :-- | :-- |
| `NAMESPACE` | namespace for the demo (e.g. `async-demo`) |
| `IGW_HOST` | inference gateway host/IP (used as `http://IGW_HOST:80`) |
| `PROJECT_ID` | your GCP project id (**Pub/Sub backend only**) |
| `INFERENCE_POOL` / model | your `InferencePool` name and served model |

## Choose your queue backend

Do the shared setup, then follow **one** of the two backends. The rest of the guide (publishing,
Scenarios A/B/C, observability) is common; per-backend commands are called out where they differ.

### Option 1 — Redis SortedSet (portable, no cloud dependency)

The bundled Redis backs both the per-team request queues and the quota counters.

```bash
kubectl create namespace NAMESPACE
kubectl apply -n NAMESPACE -f manifests/redis.yaml
```

Edit `values/redis/quota-only.yaml` for your environment (`NAMESPACE`, `IGW_HOST`, model), then:

```bash
helm install async-processor ../../../charts/async-processor \
  -f values/redis/quota-only.yaml -n NAMESPACE

kubectl -n NAMESPACE get deploy async-processor -o yaml | grep message-queue-impl
# -> --message-queue-impl=redis-sortedset
```

Queues are just sorted-set keys — no per-team resource creation needed; they appear on first publish.

### Option 2 — GCP Pub/Sub

```bash
export PROJECT_ID=your-project
./scripts/gcp-setup.sh          # topics, -sub subscriptions, results topic, SA + IAM
kubectl create namespace NAMESPACE
kubectl apply -n NAMESPACE -f manifests/redis.yaml   # still needed for the quota gate
```

`gcp-setup.sh` binds the `async-processor` SA to `pubsub.subscriber` + `pubsub.publisher` +
`pubsub.viewer` (the readiness probe's `GetSubscription`) + `monitoring.viewer` (broker backlog).
**Pod identity:** with Workload Identity, follow the printed binding to map the GSA onto the chart's
`async-processor` KSA; without WI the pod runs as the node service account — ensure it has those
roles (plus `monitoring.metricWriter` for option A dashboards).

Edit `values/pubsub/quota-only.yaml` (`PROJECT_ID`, `NAMESPACE`, `IGW_HOST`, model), then:

```bash
helm install async-processor ../../../charts/async-processor \
  -f values/pubsub/quota-only.yaml -n NAMESPACE

kubectl -n NAMESPACE get deploy async-processor -o yaml | grep message-queue-impl
# -> --message-queue-impl=gcp-pubsub-gated
```

Confirm the pod is **Ready** (this also exercises the Pub/Sub readiness probe).

## Publishing requests

A request is a JSON body — `id`, `created`, `deadline`, a `payload` (a valid completions body the
processor forwards to the gateway), and `metadata.team` (the key the quota gate reads). Use the
helper for your backend.

**Redis SortedSet** — `ZADD` the request onto the team's queue with the **`deadline` as the score**.
The member is the processor's envelope (`request_kind: "plain"` wraps the request body):

```bash
export MODEL=<your-model>
publish() {                                   # publish <team> [count]
  local team=$1 n=${2:-1} now dl member
  for i in $(seq 1 "$n"); do
    now=$(date +%s); dl=$((now+300))
    member=$(printf '{"internal":{},"request_kind":"plain","data":{"id":"%s-%s-%s","created":%s,"deadline":%s,"payload":{"model":"%s","prompt":"summarize this","max_tokens":64},"metadata":{"team":"%s"}}}' \
      "$team" "$now" "$i" "$now" "$dl" "$MODEL" "$team")
    kubectl -n NAMESPACE exec -i deploy/redis -- redis-cli ZADD "team-${team}-requests" "$dl" "$member" >/dev/null
  done
}
```

**GCP Pub/Sub** — publish to the team's topic with a `team` attribute (also mirrored into `metadata`):

```bash
export PROJECT_ID=your-project MODEL=<your-model>
publish() {                                   # publish <team> [count]
  local team=$1 n=${2:-1} now
  for i in $(seq 1 "$n"); do
    now=$(date +%s)
    gcloud pubsub topics publish "team-${team}-requests" --project "$PROJECT_ID" \
      --attribute "team=${team}" \
      --message "$(printf '{"id":"%s-%s-%s","created":%s,"deadline":%s,"payload":{"model":"%s","prompt":"summarize this","max_tokens":64},"metadata":{"team":"%s"}}' \
        "$team" "$now" "$i" "$now" "$((now+300))" "$MODEL" "$team")"
  done
}
```

> Both helpers publish serially — fine for the functional checks below. For sustained
> **rate/concurrency** load (Scenarios B and C), wrap `publish` in background loops or use your own
> publisher that sets the same `team` key and body.

## Scenarios A & B — reserved vs. overflow

**A. Steady state** — each team within its reserved quota:

```bash
for t in premium standard batch; do publish "$t" 1 & done; wait
```

Every request is within its team's quota, so all are classified `reserved` and dispatched in tier
order (premium→standard→batch), stamped `x-gateway-priority` 0/1/2. Results are delivered per backend:

- **Redis:** an `LPUSH` onto the `results-list` list — `kubectl -n NAMESPACE exec deploy/redis -- redis-cli LRANGE results-list 0 -1` (or `LPOP`).
- **Pub/Sub:** published to the `results` topic — pull from the `results-sub` subscription.

> Each result is a JSON object with `id`, `payload` (the upstream response body), and `status_code`
> (the upstream HTTP status). Non-HTTP failures carry `status_code: 0` plus `error_code`/`error_message`
> (e.g. `GATE_DROPPED`, `DEADLINE_EXCEEDED`). Consumers branch on `status_code > 0` vs. `error_code`.

**B. Overflow deprioritization** — push `batch` well past its reserved quota (1) while `premium`
stays within its own:

```bash
publish batch 100 &      # far exceeds batch's reserved 1 -> the excess becomes overflow (lane 5)
publish premium 20 &     # premium within/near its reserved 2 -> reserved (lane 0)
wait
```

batch's first concurrent request stays `reserved` (lane 2); the rest are `overflow` (lane 5) and are
dispatched only **after** all reserved traffic and all higher-tier overflow. premium's `reserved`
requests (lane 0) always jump ahead — so premium drains promptly while batch's overflow trails. The
quota counter caps at the reserved limit; the excess flows as overflow (not nacked):

```bash
kubectl -n NAMESPACE exec deploy/redis -- redis-cli GET quota:team:batch   # batch's live reserved count (<= 1)
```

Because this is `classifying` mode, the backlog does **not** grow the way *blocking* mode's does —
overflow is admitted and simply ordered last, and the stamped `x-gateway-priority` conveys each lane
to the gateway. (Set `gate_params.gating_mode: blocking` to hard-throttle and grow a backlog instead.)

## Scenario C — priority under saturation

Switch to the saturation overlay for your backend (adds the pool-level `wait-on-refuse` gates), then
drive sustained, long-running load on all teams.

- **Redis (self-hosted Prometheus):** first bring up observability option B below, then
  `helm upgrade async-processor ../../../charts/async-processor -f values/redis/saturation-prometheus.yaml -n NAMESPACE`.
- **Pub/Sub, option A (GMP):** deploy the query frontend, then upgrade to `values/pubsub/saturation-gmp.yaml`:

  ```bash
  kubectl apply -n NAMESPACE -f manifests/gmp-frontend.yaml
  helm upgrade async-processor ../../../charts/async-processor \
    -f values/pubsub/saturation-gmp.yaml -n NAMESPACE
  ```
- **Pub/Sub, option B (self-hosted):** use `values/pubsub/saturation-prometheus.yaml` after option B setup.

As saturation rises, the shared pool's budget → 0 and its workers **park in-memory (`ActionWait`)**
rather than returning messages — so the pool stops pulling new work without churning the backlog. As
capacity frees, the tier-priority merge policy drains the highest lanes first: `reserved`+`interactive`
(premium) ahead of everything, `overflow`+`batch` last. Query the pool budget:

```bash
# self-hosted Prometheus (real-time):
kubectl port-forward -n monitoring svc/kps-kube-prometheus-stack-prometheus 9090:9090 &
# GMP frontend (Pub/Sub option A):
# kubectl port-forward -n NAMESPACE deploy/gmp-frontend 9090:9090 &
curl -s localhost:9090/api/v1/query --data-urlencode \
  'query=clamp(1 - sum(vllm:num_requests_running)/20, 0, 1)'   # shared-pool budget
```

(With GMP, Monarch lags real time ~1–2 min, so control is bang-bang on that timescale; the
self-hosted Prometheus path reacts within one scrape.)

## Observability

### Option B — self-hosted Prometheus + Grafana (any cluster; required for Redis)

Works on any cluster, and the gates query Prometheus in **real time**. The chart already ships a
`PodMonitor`, a `PrometheusRule`, and a Grafana dashboard; the saturation overlays turn them on.

```bash
# 1. Prometheus Operator + Prometheus + Grafana
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install kps prometheus-community/kube-prometheus-stack \
  -n monitoring --create-namespace -f values/kube-prometheus-stack.yaml

# 2. Scrape the vLLM model server (adjust selector/namespace/port in the manifest)
kubectl apply -f manifests/prometheus-vllm-podmonitor.yaml

# 3. Deploy wired to the in-cluster Prometheus (Scenario C overlay for your backend)
helm upgrade async-processor ../../../charts/async-processor \
  -f values/redis/saturation-prometheus.yaml -n NAMESPACE      # or values/pubsub/saturation-prometheus.yaml
```

Open Grafana (`admin`/`admin` in the demo values) and run the Scenario-C load; the **Async Processor**
dashboard shows `async_dispatch_budget`, `async_inflight_requests`, `async_gate_decisions_total`, and
`async_broker_backlog{queue_name,pool_name}` (the portable backlog gauge, from `ZCARD` on Redis and
Cloud Monitoring on Pub/Sub). Because all teams now share one pool, break the per-team panels down by
**`queue_name`** (`team-*-requests`) rather than `pool_name` (a single `default` pool). Verify
scraping / budgets:

```bash
kubectl port-forward -n monitoring svc/kps-kube-prometheus-stack-prometheus 9090:9090 &
curl -s localhost:9090/api/v1/query --data-urlencode 'query=up{job="NAMESPACE/async-processor"}'
```

> The chart's `modelServerMonitor` selects `llm-d.ai/role=decode`; if your vLLM pod lacks that label,
> use `prometheus-vllm-podmonitor.yaml` (adjust selector / `namespaceSelector` / port). The Prometheus
> `up` job label is `<namespace>/<podmonitor>`.

### Option A — GCP Cloud Monitoring (Pub/Sub on GKE)

```bash
kubectl apply -n NAMESPACE -f manifests/gmp-podmonitoring.yaml          # AP metrics -> Cloud Monitoring
gcloud monitoring dashboards create --project $PROJECT_ID \
  --config-from-file=dashboards/cloud-monitoring.json
```

The `PodMonitoring` (GMP managed collection) ingests `llm_d_async_async_*`; the dashboard charts
request/success rate, in-flight, and p95 latency, plus **Pub/Sub backlog per team** from the native
`pubsub.googleapis.com/subscription/num_undelivered_messages` metric. Note: `dashboards/cloud-monitoring.json`
groups by `pool_name`, which is now the single shared pool — regroup those panels by `queue_name` for
a per-team breakdown.
**Required:** the collector identity needs `roles/monitoring.metricWriter`. The gate-metric panels
(`async_dispatch_budget`, `async_gate_decisions_total`, worker utilization — #290/#291) need an image
**newer than v0.7.2**; on v0.7.2 those panels show no data. (For the Redis backend, use option B — its
Grafana dashboard covers the same signals, with `async_broker_backlog` in place of the Pub/Sub metric.)

## Notes & gotchas

- **Image pin.** Pin a published release tag (e.g. `v0.7.2`) under
  `ghcr.io/llm-d-incubation/llm-d-async`. The in-repo chart's appVersion may lag the published image
  tag, so an explicit pin avoids an ImagePullBackOff.
- **Reserved quota vs. pool size.** Each team's quota is its *reserved* capacity (priority lane) in
  `classifying` mode, not a hard cap — over-quota flows as `overflow`. Keep the **sum** of reserved
  quotas at or below the shared pool's worker count, so reserved traffic is never itself
  capacity-starved and overflow has room to run after it. (In `blocking` mode the quota *is* a hard
  cap, and must be below the worker count to bind — as in the classic single-pool setup.)
- **Config-only Helm changes need a pod restart.** Changing the queue config / quota updates the
  processor's config, but it's read once at startup — `kubectl rollout restart` afterwards.
- **`prometheus-saturation` gate** is hard-wired to the EPP metric
  `inference_extension_flow_control_pool_saturation`; if your stack doesn't emit it, use the
  `prometheus-query` gate over `vllm:num_requests_running` (as these values do).
- **GMP read path.** Cloud Monitoring dashboards query Monarch directly (no frontend needed). Only
  in-cluster PromQL consumers (the gates) need the `gmp-frontend`.
- **Verifying via the Monitoring API** on a domain-restricted account: `gcloud auth
  print-access-token` may be rejected (401); use `gcloud auth application-default print-access-token`.

## Cleanup

```bash
helm uninstall async-processor -n NAMESPACE
kubectl delete -n NAMESPACE -f manifests/redis.yaml
# option B (Prometheus/Grafana):
kubectl delete -f manifests/prometheus-vllm-podmonitor.yaml
helm uninstall kps -n monitoring && kubectl delete ns monitoring
# option A (GMP) + Pub/Sub backend:
kubectl delete -n NAMESPACE -f manifests/gmp-frontend.yaml -f manifests/gmp-podmonitoring.yaml
gcloud monitoring dashboards list --project $PROJECT_ID --filter='displayName:"Async Processor"' \
  --format='value(name)' | xargs -r -n1 gcloud monitoring dashboards delete --project $PROJECT_ID --quiet
PROJECT_ID=$PROJECT_ID DELETE_SA=1 ./scripts/gcp-teardown.sh
```
