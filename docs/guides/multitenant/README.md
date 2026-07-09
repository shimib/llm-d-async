# Multi-tenant quota, priority & saturation

A runnable guide for the [Async Processor](../../../charts/async-processor) across three dimensions —
**team × tier × model** — giving each team a **per-team quota** (reserved vs. overflow) and **priority
tier**, and each **model** its own worker pool with independent **saturation-aware back-off**, observed
through either **self-hosted Prometheus + Grafana** or **GCP Cloud Monitoring**.

The scenario is the point; the **message queue is a pluggable backend**. It runs unchanged on:

- **Redis SortedSet** (`redis-sortedset`) — portable, no cloud dependency; queues are scored by
  request `deadline`, so within a team the processor dispatches **earliest-deadline-first**.
- **GCP Pub/Sub** (`gcp-pubsub-gated`) — managed queue on GCP.

The gate configuration, worker pools, observability, and the scenario walkthroughs below are
identical across both; only the queue wiring and how you publish differ.

![Animated architecture: two models each with their own worker pool and vLLM; within each, three team/tier lanes flow through a reserved/overflow quota gate and the tier-priority merge; model A saturates and its pool parks while model B keeps flowing](diagram/architecture.gif)

> The loop shows the full **team × tier × model** picture: two models (each its own pool + vLLM), and
> within each, the reserved/overflow × tier lanes of the tier-priority merge policy — a hot model backs
> off while the other keeps flowing. Source + regeneration: [`diagram/`](diagram/) (`architecture.html`
> is the editable animated SVG).

## What it shows

The demo runs **two models**, each served by its own `InferencePool` behind one gateway. Each model
gets its **own worker pool** (`model-a`, `model-b`) — so concurrency and saturation are isolated per
model — and within each pool three teams contend by **tier** and **quota**. That's **6 queues**
(3 teams × 2 models) → **2 pools**:

| Model → pool | Team | Queue (Redis) | Tier | Reserved quota | Quota prefix |
| :-- | :-- | :-- | :-- | :-- | :-- |
| **MODEL_A** → `model-a` | premium | `team-premium-a` | `interactive` | concurrency **2** | `quota:a:` |
| | standard | `team-standard-a` | `async` | concurrency **2** | `quota:a:` |
| | batch | `team-batch-a` | `batch` | concurrency **1** | `quota:a:` |
| **MODEL_B** → `model-b` | premium | `team-premium-b` | `interactive` | concurrency **2** | `quota:b:` |
| | standard | `team-standard-b` | `async` | concurrency **2** | `quota:b:` |
| | batch | `team-batch-b` | `batch` | concurrency **1** | `quota:b:` |

The three dimensions:

- **Model → pool isolation.** The publisher sends a request to the `(team, model)` queue and sets
  `payload.model`; the gateway routes it to that model's `InferencePool`. Each pool has its own
  workers and its own saturation gate (Scenario C), so one model saturating does not park the other.
- **Team → reservation classification.** The per-team **`redis-quota`** gate runs in **`classifying`**
  mode (keyed on `metadata.team`, with a **per-model prefix** so each `(team, model)` has its own
  counter): within quota → `reserved` (org-guaranteed), over quota → `overflow` (admitted and
  deprioritized, **not** nacked).
- **Tier → priority.** A per-queue `tier` label: `interactive` (premium) > `async` (standard) > `batch`.

The [**tier-priority merge policy**](https://github.com/llm-d-incubation/llm-d-async/pull/294) runs
**per pool independently**: within each model it buckets requests into **6 strict lanes** by
`(classification, tier)`, dispatches them in order, and stamps **`x-gateway-priority`** (0 = highest …
5 = lowest):

| Lane | `x-gateway-priority` | Who (within one model) |
| :-- | :-- | :-- |
| reserved + interactive | 0 | premium within quota |
| reserved + async | 1 | standard within quota |
| reserved + batch | 2 | batch within quota |
| overflow + interactive | 3 | premium over quota |
| overflow + async | 4 | standard over quota |
| overflow + batch | 5 | batch over quota |

So within each model **all reserved traffic drains before any overflow** (org priority), tier-ordered
within each class; and the two models are fully independent. On Redis SortedSet, within a lane
dispatch is earliest-deadline-first.

**Saturation (Scenario C)** gives each model pool its **own** `wait-on-refuse(prometheus-query)` gate,
scoped to that model's `InferencePool`: `D = clamp(1 - sum(vllm:num_requests_running{inference_pool="POOL_A"})/N, 0, 1)`
for `model-a` (and `POOL_B` for `model-b`). When one model saturates, only its pool **parks**
(`ActionWait`, in-memory — no broker churn) while the other keeps flowing; as a pool's capacity
frees, its merge policy drains the highest lanes first. (For a *tier-aware* saturation gate that
treats lanes differently — wait `reserved`, 429 `interactive`+`overflow`, refuse
`async`/`batch`+`overflow` — see the `tier-priority-admission` gate from [#294]; these overlays use
the simpler per-pool `wait-on-refuse` guard.)

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

- A Kubernetes cluster running an **llm-d stack** with **two models**: one inference gateway (EPP)
  that routes by `payload.model` to **two `InferencePool`s** (one per model), each with its vLLM model
  server. See the [e2e-deploy guide](../e2e-deploy.md) to bring one up. (For a single-model variant,
  point both `model-a` and `model-b` at the same model/pool — you lose the model-isolation story but
  the quota/tier behavior is unchanged.)
- `kubectl` and `helm` (v3). **For the Pub/Sub backend only:** a GCP project with the Pub/Sub (+
  Monitoring, for option A) APIs enabled and `gcloud` authenticated.
- The async-processor chart **v0.7.2+**. This guide installs the in-repo chart at
  `../../../charts/async-processor`.

Replace these placeholders throughout `values/` and the commands below:

| Placeholder | Meaning |
| :-- | :-- |
| `NAMESPACE` | namespace for the demo (e.g. `async-demo`) |
| `IGW_HOST` | inference gateway host/IP (used as `http://IGW_HOST:80`) |
| `MODEL_A` / `MODEL_B` | the two served model names (go in `payload.model`) |
| `POOL_A` / `POOL_B` | the two `InferencePool` names (saturation-gate `inference_pool` scope) |
| `PROJECT_ID` | your GCP project id (**Pub/Sub backend only**) |

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

A request is a JSON body — `id`, `created`, `deadline`, a `payload` (a valid completions body whose
**`model` selects the model/pool at the gateway**), and `metadata.team` (the key the quota gate
reads). Publish to the **`(team, model)`** queue. The helper takes a team and a model (`a`|`b`).

**Redis SortedSet** — `ZADD` onto the `team-<team>-<model>` queue with the **`deadline` as the score**;
the member is the processor's envelope (`request_kind: "plain"`):

```bash
export MODEL_A=<model-a> MODEL_B=<model-b>
publish() {                                   # publish <team> <a|b> [count]
  local team=$1 model=$2 n=${3:-1} now dl member name
  [ "$model" = a ] && name="$MODEL_A" || name="$MODEL_B"
  for i in $(seq 1 "$n"); do
    now=$(date +%s); dl=$((now+300))
    member=$(printf '{"internal":{},"request_kind":"plain","data":{"id":"%s-%s-%s-%s","created":%s,"deadline":%s,"payload":{"model":"%s","prompt":"summarize this","max_tokens":64},"metadata":{"team":"%s"}}}' \
      "$team" "$model" "$now" "$i" "$now" "$dl" "$name" "$team")
    kubectl -n NAMESPACE exec -i deploy/redis -- redis-cli ZADD "team-${team}-${model}" "$dl" "$member" >/dev/null
  done
}
# e.g.  publish premium a 5    # premium team, model A
```

**GCP Pub/Sub** — publish to the `team-<team>-<model>-requests` topic with a `team` attribute:

```bash
export PROJECT_ID=your-project MODEL_A=<model-a> MODEL_B=<model-b>
publish() {                                   # publish <team> <a|b> [count]
  local team=$1 model=$2 n=${3:-1} now name
  [ "$model" = a ] && name="$MODEL_A" || name="$MODEL_B"
  for i in $(seq 1 "$n"); do
    now=$(date +%s)
    gcloud pubsub topics publish "team-${team}-${model}-requests" --project "$PROJECT_ID" \
      --attribute "team=${team}" \
      --message "$(printf '{"id":"%s-%s-%s-%s","created":%s,"deadline":%s,"payload":{"model":"%s","prompt":"summarize this","max_tokens":64},"metadata":{"team":"%s"}}' \
        "$team" "$model" "$now" "$i" "$now" "$((now+300))" "$name" "$team")"
  done
}
```

> Both helpers publish serially — fine for the functional checks below. For sustained
> **rate/concurrency** load (Scenarios B and C), wrap `publish` in background loops or use your own
> publisher that sets the same `team` key, `payload.model`, and body.

## Scenarios A & B — reserved vs. overflow

**A. Steady state (one model)** — each team within its reserved quota on model A:

```bash
for t in premium standard batch; do publish "$t" a 1 & done; wait
```

Every request is within its team's quota, so all are `reserved` and dispatched in tier order
(premium→standard→batch), stamped `x-gateway-priority` 0/1/2. Results are delivered per backend:

- **Redis:** `LPUSH` onto the per-model list — `kubectl -n NAMESPACE exec deploy/redis -- redis-cli LRANGE results-a-list 0 -1` (model B → `results-b-list`).
- **Pub/Sub:** published to the single `results` topic — pull from the `results-sub` subscription.

> Each result is a JSON object with `id`, `payload` (the upstream response body), and `status_code`
> (the upstream HTTP status). Non-HTTP failures carry `status_code: 0` plus `error_code`/`error_message`
> (e.g. `GATE_DROPPED`, `DEADLINE_EXCEEDED`). Consumers branch on `status_code > 0` vs. `error_code`.

**B. Overflow deprioritization + model isolation** — flood `batch` on **model A** past its reserved
quota (1) while `premium` on **model A** and everything on **model B** run within quota:

```bash
publish batch   a 100 &   # far exceeds batch's reserved 1 on A -> excess is overflow (lane 5)
publish premium a 20  &   # premium reserved on A (lane 0) -> always jumps ahead
publish premium b 20  &   # model B, unaffected by A's overload
wait
```

Two things to observe:

- **Priority within model A:** batch's first concurrent request stays `reserved` (lane 2); the rest are
  `overflow` (lane 5), dispatched only **after** all reserved and higher-tier overflow. premium's
  `reserved` (lane 0) always drains first. The per-`(team, model)` counter caps at the reserved limit;
  the excess flows as overflow (not nacked):

  ```bash
  kubectl -n NAMESPACE exec deploy/redis -- redis-cli GET quota:a:team:batch   # <= 1 (model A, batch)
  kubectl -n NAMESPACE exec deploy/redis -- redis-cli GET quota:b:team:premium # model B counter, independent
  ```
- **Model isolation:** model B has its own pool and its own counters, so A's batch overload does not
  slow B — `results-b-list` keeps filling at model B's own rate.

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

Drive heavy load on **model A** (`publish premium a 200 & publish batch a 200 &`) while keeping
**model B** light. As model A's `InferencePool` saturates, `model-a`'s budget → 0 and its workers
**park in-memory (`ActionWait`)** — so `model-a` stops pulling new work without churning the backlog,
while **`model-b` keeps dispatching at full rate** (its own gate reads only `POOL_B`). This is the
model-isolation payoff: a hot model can't starve a cold one. As `model-a`'s capacity frees, its merge
policy drains the highest lanes first (`reserved`+`interactive` ahead of everything, `overflow`+`batch`
last). Query each model's budget independently:

```bash
# self-hosted Prometheus (real-time):
kubectl port-forward -n monitoring svc/kps-kube-prometheus-stack-prometheus 9090:9090 &
# GMP frontend (Pub/Sub option A):
# kubectl port-forward -n NAMESPACE deploy/gmp-frontend 9090:9090 &
curl -s localhost:9090/api/v1/query --data-urlencode \
  'query=clamp(1 - sum(vllm:num_requests_running{inference_pool="POOL_A"})/20, 0, 1)'   # model-a budget
curl -s localhost:9090/api/v1/query --data-urlencode \
  'query=clamp(1 - sum(vllm:num_requests_running{inference_pool="POOL_B"})/20, 0, 1)'   # model-b budget (stays ~1)
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
Cloud Monitoring on Pub/Sub). Break panels down by **`pool_name`** (`model-a` / `model-b`) for the
per-model view and by **`queue_name`** (`team-<team>-<model>`) for the per-team-per-model view. Verify
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
predates the model dimension — its panels group by `pool_name` (now `model-a`/`model-b`); add a
`queue_name` breakdown for per-(team, model) detail.
**Required:** the collector identity needs `roles/monitoring.metricWriter`. The gate-metric panels
(`async_dispatch_budget`, `async_gate_decisions_total`, worker utilization — #290/#291) need an image
**newer than v0.7.2**; on v0.7.2 those panels show no data. (For the Redis backend, use option B — its
Grafana dashboard covers the same signals, with `async_broker_backlog` in place of the Pub/Sub metric.)

## Notes & gotchas

- **Image pin.** Pin a published release tag (e.g. `v0.7.2`) under
  `ghcr.io/llm-d-incubation/llm-d-async`. The in-repo chart's appVersion may lag the published image
  tag, so an explicit pin avoids an ImagePullBackOff.
- **Reserved quota vs. pool size (per model).** Each team's quota is its *reserved* capacity (priority
  lane) in `classifying` mode, not a hard cap — over-quota flows as `overflow`. Within **each model
  pool**, keep the **sum** of that model's reserved quotas at or below the pool's worker count, so
  reserved traffic is never itself capacity-starved and overflow has room to run after it. (In
  `blocking` mode the quota *is* a hard cap and must be below the worker count to bind.)
- **Per-model quota counters.** Counters are keyed `quota:<a|b>:team:<team>` (the per-queue `prefix`),
  so a team's reserved capacity on model A is independent of its capacity on model B.
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
