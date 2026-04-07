# Design Document: GCS-Based Message Queue Implementation

## 1. Introduction
This document proposes the design for a 3rd message queue implementation for the Async Processor, using Google Cloud Storage (GCS). This implementation will complement the existing Redis and GCP PubSub flows.

## 2. Proposed Architecture: Hybrid Event-Driven GCS Flow
To achieve high scalability and low latency while supporting large initial datasets, we propose a **Hybrid GCS-PubSub approach**.

### Components:
*   **GCS Bucket:** The primary storage for message payloads (requests) and results.
*   **GCS Object Change Notifications:** Configured to push events to a PubSub topic.
*   **PubSub Topic/Subscription:** Acts as the high-throughput event bus for incoming files.
*   **GCS Flow Worker:** Subscribes to PubSub to receive real-time notifications and performs bucket traversals for initial loading.

---

## 3. Processing incoming files (High-Scale)
For "incoming files," we rely on GCS-to-PubSub notifications rather than polling.

### Workflow:
1.  **Upload:** An external producer uploads a file to `gs://request-bucket/new/`.
2.  **Notify:** GCS automatically publishes a message to a PubSub topic (e.g., `gcs-notifications-topic`).
3.  **Receive:** The Async Processor's GCS Flow receives the PubSub message containing the object name.
4.  **Download:** The worker downloads the file from GCS and decodes it into an `api.RequestMessage`.
5.  **Process:** The message is pushed to the internal `RequestChannel`.
6.  **Cleanup/Ack:** Once the internal worker finishes:
    *   **Success:** Write result to `gs://request-bucket/results/` and delete/move the request file.
    *   **Failure:** Leave the file in `new/` or move to `failed/` for inspection.
    *   **Ack:** Acknowledge the PubSub message.

### Why this scales:
*   **Parallelism:** PubSub handles the distribution of notifications across many workers.
*   **Efficiency:** No wasted listing calls; workers only act when a file actually arrives.

---

## 4. Injecting Initial Content
To process millions of existing files ("initial content"), a one-time or recurring "Bootstrap traversal" is used.

### Strategy:
1.  **Recursive Listing:** Use the GCS `Objects.List` API with a prefix filter (e.g., `new/`).
2.  **Pagination:** Iterate using `PageToken` to prevent memory exhaustion when millions of objects exist.
3.  **Atomic Handover:** To avoid double-processing between the "Bootstrap Listing" and the "Real-time Notifications," the worker must use an **atomic move** operation or GCS **Object Preconditions** (`IfGenerationMatch`).
4.  **Batching:** For very large sets, we can shard the prefix listing (e.g., `new/a*`, `new/b*`) to run multiple bootstrap workers in parallel.

---

## 5. Reasoning and Tradeoffs

| Feature | Polling (Traversals) | Notifications (PubSub) | Hybrid (Proposed) |
| :--- | :--- | :--- | :--- |
| **Scalability** | **Low:** Listing operations are slow and expensive at scale. | **High:** Event-driven; scales with PubSub's throughput. | **Very High:** Combines event-driven speed with reliable bootstrap. |
| **Latency** | **High:** Limited by polling interval. | **Low:** Near real-time. | **Low:** Real-time for new, batch-speed for initial. |
| **Durability** | **High:** Files reside in GCS. | **High:** Backed by GCS and PubSub. | **High:** Triple redundancy (GCS, PubSub, Worker logic). |
| **Complexity** | **Simple:** One GCS client. | **Medium:** Requires PubSub topic + Notification config. | **Medium-High:** Requires management of both mechanisms. |

### Pros of GCS Queue:
*   **Payload Size:** Unlike Redis (limited by memory) or PubSub (10MB limit), GCS can handle massive request/response payloads (GBs).
*   **Durable Audit Trail:** All requests and results are natively persisted and searchable in GCS.
*   **Cost-Effective for Batch:** Very cheap storage for high-volume, non-urgent tasks.

### Cons of GCS Queue:
*   **IO Overhead:** Higher latency per message compared to Redis.
*   **Atomicity:** GCS lacks an atomic "pop" operation. This must be emulated via file renaming or GCS metadata flags, which adds complexity to the state machine.

---

## 6. Implementation Plan
1.  **Interface Adherence:** Implement `GCSMQFlow` in `pkg/gcs/gcsimpl.go`.
2.  **Client Factory:** Initialize GCS and PubSub clients during `Start()`.
3.  **Notification Worker:** Implement a PubSub receiver that triggers GCS downloads.
4.  **Bootstrap Worker:** Implement the listing logic with pagination.
5.  **Result Sink:** Implement a consumer for `ResultChannel` that writes objects back to GCS.
6.  **Configuration:** Extend `cmd/main.go` flags to support GCS bucket names and PubSub topic/sub IDs.
