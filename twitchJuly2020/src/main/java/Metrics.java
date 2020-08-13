import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.concurrent.atomic.AtomicLong;

public class Metrics {

    private AtomicLong pending_requests = new AtomicLong(0);
    private AtomicLong worker_queue_size = new AtomicLong(0);
    private AtomicLong completed_requests = new AtomicLong(0);
    private AtomicLong async_requests = new AtomicLong(0);
    private AtomicLong avg_latency = new AtomicLong(0);
    private Integer workers = 0;
    private JsonObject jsonObject = new JsonObject();

    public Metrics() {}

    public long addPendingRequest(int count) {
        return pending_requests.addAndGet(count);
    }

    public long addPendingRequest() {
        return pending_requests.incrementAndGet();
    }

    public long removePendingRequests() {
        return pending_requests.decrementAndGet();
    }

    public long getCompletedCount() {
        return completed_requests.get();
    }

    public long incrementCompletedCount() {
        return completed_requests.get();
    }

    public long resetCompletedCount() {
        return completed_requests.getAndSet(0);
    }

    public void setWorkerQueueSize(long tasks) {
        worker_queue_size.set(tasks);
    }

    public long resetWorkerQueueSize() {
        return worker_queue_size.getAndSet(0);
    }

    public void setAsyncRequests(long async) {
        async_requests.set(async);
    }

    public void setAverageLatency(long latency) {
        avg_latency.set(latency);
    }

    public void setWorkers(Integer count) {
        workers = count;
    }

    @Override
    public String toString() {
        jsonObject.put("pending", pending_requests.get());
        jsonObject.put("completed", completed_requests.get());
        jsonObject.put("queue", worker_queue_size.get());
        jsonObject.put("async", async_requests.get());
        jsonObject.put("avg_latency", avg_latency.get());
        jsonObject.put("workers", workers);

        return jsonObject.encode();
    }

}
