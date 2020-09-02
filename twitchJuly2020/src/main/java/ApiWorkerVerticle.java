import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.config.ConfigRetriever;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

/**
 * Worker Verticle to handle requests asynchronously
 */
public class ApiWorkerVerticle extends AbstractVerticle {

    private Logger logger;
    private ResourceAPI api;
    private Integer max_delay_ms;
    private Subscription metrics_timer_sub;
    private JsonObject config = null;
    private ExecutorService worker_executor = null;
    private Scheduler scheduler;
    private Metrics metrics;
    private AtomicLong latency = new AtomicLong(0);
    private Integer instance = null;
    private JsonObject metrics_obj = new JsonObject();

    public ApiWorkerVerticle() {
        super();
    }

    @Override
    public void start() throws Exception {
        JsonObject try_config = config();
        if (try_config != null) {
            instance = try_config.getInteger("instance", 999);
            logger = LoggerFactory.getLogger("WORKER-" + instance);
        } else {
            logger = LoggerFactory.getLogger("WORKER");
        }
        metrics = new Metrics();

        String path_to_config = System.getProperty("reactiveapi.config", "conf/config.json");
        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setFormat("json")
                .setConfig(new JsonObject().put("path", path_to_config));

        ConfigRetriever retriever = ConfigRetriever.create(vertx, new ConfigRetrieverOptions().addStore(fileStore));
        retriever.getConfig(
                config -> {
                    logger.info("config retrieved");
                    if (config.failed()) {
                        logger.info("No config");
                    } else {
                        logger.info("Got config");
                        startup(config.result());
                    }
                }
        );

        retriever.listen(change -> {
            logger.info("config changed");
            JsonObject prev = change.getPreviousConfiguration();
            JsonObject conf = change.getNewConfiguration();
        });
    }

    private void processConfig(JsonObject config) {
        config = config;
    }

    private void startup(JsonObject config) {
        processConfig(config);

        max_delay_ms = config.getInteger("max-delay-ms", 1000);
        Integer worker_pool_size = config.getInteger("worker-pool-size", Runtime.getRuntime().availableProcessors() * 2);
        logger.info("max-delay-ms={}  worker-pool-size={}", max_delay_ms, worker_pool_size);
        worker_executor = Executors.newFixedThreadPool(worker_pool_size);
        scheduler = Schedulers.from(worker_executor);
        api = new ResourceAPI(worker_executor, max_delay_ms);
        api.build();
        MessageConsumer<String> consumer =
                vertx.eventBus().consumer("WORKER" + instance);

        consumer.handler(m -> {
            getResponse(m);
        });

        metrics_timer_sub = Observable.interval(1, TimeUnit.SECONDS)
                .subscribe(delay -> {
                            measure();
                        },
                        error -> {
                            logger.error("Metrics error", error);
                        },
                        () -> {
                        });
    }

    private void measure() {
        metrics_obj.put("instance", instance);
        int qsize = ((ThreadPoolExecutor) worker_executor).getQueue().size();
        metrics_obj.put("queue", qsize);
        metrics_obj.put("completed", metrics.getCompletedCount());
        if (metrics.getCompletedCount() > 0) {
            metrics_obj.put("avg_lat", latency.get() / metrics.getCompletedCount());
        } else {
            metrics_obj.put("avg_lat", 0);
        }

        vertx.eventBus().publish("api.to.client", Json.encode(metrics_obj));
        metrics.resetCompletedCount();
        latency.set(0);
    }

    private void getResponse(Message<String> m) {
        String id = m.body();
        int idAsInt = Integer.parseInt(id);
        long startTS = System.nanoTime();
        api.fetchResource(idAsInt)
                .subscribeOn(scheduler)
                .observeOn(scheduler)
                .subscribe(r -> {
                            logger.info("Sending response for request {} Outstanding {} ", id, metrics.removePendingRequests());
                            metrics.incrementCompletedCount();
                            latency.addAndGet((System.nanoTime() - startTS) / 1000000);
                            m.reply(Json.encodePrettily(r));
                        },
                        e -> {
                            logger.info("Sending response for request {} Outstanding {} ", id, metrics.removePendingRequests());
                            metrics.incrementCompletedCount();
                            latency.addAndGet((System.nanoTime() - startTS) / 1000000);
                            m.fail(0, "API Error");
                        },
                        () -> {
                        });
    }
}
