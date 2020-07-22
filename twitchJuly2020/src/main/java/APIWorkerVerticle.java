import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;
import rx.Subscription;



public class APIVerticle extends AbstractVerticle {

    private JsonObject configJson = null;
    private Integer max_delay_ms;
    private ExecutorService worker_executor;
    private Schedulers scheduler;
    private Metrics metrics;
    private AtomicLong latency = new AtomicLong(0);
    private Integer instance = null;


    private Logger logger = LoggerFactory.getLogger(APIWorkerVerticle.class);

    public ApiVerticle() {
        super();
        metrics = new Metrics();
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting APIVerticle");

        String configPath = System.getProperty("reactiveapi.config", "conf/config.json");
        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setFormat("json")
                .setConfig(new JsonObject().put("path", configPath));

        ConfigRetriever configRetriever = ConfigRetriever.create(vertx,
                new ConfigRetrieverOptions().addStore(fileStore));
        configRetriever.getConfig(
                config-> {
                    if (config.failed()) {
                        logger.info("Unable to find the config file!");
                    } else {
                        logger.info("Got the config file successfully!");
                        startup(config.result);
                    }
                }
        );

        configRetriever.listen((change-> {
            logger.info("config file updated");
            JsonObject previousConfig = change.getPreviousConfiguration();
            JsonObject conf = change.getNewconfiguration();
        }));
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping APIVerticle");
        worker_executor.shutdown();
        if (metrics_timer_sub != null) {
            metrics_timer_sub.unsubscribe();
        }
    }

    private void processConfig(JsonObject config) {
        configJson = config;
        Integer worker_count = config.getInteger("worker-count", 1);
        max_delay_ms = config.getInteger("max-delay-ms", 1000);
        Integer worker_pool_size = config.getInteger("worker-pool-size", Runtime.getRuntime().availableProcessors() * 2);
        logger.info("max_delay_ms: {}  worker-count: {}", max_delay_ms, worker_pool_size);
        if (worker_executor == null) {
            worker_executor = Executors.newFixedThreadPool(worker_pool_size);
        }
        if (scheduler == null) {
            scheduler = Schedulers.from(worker_executor);
        }
        metrics_timer_sub = Observable.interval(1, TimeUnit.SECONDS)
                .subscribe(delay -> {
                    measure();
                }),
                .error -> {
                    logger.error("Metrics error", error);
                },
                () -> {} );
        }


    }

    private void startup(JsonObject config) {
        processConfig(config);

        max_delay_ms = configJson.getInteger("max-delay-ms", 1000);
        Integer worker_pool_size = configJson.getInteger("worker-pool-size", Runtime.getRuntime().availableProcessors() * 2);
        logger.info("max_delay_ms={}  worker_pool_size={}", max_delay_ms, worker_pool_size);
        worker_executor = Executors.newFixedThreadPool(worker_pool_size);
        scheduler = Schedulers.from(worker_executor);
        apiResource = new ResourceAPI(worker_executor, max_delay_ms);
        apiResource.build();
        MessageConsumer<String> consumer =
                vertx.eventBus().consumer("WORKER" + instance);

        consumer.handler (m -> {
            getResponse(m);
        });

        metrics_timer_sub = Observable.interval(1, TimeUnit.SECONDS)
                .subscribe(delay->{
                            measure();
                        },
                        error-> {
                            logger.error("Metrics error", error);
                        },
                        () -> {}
                );
    }

    private void getResponse(Message<String> m) {
        String id = m.body();
        int idAsInt = Integer.parseInt(id);
        long startTS = System.nanoTime();
        api.fetchResource(idAsInt)
                .subscribeOn(scheduler)
                .observeOn(scheduler)
                .subscribe(r -> {
                            logger.info("Sending response for request {} Outstanding {}", id, metrics.removePendingRequest());
                            metrics.incrementCompletedCount();
                            latency.addAndGet((System.nanoTime() - startTS) / 1000000);
                            m.reply(Json.encodePrettily(r));
                        },
                        e -> {
                            logger.info("Sending response for request {} Outstanding {}", id, metrics.removedPendingRequest());
                            metrics.incrementCompletedCount();
                            latency.addAndGet((System.nanoTime() - startTS) / 1000000);
                            m.fail(0, "API Error");
                        },
                        () -> {
                        });
    }


                })

    }



}
