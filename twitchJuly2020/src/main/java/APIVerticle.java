import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.config.ConfigRetriever;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.MessageProducer;
import io.vertx.rxjava.core.http.HttpServerResponse;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.ext.web.handler.CorsHandler;
import io.vertx.rxjava.ext.web.handler.StaticHandler;
import io.vertx.rxjava.ext.web.handler.sockjs.SockJSHandler;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

/**
 * APIVerticle:  A Verticle that handles multiple API requests and funnels them accordingly
 */
public class APIVerticle extends AbstractVerticle {

    static final int WORKER_POOL_SIZE = 20;

    public Router router;
    public Integer max_delay_ms;
    public Subscription metrics_timer_sub;
    private JsonObject configJson = null;
    private ExecutorService worker_executor;
    private Scheduler scheduler;
    private Metrics metrics;
    private AtomicLong latency = new AtomicLong(0);
    private Integer worker_count = 0;
    private Integer worker_pool_size;
    private AtomicInteger current_workers = new AtomicInteger(0);
    private Integer instance_counter = 1;
    private Stack<String>  deployed_verticles = new Stack<String>();
    private AtomicInteger next_worker = new AtomicInteger(1);


    private Logger logger = LoggerFactory.getLogger(APIVerticle.class);

    public APIVerticle() {
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
                    logger.info("Retrieved config");
                    if (config.failed()) {
                        logger.info("Unable to find the config file!");
                    } else {
                        logger.info("Got the config file successfully!");
                        startup(config.result());
                    }
                }
        );

        configRetriever.listen(change -> {
            logger.info("config file modified");
            // saving the previous config
            JsonObject previousConfig = change.getPreviousConfiguration();
            // now get the new config
            JsonObject conf = change.getNewConfiguration();
            processConfigChange(previousConfig, conf);
        });

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
        worker_count = config.getInteger("worker-count", 1);
        max_delay_ms = config.getInteger("max-delay-ms", 1000);
        worker_pool_size = config.getInteger("worker-pool-size", Runtime.getRuntime().availableProcessors() * 2);
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
                },
                error -> {
                    logger.error("Metrics error", error);
                },
                () -> {});
    }

    private void processConfigChange(JsonObject prev, JsonObject curr) {
    }

    private void startup(JsonObject config) {
    }

    private void measure(){
    }
}
