import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSBridgeOptions;
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
                        logger.warn("Unable to find the config file!");
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
        processConfig(config);

        // Create a Router object: to handle requests
        router = Router.router(vertx);

        // Handle CORS requests
        router.route().handler(CorsHandler.create("*")
                .allowedMethod(HttpMethod.GET)
                .allowedMethod(HttpMethod.OPTIONS)
                .allowedHeader("Accept")
                .allowedHeader("Authorization")
                .allowedHeader("Content-Type"));

        router.get("/health").handler(this::generateHealth);
        router.get("/api/resources").handler(this::getAll);
        router.get("/api/resources/:id").handler(this::getOne);
        router.route("/static/*").handler(StaticHandler.create());

        // set in/out Permission for the addresses
        SockJSBridgeOptions opts = new SockJSBridgeOptions()
                .addInboundPermitted(new PermittedOptions().setAddress("client.to.api"))
                .addInboundPermitted(new PermittedOptions().setAddress("api.to.client"))
                .addInboundPermitted(new PermittedOptions().setAddress("worker.to.client"));

        SockJSHandler ebusHandler = SockJSHandler.create(vertx).bridge(opts);
        router.route("/eventbus/*").handler(ebusHandler);

        // Create the HTTP server and pass the "accept" method to the request handler
        int port = config.getInteger("port", 8080);
        vertx.createHttpServer()
                .requestHandler(router::accept)
                .listen(
                        port,
                        result -> {
                            if (result.succeeded()) {
                                logger.info("Listening now on port {}", port);
                                deployJavaWorker();
                                deployJSWorker();
                                deployKotlinWorker();
                            } else {
                                logger.error("Unable to listen", result.cause());
                            }
                        }
                );

        vertx.eventBus().consumer("chat.to.server").handler(message -> {
            vertx.eventBus().publish("chat.to.client", "Test: ");
        });
    }

    private void getAll(RoutingContext rctx) {
        rctx.response()
                .putHeader("content-type", "application/json; charset=utf-8")
                .end(Json.encodePrettily(null));
    }

    private void getOne(RoutingContext rctx) {
        HttpServerResponse response = rctx.response();
        String id = rctx.request().getParam("id");
        logger.info("Request for {} Outstanding {}", id, metrics.addPendingRequest());
        int idAsInt = Integer.parseInt(id);
        long startTS = System.nanoTime();

        MessageProducer<String> producer = vertx.eventBus().publisher("WORKER" + getNextWorker());
        producer.send(id, result -> {
            if (result.succeeded()) {
                logger.info("Sending response for request {} Outstanding {}", id, metrics.removePendingRequests());
                metrics.incrementCompletedCount();
                latency.addAndGet((System.nanoTime() - startTS) / 1000000);
                if (response.closed() || response.ended()) {
                    return;
                }
                response.setStatusCode(201)
                        .putHeader("content-type", "application/json; charset=utf-8")
                        .end(Json.encodePrettily(result.result()));
            } else {
                logger.info("Sending error response for request {} Outstanding {}", id, metrics.removePendingRequests());
                metrics.incrementCompletedCount();
                latency.addAndGet((System.nanoTime() - startTS) / 1000000);
                if (response.closed() || response.ended()) {
                    return;
                }

                response.setStatusCode(404)
                        .putHeader("content-type", "application/json; charset=utf-8")
                        .end();
            }
        });
    }

    private int getNextWorker() {
        int worker_index = 1;
        if (current_workers.get() > 0) {
            worker_index = next_worker.getAndIncrement();
            if (next_worker.get() > current_workers.get()) {
                next_worker.set(1);
            }
        }
    }

    private void generateHealth(RoutingContext rctx) {
        rctx.response()
                .setChunked(true)
                .putHeader("Content-type", "application/json;charset=utf-8")
                .putHeader("Access-Control-Allow-Methods", "GET")
                .putHeader("Access-Control-Allow-Origin", "*")
                .putHeader("Access-Control-Allow-Headers", "Accept, Authorization, Content-Types")
                .write((new JsonObject().put("status", "OK")).encode())
                .end();
    }

    private void measure(){
        int qsize = ((ThreadPoolExecutor) worker_executor).getQueue().size();
        metrics.setWorkerQueueSize(qsize);
        if (metrics.getCompletedCount() > 0) {
            metrics.setAverageLatency(latency.get() / metrics.getCompletedCount());
        } else {
            metrics.setAverageLatency(0);
        }

        vertx.eventBus().publish("api.to.client", metrics.toString());
        metrics.resetCompletedCount();
        latency.set(0);
    }

    private void deployJavaWorker() {
        current_workers.incrementAndGet();
        JsonObject config = new JsonObject().put("instance", current_workers.get());
        DeploymentOptions workerOpts = new DeploymentOptions()
                .setConfig(config)
                .setWorker(true)
                .setInstances(1)
                .setWorkerPoolSize(1);
        vertx.deployVerticle(ApiWorkerVerticle.class.getName(), workerOpts, res-> {
            if (res.failed()) {
                logger.error("Failed to deploy Java worker verticle {}", ApiWorkerVerticle.class.getName(), res.cause());
            } else {
                String depId = res.result();
                deployed_verticles.add(depId);
                logger.info("Successfully deployed Java worker verticle {} DeploymentID {}", ApiWorkerVerticle.class.getName(), depId);
            }
        });
    }

    private void deployKotlinWorker() {
        current_workers.incrementAndGet();
        JsonObject config = new JsonObject().put("instance", current_workers.get());
        DeploymentOptions workerOpts = new DeploymentOptions()
                .setConfig(config)
                .setWorker(true)
                .setInstances(1)
                .setWorkerPoolSize(1);
        vertx.deployVerticle(KotlinWorkerVerticle.class.getName(), workerOpts, res-> {
            if (res.failed()) {
                logger.error("Failed to deploy Java worker verticle {}", KotlinWorkerVerticle.class.getName(), res.cause());
            } else {
                String depId = res.result();
                deployed_verticles.add(depId);
                logger.info("Successfully deployed Java worker verticle {} DeploymentID {}", KotlinWorkerVerticle.class.getName(), depId);
            }
        });
    }

    private void deployJSWorker() {
        current_workers.incrementAndGet();
        JsonObject config = new JsonObject().put("instance", current_workers.get());
        DeploymentOptions workerOpts = new DeploymentOptions()
                .setConfig(config)
                .setWorker(true)
                .setInstances(1)
                .setWorkerPoolSize(1);
        vertx.deployVerticle("worker_verticle.js", workerOpts, res-> {
            if (res.failed()) {
                logger.error("Failed to deploy Javascript worker verticle {}", "worker_verticle.js", res.cause());
            } else {
                String depId = res.result();
                deployed_verticles.add(depId);
                logger.info("Successfully deployed Java worker verticle {} DeploymentID {}", "worker_verticle.js", depId);
            }
        });
    }

}
