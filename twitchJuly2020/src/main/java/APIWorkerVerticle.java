import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import rx.Scheduler;
import rx.schedulers.Schedulers;



public class APIWorkerVerticle extends AbstractVerticle {

    private JsonObject configJson = null;
    private Integer max_delay_ms;
    private ExecutorService worker_executor;
    private Schedulers scheduler;
    private


    private Logger logger = LoggerFactory.getLogger(APIWorkerVerticle.class);

    @Override
    public void start() throws Exception {
        String configPath = System.getProperty("reactiveapi.config", "conf/config.json");
        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setFormat("json")
                .setConfig(new JsonObject().put("path", configPath));

        ConfigRetriever configRetriever = ConfigRetriever.create(vertx,
                new ConfigRetrieverOptions().addStore(fileStore));
        configRetriever.getConfig(
                config-> {
                    if (config.faied()) {
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

    private void processConfig(JsonObject config) {
        configJson = config;
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
    }
}
