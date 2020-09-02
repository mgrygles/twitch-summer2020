import io.vertx.core.json.Json;
import rx.Observable;
import rx.Subscription;

class KotlinWorkerVerticle : io.vertx.core.AbstractVerticle() {
    private var instance = 0
    private var completed = 0
    private var logger: Logger = LoggerFactory.getLogger("KOTLIN_WORKER")
    private var metrics_obj : JsonObject = JsonObject()

    override fun start() {
        logger.info("[Kotlin Worker] Starting in ${java.lang.Thread.currentThread().getName()}")
        instance = config().getInteger("instance")
        logger.info("[Kotlin Worker] instance ${instance}")

        vertx.setPeriodic(1000, { id ->
            metrics_obj.put("instance", instance)
            metrics_obj.put("queue", 0)
            metrics_obj.put("completed", completed)
            metrics_obj.put("avg_latency", 0)
            completed = 0
            vertx.eventBus().publish("api.to.client", Jscon.encode(metrics_obj));
        })

        vertx.eventBus().consumer<String>("WORKER" + instance, { message ->
            logger.info("[Kotlin Worker] Consuming data in ${java.lang.Thread.currentThread().getName()}")
            var id = message.body().toInt()
            var r = Resource(id, "", "")
            message.reply(Json.encodePrettily(r))
            ++completed
        })
    }

    override fun stop() {
        logger.info("[Kotlin Worker] Stopping in ${java.lang.Thread.currentThread().getName()}")
    }
}