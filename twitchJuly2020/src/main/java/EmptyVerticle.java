import io.vertx.core.AbstractVerticle;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.logging.Logger;
import io.vertx.core.Vertx;

public class EmptyVerticle extends AbstractVerticle {
    private final Logger logger = LoggerFactory.getLogger(EmptyVerticle.class);

    @Override
    public void start() {
        logger.info("Start");
    }

    @Override
    public void stop() {
        logger.info("Stop");
    }

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new EmptyVerticle());
    }
}
