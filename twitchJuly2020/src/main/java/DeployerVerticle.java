import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public  class DeployerVerticle extends AbstractVerticle {

    private final Logger logger = LoggerFactory.getLogger(DeployerVerticle.class);

    @Override
    public void start() {
        long delay = 1000;
        for (int i=0; i < 50; i++) {
            vertx.setTimer(delay, id -> deploy());
            delay = delay + 1000;
        }
    }

    private void deploy() {
        vertx.deployVerticle(new EmptyVerticle(), ar -> {
            if (ar.succeeded()) {
                String id = ar.result();
                logger.info("Successfully deployed {}", id);
                vertx.setTimer(5000, tid -> undeployLater(id));
            }
        });
    }

    public void undeployLater(String id) {
        vertx.undeploy(id, ar -> {
            if (ar.succeeded() ){
                logger.info("{} got undeployed", id);
            } else {
                logger.info("{} unable to be undeployed", id);
            }
        });
    }

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new DeployerVerticle());
    }
}
