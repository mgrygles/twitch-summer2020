import io.vertx.rxjava.core.Future;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Single;
import rx.schedulers.Schedulers;

public class ResourceAPI {

    private ConcurrentHashMap<Integer, Resource>  resources;
    private Random     rand;
    private final Logger   logger;
    private Integer        max_delay_ms;
    private final Scheduler      scheduler;
    private AtomicLong        async_requests = new AtomicLong(0);

    public ResourceAPI(final ExecutorService executor, Integer max_delay_ms) {
        logger = LoggerFactory.getLogger("ResourceAPI");
        max_delay_ms = max_delay_ms;
        scheduler = Schedulers.from(executor);
    }

    public Future<Resource> getResource(int id) {
        Future<Resource> future = Future.future();
        int random_interval = rand.nextInt(max_delay_ms);
        logger.info("Request for {} will execute in {} milliseconds max {} milliseconds", id, random_interval, max_delay_ms);

        Observable.timer(random_interval, TimeUnit.MILLISECONDS)
                .observeOn(scheduler)
                .subscribe(delay-> {
                    logger.info("Returning resource for {}", id);
                    Resource r = resources.get(id);
                    future.complete(r);
                },
                        error -> {},
                        () ->{} );
        return future;
    }

    public void build() {
        resources = new ConcurrentHashMap<Integer, Resource>();
        for (int i=1; i<=100; i++){
            resources.put(i, new Resource(i));
        }
        rand = new Random();
        rand.setSeed(System.currentTimeMillis());
    }

    public Observable<Resource> fetchResource(int id) {
        return Observable.create(subscriber -> {
            async_requests.incrementAndGet();
            int random_interval = rand.nextInt(max_delay_ms);
            logger.info("Request for {} will execute in {} milliseconds max {} milliseconds", id, random_interval, max_delay_ms);
            Observable.timer(random_interval, TimeUnit.MILLISECONDS)
                    .observeOn(scheduler)
                    .subscribe(delay -> {
                        async_requests.decrementAndGet();
                        logger.info("Returning resource for {}", id);
                        Resource r = resources.get(id);
                        subscriber.onNext(r);
                        subscriber.onCompleted();
                        },
                            error -> {
                                subscriber.onError(error);
                            },
                            () -> {}
                    );
         });
    }

    public long getAsyncRequests() {
        return async_requests.get();
    }
}
