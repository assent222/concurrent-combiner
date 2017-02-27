package pkk.interview;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Created by root on 21.02.2017.
 */
public class CombinerImpl<T> extends Combiner<T> implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(CombinerImpl.class);

    private final Map<BlockingQueue, Producer> producers = new ConcurrentHashMap<>();
    private volatile QueueBalancer balancer = new QueueBalancer();

    protected CombinerImpl(SynchronousQueue<T> outputQueue) {
        super(outputQueue);
    }

    public void addInputQueue(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit) throws CombinerException {
        logger.info("add queue");
        try {
            if (producers.putIfAbsent(queue, new Producer(queue, priority, isEmptyTimeout, timeUnit)) == null) {
                balancer = new QueueBalancer();
            }
        } catch (Exception e) {
            throw new CombinerException("add queue issue", e);
        }
    }

    public void removeInputQueue(BlockingQueue<T> queue) throws CombinerException {
        logger.info(":remove queue");
        try {
            if (producers.remove(queue) != null) {
                balancer = new QueueBalancer();
            }
        } catch (Exception e) {
            throw new CombinerException("remove queue issue", e);
        }
    }

    public boolean hasInputQueue(BlockingQueue<T> queue) {
        return producers.containsKey(queue);
    }

    public void run() {
        logger.info(">> run");
        try {
            while (!Thread.currentThread().isInterrupted()) {
                for (Producer<T> producer : producers.values()) {
                    for (int i = 0; i < balancer.getRate(producer.getQueue()); i++) {
                        T msg = producer.getQueue().poll(producer.getIsEmptyTimeout(), producer.getTimeUnit());
                        if (msg == null) {
                            removeInputQueue(producer.getQueue());
                            break;
                        } else {
                            outputQueue.put(msg);
                        }
                    }
                    logger.info(producer.getPriority() + " completed");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("<< run");
    }

    /*
    * Sum of all priority - 100%
    * Calc percents and cast it to int
    * As result got Map<BlockingQueue, Integer> with distribution rates per queues for every 100 read messages
    * */
    private class QueueBalancer {
        private final Map<BlockingQueue, Integer> rates;

        public QueueBalancer() {
            logger.info(":create QueueBalancer");
            double sum = producers.values().parallelStream().mapToDouble(p -> p.getPriority()).sum();
            rates = producers.values().parallelStream().collect(Collectors.toMap(p -> p.getQueue(), p -> Double.valueOf(p.getPriority() / sum * 100).intValue()));
        }

        public int getRate(BlockingQueue key) {
            Integer res = rates.get(key);
            return res == null ? 0 : res;
        }
    }
}
