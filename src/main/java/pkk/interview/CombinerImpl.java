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
    private final DelayQueue<DelayNode<Producer>> finalizeQueue = new DelayQueue<>();
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
        Thread finalizeQueueThread = new Thread(new FinalizeQueueTask());
        try {
            finalizeQueueThread.start();
            while (!Thread.currentThread().isInterrupted()) {
                for (Map.Entry<BlockingQueue, Producer> entry : producers.entrySet()) {
                    Producer<T> producer = entry.getValue();
                    for (int i = 0; i < balancer.getRate(producer.getQueue()); i++) {
                        if (producer.getQueue().isEmpty()) {
                            finalizeQueue.put(new DelayNode<Producer>(producer, producer.getIsEmptyTimeout(), producer.getTimeUnit()));
                            removeInputQueue(producer.getQueue());
                            break;
                        }
                        T msg = producer.getQueue().peek();
                        outputQueue.put(msg);
                        producer.getQueue().remove();
                    }
                    logger.info(producer.getPriority() + " completed");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            finalizeQueueThread.interrupt();
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

    /*
    * If query delayed then remowe it form reading loop and put it into DelayQueue
    * If putted queue still empty after isEmptyTimeout it will be removet
    * If putted queue is not empty after isEmptyTimeout it will be added as new queue throw addInputQueue
    * */
    private class FinalizeQueueTask implements Runnable {
        @Override
        public void run() {
            logger.info(">> run FinalizeQueueTask");
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    DelayNode<Producer> node = finalizeQueue.take();
                    Producer producer = node.getValue();

                    if (!producer.getQueue().isEmpty()) {
                        if (producers.putIfAbsent(producer.getQueue(), producer) == null) {
                            balancer = new QueueBalancer();
                        }
                    } else {
                        logger.info("finalize queue");
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("<< run FinalizeQueueTask");
        }
    }


    private static class DelayNode<T> implements Delayed {
        private final long delay;
        private final T value;

        public DelayNode(T value, long delay, TimeUnit unit) {
            this.delay = unit.convert(delay, TimeUnit.MILLISECONDS) + System.currentTimeMillis();
            this.value = value;
        }

        public T getValue() {
            return value;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(delay - System.currentTimeMillis(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            if (this == o) return 0;
            return Long.compare(this.getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
        }
    }
}
