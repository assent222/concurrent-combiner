package pkk.interview;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Created by root on 21.02.2017.
 */
public class Producer<T>  {

    private final BlockingQueue<T> queue;
    private final double priority;
    private final long isEmptyTimeout;
    private final TimeUnit timeUnit;

    public Producer() {
        this(new ArrayBlockingQueue<T>(100, false), 100, 1, TimeUnit.MINUTES);
    }

    public Producer(double priority) {
        this(new ArrayBlockingQueue<T>(100, false), priority, 1, TimeUnit.MINUTES);
    }

    public Producer(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit) {
        this.queue = queue;
        this.priority = priority;
        this.isEmptyTimeout = isEmptyTimeout;
        this.timeUnit = timeUnit;
    }

    BlockingQueue<T> getQueue() {
        return queue;
    }

    public double getPriority() {
        return priority;
    }

    public long getIsEmptyTimeout() {
        return isEmptyTimeout;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        return queue == ((Producer) o).queue;
    }

    @Override
    public int hashCode() {
        return queue.hashCode();
    }

}
