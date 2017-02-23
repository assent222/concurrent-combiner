package pkk.interview;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.SynchronousQueue;

/**
 * Created by root on 21.02.2017.
 */
public class Consumer<T> implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private final SynchronousQueue<T> outputQueue;

    public Consumer() {
        outputQueue = new SynchronousQueue<T>();
    }

    public Consumer(SynchronousQueue<T> outputQueue) {
        this.outputQueue = outputQueue;
    }

    public SynchronousQueue<T> getOutputQueue() {
        return outputQueue;
    }

    public void run() {
        logger.info(">> run");
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(300);
                logger.info(outputQueue.take().toString());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("<< run");
    }
}
