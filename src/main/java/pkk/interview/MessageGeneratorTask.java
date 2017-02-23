package pkk.interview;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

/**
 * Created by root on 21.02.2017.
 */
class MessageGeneratorTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MessageGeneratorTask.class);

    private final BlockingQueue queue;
    private final int cntMessage;

    public MessageGeneratorTask(BlockingQueue queue) {
        this.queue = queue;
        this.cntMessage = 100;
    }

    public MessageGeneratorTask(BlockingQueue queue, int cntMessage) {
        this.queue = queue;
        this.cntMessage = cntMessage;
    }

    public void run() {
        logger.info(">> run");
        try {
            for (int i = 0; i < cntMessage && !Thread.currentThread().isInterrupted(); i++) {
                queue.put(Thread.currentThread().getName() + ":msg-" + i);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("<< run");
    }
}
