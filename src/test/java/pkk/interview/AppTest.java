package pkk.interview;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for simple App.
 */
public class AppTest {
    private static final Logger logger = LoggerFactory.getLogger(AppTest.class);

    @org.junit.Test
    public void FinalizeQueueTaskTest() throws Exception {
        logger.info(">> FinalizeQueueTaskTest");

        Producer<String> producer1 = new Producer<String>(new ArrayBlockingQueue<String>(100), 5, 10_000, TimeUnit.MILLISECONDS);
        Consumer<String> consumer = new Consumer<String>();
        CombinerImpl<String> combiner = new CombinerImpl<String>(consumer.getOutputQueue());
        combiner.addInputQueue(producer1.getQueue(), producer1.getPriority(), producer1.getIsEmptyTimeout(), producer1.getTimeUnit());

        ExecutorService executorService = Executors.newFixedThreadPool(4);
        executorService.execute(new MessageGeneratorTask(producer1.getQueue(), 10));
        executorService.execute(consumer);
        executorService.execute(combiner);

        Thread.sleep(5_000);
        executorService.execute(new MessageGeneratorTask(producer1.getQueue(), 10));

        Thread.sleep(20_000);

        Assert.assertFalse(combiner.hasInputQueue(producer1.getQueue()));

        executorService.shutdownNow();

        logger.info("<< FinalizeQueueTaskTest");

    }

    @org.junit.Test
    public void LoadBalancerTest() throws Exception {
        logger.info(">> LoadBalancerTest");

        Producer<String> producer1 = new Producer<String>(9.5);
        Producer<String> producer2 = new Producer<String>(0.5);
        Consumer<String> consumer = new Consumer<String>();
        CombinerImpl<String> combiner = new CombinerImpl<String>(consumer.getOutputQueue());
        combiner.addInputQueue(producer1.getQueue(), producer1.getPriority(), producer1.getIsEmptyTimeout(), producer1.getTimeUnit());
        combiner.addInputQueue(producer2.getQueue(), producer2.getPriority(), producer2.getIsEmptyTimeout(), producer2.getTimeUnit());

        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(new MessageGeneratorTask(producer1.getQueue(), 1000));
        executorService.execute(new MessageGeneratorTask(producer2.getQueue(), 1000));
        executorService.execute(consumer);
        executorService.execute(combiner);

        Thread.sleep(300 * 200);

        Assert.assertFalse(combiner.hasInputQueue(producer1.getQueue()));

        executorService.shutdownNow();

        logger.info("<< LoadBalancerTest");
    }

    @org.junit.Test
    public void WorkTest() throws Exception {
        logger.info(">> WorkTest");

        Producer<String> producer1 = new Producer<String>(new ArrayBlockingQueue<String>(100), 5,1000, TimeUnit.MILLISECONDS);
        Producer<String> producer2 = new Producer<String>(0.5);
        Producer<String> producer3 = new Producer<String>(7.5);
        Producer<String> producer4 = new Producer<String>(9.5);
        Consumer<String> consumer = new Consumer<String>();
        CombinerImpl<String> combiner = new CombinerImpl<String>(consumer.getOutputQueue());
        combiner.addInputQueue(producer1.getQueue(), producer1.getPriority(), producer1.getIsEmptyTimeout(), producer1.getTimeUnit());
        combiner.addInputQueue(producer2.getQueue(), producer2.getPriority(), producer2.getIsEmptyTimeout(), producer2.getTimeUnit());

        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(new MessageGeneratorTask(producer1.getQueue(),10));
        executorService.execute(new MessageGeneratorTask(producer2.getQueue()));
        executorService.execute(consumer);
        executorService.execute(combiner);

        Thread.sleep(5_000);

        executorService.execute(new MessageGeneratorTask(producer3.getQueue()));
        executorService.execute(new MessageGeneratorTask(producer4.getQueue()));
        combiner.addInputQueue(producer3.getQueue(), producer3.getPriority(), producer3.getIsEmptyTimeout(), producer3.getTimeUnit());
        combiner.addInputQueue(producer4.getQueue(), producer4.getPriority(), producer4.getIsEmptyTimeout(), producer4.getTimeUnit());
//        combiner.removeInputQueue(producer1.getQueue());
        combiner.removeInputQueue(producer2.getQueue());

        Thread.sleep(60_000);

        executorService.shutdownNow();

        logger.info("<< WorkTest");
    }
}
