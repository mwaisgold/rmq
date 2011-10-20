package redis.rmq.benchmarks;

import java.io.IOException;
import java.util.Calendar;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.rmq.Consumer;
import redis.rmq.Producer;
import redis.rmq.simpleInstance.SimpleInstanceProducer;

public class PublishConsumeBenchmarkTest extends Assert {

    static Integer cantMessages = 10000;

    @Before
    public void setUp() throws IOException {
        Jedis jedis = new Jedis("localhost");
        //jedis.flushAll();
        jedis.disconnect();

    }

    @Test
    public void publish() {
        final String topic = "foo";
        final String message = "hello world!";
        final int MESSAGES = cantMessages;
        Producer p = new Producer(new Jedis("localhost"), topic);

        long start = Calendar.getInstance().getTimeInMillis();
        long acum = 0l;
        for (int n = 0; n < MESSAGES; n++) {
            long startOp = Calendar.getInstance().getTimeInMillis();
            p.publish(message);
            long elapsedOp = Calendar.getInstance().getTimeInMillis() - startOp;
            acum+=elapsedOp;
        }
        long elapsed = Calendar.getInstance().getTimeInMillis() - start;
        System.out.println("Regular publish: " + ((1000 * MESSAGES) / elapsed) + " ops");
        System.out.println("Regular average: " + (acum * 1.0/MESSAGES));
        System.out.println("Regular average: " + (MESSAGES * 1.0/elapsed));
        System.out.println("Regular elapsed: " + (elapsed));
    }

    @Test
    public void publishSimpleProducer() {
        final String topic = "foo";
        final String message = "hello world!";
        final int MESSAGES = cantMessages;
        //Producer p = new Producer(new Jedis("localhost"), topic);
        SimpleInstanceProducer.configureInstance(new Jedis("localhost"), topic);
        Producer p = SimpleInstanceProducer.getInstance();

        long start = Calendar.getInstance().getTimeInMillis();
        long acum = 0l;
        for (int n = 0; n < MESSAGES; n++) {
            long startOp = Calendar.getInstance().getTimeInMillis();
            p.publish(message);
            long elapsedOp = Calendar.getInstance().getTimeInMillis() - startOp;
            acum+=elapsedOp;
        }

        long elapsed = Calendar.getInstance().getTimeInMillis() - start;
        System.out.println("Simple publish: " +((1000 * MESSAGES) / elapsed) + " ops");
        System.out.println("Simple average: " + (acum * 1.0/MESSAGES));
        System.out.println("Simple average: " + (MESSAGES * 1.0/elapsed));
        System.out.println("Simple elapsed: " + (elapsed));
    }

    @Test
    @Ignore
    public void consume() {
        final String topic = "foo";
        final String message = "hello world!";
        final int MESSAGES = cantMessages;
        Producer p = new Producer(new Jedis("localhost"), topic);
        Consumer c = new Consumer(new Jedis("localhost"), "consumer", topic);
        for (int n = 0; n < MESSAGES; n++) {
            p.publish(message);
        }

        long start = Calendar.getInstance().getTimeInMillis();
        String m = null;
        do {
            m = c.consume();
        } while (m != null);
        long elapsed = Calendar.getInstance().getTimeInMillis() - start;

        System.out.println("Consume publish: " +((1000 * MESSAGES * 3) / elapsed) + " ops");
    }
}