package redis.rmq.simpleInstance;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.rmq.Producer;

import java.util.List;

/**
 * User: mwaisgold
 * Date: 19/10/11
 * Time: 14:47
 */
public class SimpleInstanceProducer extends Producer {

    //Treat as singleton because most of it's work is in memory
    private static SimpleInstanceProducer uniqueInstance = null;

    public static final void configureInstance(final Jedis jedis, String topic){
        if (uniqueInstance != null){
            throw new RuntimeException("Instance is already created");
        }

        uniqueInstance = new SimpleInstanceProducer(jedis, topic);
    }

    public static final SimpleInstanceProducer getInstance(){
        return uniqueInstance;
    }

    Integer nextMessageId = null;

    private SimpleInstanceProducer(final Jedis jedis, final String topic) {
        super(jedis, topic);
        //Get last given message from redis
        this.nextMessageId = super.getNextMessageId();
    }

    @Override
    public void publish(String message, int seconds) {
        synchronized (nextMessageId){
            List<Object> exec = null;
            Integer nextMessageId = getNextMessageId();
            String msgKey = getTopic().cat("message").cat(nextMessageId).key();

            while (getJedis().setnx(msgKey, nextMessageId.toString()) == 0){
                nextMessageId = getNextMessageId();
                msgKey = getTopic().cat("message").cat(nextMessageId).key();
            }

            if (seconds > 0)
                getJedis().expire(msgKey, seconds);

            getJedis().incr(getTopic().key());

        }
    }

    protected Integer getNextMessageId() {
        Integer ret = this.nextMessageId;
        this.nextMessageId++;
        return ret;
    }
}
