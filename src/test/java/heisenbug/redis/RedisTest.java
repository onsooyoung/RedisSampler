package heisenbug.redis;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by PC000411 on 2019-01-31.
 */
public class RedisTest {

    public JedisPool __pool = null;

    public Jedis jedis = null;

    @Before
    public void initRedis() {


        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(200);
        config.setMaxIdle(100);
        config.setMinIdle(50);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setTestWhileIdle(true);
        config.setMaxWaitMillis(2000);//write timeout
        config.setBlockWhenExhausted(true);

        String redisIp = "127.0.0.1";
        int redisPort = 6379;

        __pool = new JedisPool(config, redisIp, redisPort
                ,6000);//connection, read timeout

        try {
            jedis = __pool.getResource();
            jedis.getClient().getSocket().setTcpNoDelay(true);
        } catch (Exception e) {
            __pool.returnBrokenResource(jedis);
        } finally {
            __pool.returnResource(jedis);
        }
    }

    @Test
    public void connTest(){

        System.out.printf("connTest : %s\n", __pool.toString());
        Assert.assertTrue(__pool.getResource() !=null);

    }

    @Test
    public void resourceTest(){

        System.out.printf("resourceTest : %s\n", __pool.getResource().toString());
        Assert.assertTrue(__pool.getResource().toString() !=null);

    }
}
