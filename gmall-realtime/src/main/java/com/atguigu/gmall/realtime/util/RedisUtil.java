package com.atguigu.gmall.realtime.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author coderhyh
 * @create 2022-04-10 12:55
 */
public class RedisUtil {

    private static JedisPool pool;

    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);
        config.setMaxIdle(10);
        config.setMaxWaitMillis(30 * 1000);
        config.setMinIdle(2);
        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        pool = new JedisPool(
                config, "hadoop102", 6379
        );
    }

    public static Jedis getRedisClient() {

        Jedis jedis = pool.getResource();
        jedis.select(1);//以后所有的数据都存储到1号库

        return jedis;
    }
}
