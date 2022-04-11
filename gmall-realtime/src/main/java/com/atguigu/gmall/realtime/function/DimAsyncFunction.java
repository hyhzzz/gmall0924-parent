package com.atguigu.gmall.realtime.function;

import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.JdbcUtil;
import com.atguigu.gmall.realtime.util.RedisUtil;
import com.atguigu.gmall.realtime.util.ThreadPoolUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ThreadPoolExecutor;

import redis.clients.jedis.Jedis;

/**
 * @author coderhyh
 * @create 2022-04-11 0:25
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {


    public abstract void addDim(Jedis redisClient,
                                Connection conn,
                                T input,
                                ResultFuture<T> resultFuture) throws Exception;


    private ThreadPoolExecutor ThreadPool;

    @Override
    public void open(Configuration parameters) throws Exception {
        ThreadPool = ThreadPoolUtil.getThreadPool();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        //每来一条数据，就使用线程池 启动一个线程进行读取维度信息
        ThreadPool.submit(new Runnable() {
            @Override
            public void run() {
                Connection conn = null;
                Jedis redisClient = null;
                //读取维度的信息

                try {
                    conn = JdbcUtil.getJdbcConnect(Constant.PHOENIX_DRIVER,
                            Constant.PHOENIX_URL, null, null
                    );
                    redisClient = RedisUtil.getRedisClient();

                    //处理维度
                    // 将来不同流, T类型不同, 需要的维度表也不一样
                    addDim(redisClient, conn, input, resultFuture);

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    // 关闭连接对象
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if (redisClient != null) {
                        redisClient.close();
                    }
                }
            }
        });
    }
}
