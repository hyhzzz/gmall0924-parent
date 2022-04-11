package com.atguigu.gmall.realtime.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author coderhyh
 * @create 2022-04-11 0:08
 */
public class ThreadPoolUtil {

    public static ThreadPoolExecutor getThreadPool() {
        return new ThreadPoolExecutor(
                300,//线程池的核心线程数
                500,//线程池的最大线程数
                30,//表示500-300 这200个最多空闲的世界
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(100) //阻塞队列中最多存储的线程数
        );
    }
}
