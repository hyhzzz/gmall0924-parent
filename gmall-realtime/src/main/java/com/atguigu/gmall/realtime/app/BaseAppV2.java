package com.atguigu.gmall.realtime.app;

import com.atguigu.gmall.realtime.util.FlinkSourceUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * @author coderhyh
 * @create 2022-04-06 23:50
 * 每次消费kafka数据总会有很多的模板代码, BaseApp把一些模板进行封装, 该类的子类只需要实现相应的业务逻辑即可
 * 可以消费多个topic v1只能消费一个
 */
abstract public class BaseAppV2 {

    /**
     * 做初始化相关工作
     *
     * @param port          端口号
     * @param parallelism   默认并行度
     * @param checkpointing ck路径
     * @param groupId       消费者组
     * @param otherTopics   可以传多个topic
     * @param firstTopic    至少传一个topic
     */
    public void init(int port,
                     int parallelism,
                     String checkpointing,
                     String groupId,
                     String firstTopic, String... otherTopics) {

        //获取流的执行环境
        //自定义web ui
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);

        //开启 checkpoint
        env.enableCheckpointing(3000);

        //设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck/gmall" + checkpointing);

        //同时允许多少个checkpoint在工作
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //两个checkpoint之间最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        //设置严格一次语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //设置checkpoint超时时间
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);

        //设置当程序取消了 checkpoint数据/目录保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //存储了所有topic
        ArrayList<String> topics = new ArrayList<>();
        topics.add(firstTopic);
        topics.addAll(Arrays.asList(otherTopics));

        //把流放入map中,存储topic和对应的流
        HashMap<String, DataStreamSource<String>> topicStreamMap = new HashMap<>();

        for (String topic : topics) {
            DataStreamSource<String> stream =
                    env.addSource(FlinkSourceUtil.getKafkaSource(groupId, topic));
            topicStreamMap.put(topic, stream);
        }


        /**
         * 子类在此抽象方法中完成自己的业务逻辑
         * @param env    执行环境
         * @param stream  从Kafka直接获取得到的流
         */
        run(env, topicStreamMap);


        //启动执行环境
        try {
            env.execute(checkpointing);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected abstract void run(StreamExecutionEnvironment
                                        env, HashMap<String, DataStreamSource<String>> topicStreamMap);

}
