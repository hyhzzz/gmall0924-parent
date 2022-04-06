package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.Constant;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author coderhyh
 * @create 2022-04-06 23:49
 * kafka工具类
 */
public class FlinkSourceUtil {

    public static SourceFunction<String> getKafkaSource(String groupId, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constant.KAFKA_BROKERS);
        props.put("group.id", groupId);
        props.put("auto.offset.reset", "latest");
        props.put("isolation.level", "read_committed");

        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
    }
}
