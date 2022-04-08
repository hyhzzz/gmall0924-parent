package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.Constant;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import javax.annotation.Nullable;

/**
 * @author coderhyh
 * @create 2022-04-07 12:30
 * kafka sink工具类
 */
public class FlinkSinkUtil {

    public static SinkFunction getKafkaSink(String topic) {

        Properties props = new Properties();
        props.put("bootstrap.servers", Constant.KAFKA_BROKERS);

        //设置事务超时属性
        props.put("transaction.timeout.ms", 15 * 60 * 1000);

        return new FlinkKafkaProducer<String>("default", new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element,
                                                            @Nullable Long aLong) {
                return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
            }
        }, props,
                //开启严格一次 要把生产者事务的超时时间不能大于服务器的要求
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
