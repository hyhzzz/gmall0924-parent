package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.sink.PhoenixSink;

import org.apache.flink.api.java.tuple.Tuple2;
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

    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getKafkaSink() {

        Properties props = new Properties();
        props.put("bootstrap.servers", Constant.KAFKA_BROKERS);

        //设置事务超时属性
        props.put("transaction.timeout.ms", 15 * 60 * 1000);
        return new FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>>(
                "default",
                new KafkaSerializationSchema<Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcess> element, @Nullable Long aLong) {

                        String topic = element.f1.getSinkTable();
                        byte[] data = element.f0.toJSONString().getBytes(StandardCharsets.UTF_8);
                        return new ProducerRecord<>(topic, data);
                    }
                }, props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {
        return new PhoenixSink();
    }
}
