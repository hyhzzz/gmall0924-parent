package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.app.BaseAppV2;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentInfo;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;

/**
 * @author coderhyh
 * @create 2022-04-11 0:55
 */
public class DwmPaymentWideApp extends BaseAppV2 {

    public static void main(String[] args) {

        new DwmPaymentWideApp().init(
                3004,
                4,
                "DwmPaymentWideApp",
                "DwmPaymentWideApp",
                Constant.TOPIC_DWM_ORDER_WIDE, Constant.TOPIC_DWD_PAYMENT_INFO);
    }

    //写具体的业务逻辑
    @Override
    protected void run(StreamExecutionEnvironment env,
                       HashMap<String, DataStreamSource<String>> topicStreamMap) {

        //        topicStreamMap.get(Constant.TOPIC_DWM_ORDER_WIDE).print("orderwide");
        //        topicStreamMap.get(Constant.TOPIC_DWD_PAYMENT_INFO).print("pay");

        KeyedStream<PaymentInfo, Long> paymentInfoStream = topicStreamMap.get(Constant.TOPIC_DWD_PAYMENT_INFO)
                .map(json -> JSON.parseObject(json, PaymentInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((pay, ts) -> pay.getTs())
                )
                .keyBy(PaymentInfo::getOrder_id);


        KeyedStream<OrderWide, Long> orderWideStream = topicStreamMap.get(Constant.TOPIC_DWM_ORDER_WIDE)
                .map(json -> JSON.parseObject(json, OrderWide.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((pay, ts) -> pay.getTs())
                )
                .keyBy(OrderWide::getOrder_id);


        paymentInfoStream.intervalJoin(orderWideStream)
                .between(Time.minutes(-45), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo,
                                               OrderWide orderWide,
                                               ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context ctx,
                                               Collector<PaymentWide> out) throws Exception {

                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                })
                .map(
                        JSON::toJSONString
                )
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWM_PAYMENT_WIDE));

    }
}
