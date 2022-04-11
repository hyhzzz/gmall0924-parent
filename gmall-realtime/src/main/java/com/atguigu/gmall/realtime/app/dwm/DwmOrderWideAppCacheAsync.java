package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV2;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.util.DimUtil;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.Jedis;

/**
 * @author coderhyh
 * @create 2022-04-09 18:20
 * 处理订单宽表
 */
public class DwmOrderWideAppCacheAsync extends BaseAppV2 {

    public static void main(String[] args) {
        //环境初始化
        new DwmOrderWideAppCacheAsync().init(
                3003,
                4,
                "DwmOrderWideAppCacheAsync",
                "DwmOrderWideAppCacheAsync",
                Constant.TOPIC_DWD_ORDER_INFO, Constant.TOPIC_DWD_ORDER_DETAIL);
    }

    //写具体的业务逻辑
    @Override
    protected void run(StreamExecutionEnvironment env, HashMap<String, DataStreamSource<String>> topicStreamMap) {

        //        topicStreamMap.get(Constant.TOPIC_DWD_ORDER_INFO).print("orderinfo");
        //        topicStreamMap.get(Constant.TOPIC_DWD_ORDER_DETAIL).print("orderdetail");


        //1.事实表join
        SingleOutputStreamOperator<OrderWide> orderWideWithoutDimStream = factsJoin(topicStreamMap);
        //        orderWideWithoutDimStream.print();

        //2.事实表和维度表join
        SingleOutputStreamOperator<OrderWide> orderWideWithDimStream = factJoinDim(orderWideWithoutDimStream);

        //2.1 旁路缓存优化 、异步io优化

        //3.把数据写入到kafka中
        writeToKafka(orderWideWithDimStream);

    }

    private void writeToKafka(SingleOutputStreamOperator<OrderWide> stream) {
        stream
                .map(JSON::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWM_ORDER_WIDE));
    }

    /**
     * 异步io
     *
     * @return
     */
    private SingleOutputStreamOperator<OrderWide> factJoinDim(SingleOutputStreamOperator<OrderWide> orderWideWithoutDimStream) {

        return AsyncDataStream.unorderedWait(
                orderWideWithoutDimStream,
                new DimAsyncFunction<OrderWide>() {
                    @Override
                    public void addDim(Jedis redisClient,
                                       Connection phoenixConn,
                                       OrderWide orderWide,
                                       ResultFuture<OrderWide> resultFuture) throws Exception {
                        // 补齐维度
                        // 读取维度数据, 6张表
                        // 1.补齐user_info
                        JSONObject userInfo = DimUtil.readDim(redisClient, phoenixConn, "dim_user_info", orderWide.getUser_id());
                        orderWide.setUser_gender(userInfo.getString("GENDER"));
                        orderWide.calcuUserAge(userInfo.getString("BIRTHDAY"));

                        // 2. 补充省份
                        JSONObject baseProvince = DimUtil.readDim(redisClient, phoenixConn, "dim_base_province", orderWide.getProvince_id());
                        orderWide.setProvince_name(baseProvince.getString("NAME"));
                        orderWide.setProvince_3166_2_code(baseProvince.getString("ISO_3166_2"));
                        orderWide.setProvince_area_code(baseProvince.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(baseProvince.getString("ISO_CODE"));

                        // 3. sku_info
                        JSONObject skuInfo = DimUtil.readDim(redisClient, phoenixConn, "dim_sku_info", orderWide.getSku_id());
                        orderWide.setSku_name(skuInfo.getString("SKU_NAME"));
                        orderWide.setSku_price(skuInfo.getBigDecimal("PRICE"));

                        orderWide.setSpu_id(skuInfo.getLong("SPU_ID"));
                        orderWide.setTm_id(skuInfo.getLong("TM_ID"));
                        orderWide.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));

                        // 4. spu_info
                        JSONObject spuInfo = DimUtil.readDim(redisClient, phoenixConn, "dim_spu_info", orderWide.getSpu_id());
                        orderWide.setSpu_name(spuInfo.getString("SPU_NAME"));
                        // 5. base_trademark
                        JSONObject baseTrademark = DimUtil.readDim(redisClient, phoenixConn, "dim_base_trademark", orderWide.getTm_id());
                        orderWide.setTm_name(baseTrademark.getString("TM_NAME"));


                        // 6. c3
                        JSONObject c3 = DimUtil.readDim(redisClient, phoenixConn, "dim_base_category3", orderWide.getCategory3_id());
                        orderWide.setCategory3_name(c3.getString("NAME"));

                        resultFuture.complete(Collections.singletonList(orderWide));
                    }
                },
                30,
                TimeUnit.SECONDS
        );

    }

    /**
     * 订单表和订单明细表join
     *
     * @param topicStreamMap 消费的多个流
     * @return join后的表
     */
    private SingleOutputStreamOperator<OrderWide> factsJoin(HashMap<String, DataStreamSource<String>> topicStreamMap) {

        /*
        双流join 有两种种：
        1.窗口join
        2.interval join
               keyby之后
               只支持事件时间

         */
        // 1. 解析数据, 并添加水印
        KeyedStream<OrderInfo, Long> orderInfoStream = topicStreamMap.get(Constant.TOPIC_DWD_ORDER_INFO)
                .map(info -> JSON.parseObject(info, OrderInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((info, ts) -> info.getCreate_ts())
                )
                .keyBy(OrderInfo::getId);

        KeyedStream<OrderDetail, Long> orderDetailStream = topicStreamMap.get(Constant.TOPIC_DWD_ORDER_DETAIL)
                .map(info -> JSON.parseObject(info, OrderDetail.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((detail, ts) -> detail.getCreate_ts())
                )
                .keyBy(OrderDetail::getOrder_id);

        // 2. 订单和订单明细表使用 intervalJoin 进行join
        return orderInfoStream.intervalJoin(orderDetailStream)
                .between(Time.seconds(-10), Time.seconds(10))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo,
                                               OrderDetail orderDetail,
                                               ProcessJoinFunction<OrderInfo, OrderDetail,
                                                       OrderWide>.Context ctx,
                                               Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

    }
}
/*
异步超时:
    1. 检查所有集群是否全部开启
        redis hadoop phoenix(hbase)
    2. 检查phoenix中6张维度表是否都在  ...
    3. 重点检查dim_user_info是否有4000条数据
    4. 检查redis是否允许远程连接
        bind 0.0.0.0
    5. 检测下redis中字段大小写问题

 打包到linux执行会报异步超时:
 1. 打包的时候不要把phoenix打包到
 2. 在flink的classpath目录$flink_home/lib 添加phoenix的依赖
        phoenix-5.0.0-HBase-2.0-client.jar
 */