package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV2;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.DimUtil;
import com.atguigu.gmall.realtime.util.JdbcUtil;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.time.Duration;
import java.util.HashMap;

/**
 * @author coderhyh
 * @create 2022-04-09 18:20
 * 处理订单宽表
 */
public class DwmOrderWideApp extends BaseAppV2 {

    public static void main(String[] args) {
        //环境初始化
        new DwmOrderWideApp().init(3003, 4, "DwmOrderWideApp", "DwmOrderWideApp", Constant.TOPIC_DWD_ORDER_INFO, Constant.TOPIC_DWD_ORDER_DETAIL);
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
        factJoinDim(orderWideWithoutDimStream);
        //2.1 旁路缓存优化 、异步io优化

        //3.把数据写入到kafka中

    }

    /**
     * join 维度数据
     *
     * @param orderWideWithoutDimStream 没有维度信息的流
     * @return SingleOutputStreamOperator<OrderWide> join维度表之后的流
     */
    private void factJoinDim(SingleOutputStreamOperator<OrderWide> orderWideWithoutDimStream) {
        /*
        join维度的思路:
            拿到每个order_wide 根据相应的维度表的id去phoenix中查找对应的维度信息

            涉及到了6张维度表 user_info base_province sku_info spu_info  base_trademark base_category3

            使用jdbc的方式
         */
        orderWideWithoutDimStream.map(
                new RichMapFunction<OrderWide, OrderWide>() {
                    private Connection phoenixConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        phoenixConn = JdbcUtil.getJdbcConnect(Constant.PHOENIX_DRIVER, Constant.PHOENIX_URL, null, null);
                    }

                    @Override
                    public void close() throws Exception {
                        if (phoenixConn == null && !phoenixConn.isClosed()) {
                            phoenixConn.close();
                        }
                    }

                    @Override
                    public OrderWide map(OrderWide orderWide) throws Exception {
                        // 读取维度数据, 6张表
                        // 1.补齐user_info
                        JSONObject userInfo = DimUtil.readDimFromPhoenix(phoenixConn, "dim_user_info", orderWide.getUser_id());
                        orderWide.setUser_gender(userInfo.getString("GENDER"));
                        orderWide.calcuUserAge(userInfo.getString("BIRTHDAY"));

                        // 2. 补充省份
                        JSONObject baseProvince = DimUtil.readDimFromPhoenix(phoenixConn, "dim_base_province", orderWide.getProvince_id());
                        orderWide.setProvince_name(baseProvince.getString("NAME"));
                        orderWide.setProvince_3166_2_code(baseProvince.getString("ISO_3166_2"));
                        orderWide.setProvince_area_code(baseProvince.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(baseProvince.getString("ISO_CODE"));

                        // 3. sku_info
                        JSONObject skuInfo = DimUtil.readDimFromPhoenix(phoenixConn, "dim_sku_info", orderWide.getSku_id());
                        orderWide.setSku_name(skuInfo.getString("SKU_NAME"));
                        orderWide.setSku_price(skuInfo.getBigDecimal("PRICE"));

                        orderWide.setSpu_id(skuInfo.getLong("SPU_ID"));
                        orderWide.setTm_id(skuInfo.getLong("TM_ID"));
                        orderWide.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));

                        // 4. spu_info
                        JSONObject spuInfo = DimUtil.readDimFromPhoenix(phoenixConn, "dim_spu_info", orderWide.getSpu_id());
                        orderWide.setSpu_name(spuInfo.getString("SPU_NAME"));
                        // 5. base_trademark
                        JSONObject baseTrademark = DimUtil.readDimFromPhoenix(phoenixConn, "dim_base_trademark", orderWide.getTm_id());
                        orderWide.setTm_name(baseTrademark.getString("TM_NAME"));


                        // 6. c3
                        JSONObject c3 = DimUtil.readDimFromPhoenix(phoenixConn, "dim_base_category3", orderWide.getCategory3_id());
                        orderWide.setCategory3_name(c3.getString("NAME"));

                        return orderWide;
                    }
                }
        ).print();

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
