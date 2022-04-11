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
import com.atguigu.gmall.realtime.util.RedisUtil;

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

import redis.clients.jedis.Jedis;

/**
 * @author coderhyh
 * @create 2022-04-09 18:20
 * 处理订单宽表
 */
public class DwmOrderWideAppCache extends BaseAppV2 {

    public static void main(String[] args) {
        //环境初始化
        new DwmOrderWideAppCache().init(3003,
                4,
                "DwmOrderWideAppCache",
                "DwmOrderWideAppCache",
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
                    private Jedis redisClient;
                    private Connection phoenixConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        phoenixConn = JdbcUtil.getJdbcConnect(Constant.PHOENIX_DRIVER, Constant.PHOENIX_URL, null, null);

                        redisClient = RedisUtil.getRedisClient();
                    }

                    @Override
                    public void close() throws Exception {
                        if (phoenixConn == null && !phoenixConn.isClosed()) {
                            phoenixConn.close();
                        }
                        if (redisClient != null) {
                            redisClient.close();//关闭客户端
                            //如果客户端是从连接池获取的，则是归还连接给客户端
                            //如果客户端是通过new jedis ，则是关闭
                        }
                    }

                    @Override
                    public OrderWide map(OrderWide orderWide) throws Exception {
                        // 读取维度数据, 6张表
                        // 1.补齐user_info
                        JSONObject userInfo = DimUtil.readDim(redisClient,phoenixConn, "dim_user_info", orderWide.getUser_id());
                        orderWide.setUser_gender(userInfo.getString("GENDER"));
                        orderWide.calcuUserAge(userInfo.getString("BIRTHDAY"));

                        // 2. 补充省份
                        JSONObject baseProvince = DimUtil.readDim(redisClient,phoenixConn, "dim_base_province", orderWide.getProvince_id());
                        orderWide.setProvince_name(baseProvince.getString("NAME"));
                        orderWide.setProvince_3166_2_code(baseProvince.getString("ISO_3166_2"));
                        orderWide.setProvince_area_code(baseProvince.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(baseProvince.getString("ISO_CODE"));

                        // 3. sku_info
                        JSONObject skuInfo = DimUtil.readDim(redisClient,phoenixConn, "dim_sku_info", orderWide.getSku_id());
                        orderWide.setSku_name(skuInfo.getString("SKU_NAME"));
                        orderWide.setSku_price(skuInfo.getBigDecimal("PRICE"));

                        orderWide.setSpu_id(skuInfo.getLong("SPU_ID"));
                        orderWide.setTm_id(skuInfo.getLong("TM_ID"));
                        orderWide.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));

                        // 4. spu_info
                        JSONObject spuInfo = DimUtil.readDim(redisClient,phoenixConn, "dim_spu_info", orderWide.getSpu_id());
                        orderWide.setSpu_name(spuInfo.getString("SPU_NAME"));

                        // 5. base_trademark
                        JSONObject baseTrademark = DimUtil.readDimFromPhoenix(phoenixConn, "dim_base_trademark", orderWide.getTm_id());
                        orderWide.setTm_name(baseTrademark.getString("TM_NAME"));


                        // 6. c3
                        JSONObject c3 = DimUtil.readDim(redisClient,phoenixConn, "dim_base_category3", orderWide.getCategory3_id());
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
/*

缓存会加过期时间 ttl

缓存选择redis
数据结构的选择
string
    key       value
    表名:id    json格式的字符串

    优点:
        存取方便
        单独设置ttl
    缺点:
        key过多, 不方便管理
         解决: 可以单独存储到一个数据库中
            默认16个库, 选择14号
list
    key         value
    表名        所有的维度

    优点:
        key比较少, 一张表一个key

     缺点:
        不方便按照id来取对应的维度信息

set


hash

   key     field  value
   表名     id1   jison字符串
            id2   jison字符串
            id2   jison字符串

   最大的问题, 没有办法单独给每个维度设置过期时间

zset

个别的维度在短时间多次使用, 每次都需要去数据库中查找, 效率低.
加缓存

1. 使用flink的状态
   优点:
    1. 缓存存储在flink的内存中, 读写方便, 速度快

   缺点;
    1. 维度缓存到内存, 会占用flink的内存, 影响flink的执行

    2. 最大的问题, 一旦维度数据发生变化, 则状态的没有办法及时更新.
          因为, 没有办法收到维度变化的通知

2. redis
    优点:
        1. 存在于专门的redis缓存中, 内存比较大

        2. 维度发生变化, redis的缓存能否更新?
            谁能知道维度发生变化?
                分流app当发现维度有变化,可以去redis缓存中更新维度.
    缺点:
        每次访问redis, 都需要通过网络


 */