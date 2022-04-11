package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author coderhyh
 * @create 2022-04-09 10:42
 */
public class DwmUserJumpDetailApp_1 extends BaseAppV1 {
    public static void main(String[] args) {
        //环境初始化
        new DwmUserJumpDetailApp_1().init(2004,
                4,
                "DwmUserJumpDetailApp_1",
                "DwmUserJumpDetailApp_1",
                Constant.TOPIC_DWD_PAGE
        );
    }
    //写具体的业务逻辑
    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {

        // 1. 数据封装, 添加水印, 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
                )
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"));


        // 2. 定义模式
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("entry1")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                })
                .next("entry2")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                })
                .within(Time.seconds(5));

        // 3. 把模式应用到流上, 得到一个模式流
        PatternStream<JSONObject> ps = CEP.pattern(keyedStream, pattern);
        // 4. 从模式流中获取匹配到的数据, 或者超时的数据
        SingleOutputStreamOperator<JSONObject> normal = ps.select(
                new OutputTag<JSONObject>("jump") {
                },
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map,
                                              long timeoutTimestamp) throws Exception {
                        // 如果来了一个入口, 第二个入口在规定时间没有, 则第一个入口是跳出
                        return map.get("entry1").get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        // 如果两个都是入口, 匹配模式, 第一个入口就是跳出
                        return map.get("entry1").get(0);
                    }

                }
        );
        normal.getSideOutput(new OutputTag<JSONObject>("jump") {
                })
                // 5. 把侧输出流的结果写入到dwm_uj
                .union(normal)
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWM_UJ));
    }
}