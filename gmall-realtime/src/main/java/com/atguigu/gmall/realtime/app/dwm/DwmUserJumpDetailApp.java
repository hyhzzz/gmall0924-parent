package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.common.Constant;

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
public class DwmUserJumpDetailApp extends BaseAppV1 {
    public static void main(String[] args) {
        //环境初始化
        new DwmUserJumpDetailApp().init(2004,
                4,
                "DwmUserJumpDetailApp",
                "DwmUserJumpDetailApp",
                Constant.TOPIC_DWD_PAGE
        );
    }
    //写具体的业务逻辑
    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        stream =
                env.fromElements(
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":11000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000}",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"home\"},\"ts\":14000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"detail\"},\"ts\":50000} "
                );


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
                .<JSONObject>begin("entry")
                .where(new SimpleCondition<JSONObject>() {
                    // 条件1: 进入第一个页面 (没有上一个页面)
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                })
                .next("normal")
                .where(new SimpleCondition<JSONObject>() {
                    // 条件2: 一个或多个访问记录
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId != null && lastPageId.length() > 0;
                    }
                })
                .within(Time.seconds(5));

        // 3. 把模式应用到流上, 得到一个模式流
        PatternStream<JSONObject> ps = CEP.pattern(keyedStream, pattern);
        // 3. 从模式流中获取匹配到的数据, 或者超时的数据
        SingleOutputStreamOperator<JSONObject> normal = ps.select(
                new OutputTag<JSONObject>("jump") {
                },
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map,
                                              long timeoutTimestamp) throws Exception {
                        return map.get("entry").get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                        return null;
                    }
                }
        );
        normal.getSideOutput(new OutputTag<JSONObject>("jump") {
                })
                .print();
    }
}