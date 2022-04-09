package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.common.Constant;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author coderhyh
 * @create 2022-04-08 23:33
 * 为了dws层计算uv值，做提前处理，去重
 */
public class DwmUvApp extends BaseAppV1 {
    public static void main(String[] args) {

        new DwmUvApp().init(
                2003,
                4,
                "DwmUvApp",
                "DwmUvApp", Constant.TOPIC_DWD_PAGE
        );

    }

    //写具体的业务逻辑
    @Override
    protected void run(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //        stream.print();


        stream.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(3)
                                )
                                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts");
                                    }
                                })
                )
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        
                    }

                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>.Context context,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {


                    }
                }).print();
    }
}
