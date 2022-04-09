package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-08 23:33
 * 为了dws层计算uv值，做提前处理，去重
 */
public class DwmUvApp extends BaseAppV1 {
    public static void main(String[] args) {

        //环境初始化
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

                    private SimpleDateFormat sdf;
                    private ValueState<String> dateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        dateState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("dateState", String.class));

                        sdf = new SimpleDateFormat("yyyy-MM-dd");

                    }

                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>.Context context,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {

                        String yesterday = dateState.value();
                        String today = sdf.format(context.window().getStart());

                        if (!today.equals(yesterday)) {
                            //今天和昨天不一样就是跨天了
                            dateState.clear();
                        }

                        if (dateState.value() == null) {
                            //表示第一条数据来了，只有第一个窗口才有必要把Iterable转成list，其他的窗口，可以忽略
                            List<JSONObject> list = AtguiguUtil.toList(elements);
                            //                            Collections.min(list, new Comparator<JSONObject>() {
                            //                                @Override
                            //                                public int compare(JSONObject o1, JSONObject o2) {
                            //                                    return o1.getLong("ts").compareTo(o2.getLong("ts"));
                            //                                }
                            //                            });

                            //                            Collections.min(list, (o1, o2) -> o1.getLong("ts").compareTo(o2.getLong("ts")));

                            JSONObject min = Collections.min(list, Comparator.comparing(o -> o.getLong("ts")));
                            out.collect(min);

                            //更新状态 状态存的年月日
                            dateState.update(sdf.format(min.getLong("ts")));
                        }
                    }
                })
                .map(obj -> obj.toJSONString())
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWM_UV));
    }
}
