package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-06 23:50
 */
public class DwdLogApp extends BaseAppV1 {

    private final String PAGE = "page";
    private final String START = "start";
    private final String DISPLAY = "display";


    public static void main(String[] args) throws Exception {

        //环境初始化
        new DwdLogApp().init(2001,
                4,
                "DwdLogApp",
                "DwdLogApp",
                Constant.TOPIC_ODS_LOG);

    }

    //写具体的业务逻辑
    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        //        stream.print();

        //1. 区别新老客户，把is_new的字段做一个验证，
        SingleOutputStreamOperator<JSONObject> validatedStream = validateNewOrOld(stream);
        //        validatedStream.print();

        //2.对数据进行分流   页面日志-->主流   启动日志 曝光日志  -->测输出流
        HashMap<String, DataStream> threeStreams = splitStream(validatedStream);
        threeStreams.get(PAGE).print("page==");
        threeStreams.get(DISPLAY).print("display##");
        threeStreams.get(START).print("start$$");


        //3.不同的流写入到不同的topic中
        writeToKafka(threeStreams);

    }

    private void writeToKafka(HashMap<String, DataStream> threeStreams) {

        threeStreams.get(START)
                .map(JSONObject::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_START));

        threeStreams.get(DISPLAY)
                .map(JSONObject::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_DISPLAY));

        threeStreams.get(PAGE)
                .map(JSONObject::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_PAGE));
    }

    private HashMap<String, DataStream> splitStream(SingleOutputStreamOperator<JSONObject> validatedStream) {

        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("page") {
        };
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {
        };

        SingleOutputStreamOperator<JSONObject> startStream = validatedStream.process(new ProcessFunction<JSONObject, JSONObject>() {

            @Override
            public void processElement(JSONObject jsonStr,
                                       ProcessFunction<JSONObject, JSONObject>.Context ctx,
                                       Collector<JSONObject> out) throws Exception {


                //1.判断是不是启动日志
                if (jsonStr.containsKey("start")) {
                    out.collect(jsonStr);
                } else {
                    //2.判断是不是页面日志
                    if (jsonStr.containsKey("page")) {
                        ctx.output(pageTag, jsonStr);
                    }
                    //3.判断是不是曝光日志
                    if (jsonStr.containsKey("displays")) {
                        //因为曝光日志中 的曝光数据是数组，最好展开，分别放入流中
                        JSONArray array = jsonStr.getJSONArray("displays");
                        for (int i = 0; i < array.size(); i++) {
                            JSONObject display = array.getJSONObject(i);

                            //给曝光数据增添一些其他的字段
                            display.putAll(jsonStr.getJSONObject("common"));
                            display.putAll(jsonStr.getJSONObject("page"));
                            display.put("ts", jsonStr.getLong("ts"));

                            ctx.output(displayTag, display);
                        }
                    }
                }
            }
        });

        //返回多个流  1.返回一个集合 2.返回元组  3.map
        DataStream<JSONObject> pageStream = startStream.getSideOutput(pageTag);
        DataStream<JSONObject> displayStream = startStream.getSideOutput(displayTag);

        HashMap<String, DataStream> streams = new HashMap<>();
        streams.put(START, startStream);
        streams.put(PAGE, pageStream);
        streams.put(DISPLAY, displayStream);

        return streams;


    }

    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(DataStreamSource<String> stream) {
        /*

        对is_new做修改：如果是新用户就设置为1，如果是老用户就设置为0
        如何判断新老用户：这个用户的第一条记录 应该是1 其他记录应该是0
        使用状态存储时间戳：第一条数据来的时候，状态是空，如何给状态赋值，后面的数据再来就不为空，就不是第一条了。

        如果数据有乱序？ 事件时间+水印+窗口 :使用五秒的滚动窗口，找到这个用户的第一个窗口，时间戳最小的那个才是第一条记录，
        其他的都不是。
        其他窗口里面所有操作肯定都是0

         */

        return stream
                //封装成json对象  string-->jsonStr
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
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

                    private ValueState<Long> firstWindowState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstWindowState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("firstWindowState", Long.class));
                    }

                    @Override
                    public void process(String s,
                                        ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>.Context context,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {

                        //判断是否第一个窗口
                        if (firstWindowState.value() == null) {
                            //第一个窗口
                            //找到所有元素中时间戳最小的那个元素，把他的is_new置为1，其他的是0
                            List<JSONObject> list = AtguiguUtil.toList(elements);
                            JSONObject min = Collections.min(list, Comparator.comparing(o -> o.getLong("ts")));

                            //更新状态
                            firstWindowState.update(min.getLong("ts"));

                            for (JSONObject obj : list) {
                                if (obj == min) {
                                    obj.getJSONObject("common").put("is_new", 1);
                                } else {
                                    obj.getJSONObject("common").put("is_new", 0);
                                }
                                out.collect(obj);
                            }

                        } else {
                            //不是第一个窗口
                            for (JSONObject obj : elements) {
                                obj.getJSONObject("common").put("is_new", 0);
                                out.collect(obj);
                            }
                        }
                    }
                });
    }
}
