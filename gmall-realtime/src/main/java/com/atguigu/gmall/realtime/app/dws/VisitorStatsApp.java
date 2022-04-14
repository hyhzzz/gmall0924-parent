package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV2;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.HashMap;

import static com.atguigu.gmall.realtime.common.Constant.TOPIC_DWD_PAGE;
import static com.atguigu.gmall.realtime.common.Constant.TOPIC_DWM_UJ;
import static com.atguigu.gmall.realtime.common.Constant.TOPIC_DWM_UV;

/**
 * @author coderhyh
 * @create 2022-04-11 22:40
 */
public class VisitorStatsApp extends BaseAppV2 {
    public static void main(String[] args) {
        //环境初始化
        new VisitorStatsApp().init(
                4001,
                4,
                "VisitorStatsApp",
                "VisitorStatsApp",
                TOPIC_DWD_PAGE,
                TOPIC_DWM_UV,
                TOPIC_DWM_UJ
        );

    }

    //写具体的业务逻辑
    @Override
    protected void run(StreamExecutionEnvironment env,
                       HashMap<String, DataStreamSource<String>> topicStreamMap) {

        //        topicStreamMap.get(TOPIC_DWD_PAGE).print("page");
        //        topicStreamMap.get(TOPIC_DWM_UV).print("uv");
        //        topicStreamMap.get(TOPIC_DWM_UJ).print("uj");

        // 1. 把多个流union成一个流
        DataStream<VisitorStats> vsStream = unionOne(topicStreamMap);
        //        vsStream.print();

        // 2. 开窗聚合
        SingleOutputStreamOperator<VisitorStats> vsAggregatedStream = windowAndAgg(vsStream);
        //        vsAggregatedStream.print();

        // 3. 数据写入到ClickHouse中
        writToClickHouse(vsAggregatedStream);

    }

    private void writToClickHouse(SingleOutputStreamOperator<VisitorStats> vsAggregatedStream) {

         /*
                没有专门clickhouseSink, 在jdbcSink的基础上进行封装, 得到一个ClickhouseSink
         */

        vsAggregatedStream
                .addSink(FlinkSinkUtil.getClickHouseSink(Constant.CLICKHOUSE_DB,
                                Constant.CLICKHOUSE_TABLE_VISITOR_STATS_2022,
                                VisitorStats.class
                        )
                );
    }

    private SingleOutputStreamOperator<VisitorStats> windowAndAgg(DataStream<VisitorStats> vsStream) {
        // 基于事件时间
        return vsStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                                .withTimestampAssigner((vs, ts) -> vs.getTs())
                )
                .keyBy(vs -> vs.getVc() + "_" + vs.getCh() + "_" + vs.getAr() + "_" + vs.getIs_new())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(new OutputTag<VisitorStats>("late") {
                })
                .reduce(
                        new ReduceFunction<VisitorStats>() {
                            @Override
                            public VisitorStats reduce(VisitorStats vs1,
                                                       VisitorStats vs2) throws Exception {
                                vs1.setPv_ct(vs1.getPv_ct() + vs2.getPv_ct());
                                vs1.setUv_ct(vs1.getUv_ct() + vs2.getUv_ct());
                                vs1.setUj_ct(vs1.getUj_ct() + vs2.getUj_ct());
                                vs1.setSv_ct(vs1.getSv_ct() + vs2.getSv_ct());
                                vs1.setDur_sum(vs1.getDur_sum() + vs2.getDur_sum());
                                return vs1;
                            }
                        },
                        new ProcessWindowFunction<VisitorStats, VisitorStats, String, TimeWindow>() {
                            @Override
                            public void process(String key,
                                                Context ctx,
                                                Iterable<VisitorStats> elements, // 只有一个值, 就是前面聚合的最终结果
                                                Collector<VisitorStats> out) throws Exception {

                                VisitorStats vs = elements.iterator().next();

                                String stt = AtguiguUtil.toDateTime(ctx.window().getStart());
                                String edt = AtguiguUtil.toDateTime(ctx.window().getEnd());

                                vs.setStt(stt);
                                vs.setEdt(edt);

                                vs.setTs(System.currentTimeMillis());  // 改成统计时间
                                out.collect(vs);
                            }
                        }
                );
    }

    private DataStream<VisitorStats> unionOne(HashMap<String, DataStreamSource<String>> topicStreamMap) {

          /*
            private String vc;
            //维度：渠道
            private String ch;
            //维度：地区
            private String ar;
            //维度：新老用户标识
            private String is_new;
            //度量：独立访客数
            private Long uv_ct = 0L;
            //度量：页面访问数
            private Long pv_ct = 0L;
            //度量： 进入次数
            private Long sv_ct = 0L;
            //度量： 跳出次数
            private Long uj_ct = 0L;
            //度量： 持续访问时间
            private Long dur_sum = 0L;
            //统计时间
            private Long ts;
         */

        // 1. pv和持续访问时间
        SingleOutputStreamOperator<VisitorStats> pvAndDuringTimeStream = topicStreamMap.get(TOPIC_DWD_PAGE)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);

                    JSONObject common = obj.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    Long ts = obj.getLong("ts");
                    Long duringTime = obj.getJSONObject("page").getLong("during_time");

                    return new VisitorStats(
                            "", "",
                            vc, ch, ar, isNew,
                            0L,
                            1L,
                            0L,
                            0L,
                            duringTime,
                            ts
                    );
                });


        // 2. uv
        SingleOutputStreamOperator<VisitorStats> uvStream = topicStreamMap
                .get(TOPIC_DWM_UV)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);
                    JSONObject common = obj.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    Long ts = obj.getLong("ts");

                    return new VisitorStats(
                            "", "",
                            vc, ch, ar, isNew,
                            1L, 0L, 0L, 0L, 0L,
                            ts
                    );
                });


        // 3. uj
        SingleOutputStreamOperator<VisitorStats> ujStream = topicStreamMap
                .get(TOPIC_DWM_UJ)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);
                    JSONObject common = obj.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    Long ts = obj.getLong("ts");

                    return new VisitorStats(
                            "", "",
                            vc, ch, ar, isNew,
                            0L, 0L, 0L, 1L, 0L,
                            ts
                    );
                });

        // 4. sv
        SingleOutputStreamOperator<VisitorStats> svStream = topicStreamMap
                .get(TOPIC_DWD_PAGE)
                .flatMap(new FlatMapFunction<String, VisitorStats>() {
                    @Override
                    public void flatMap(String json,
                                        Collector<VisitorStats> out) throws Exception {
                        JSONObject obj = JSON.parseObject(json);
                        String lastPageId = obj
                                .getJSONObject("page")
                                .getString("last_page_id");

                        if (lastPageId == null || lastPageId.length() == 0) {
                            JSONObject common = obj.getJSONObject("common");
                            String vc = common.getString("vc");
                            String ch = common.getString("ch");
                            String ar = common.getString("ar");
                            String isNew = common.getString("is_new");

                            Long ts = obj.getLong("ts");

                            VisitorStats vs = new VisitorStats(
                                    "", "",
                                    vc, ch, ar, isNew,
                                    0L, 0L, 1L, 0L, 0L,
                                    ts
                            );
                            out.collect(vs);
                        }
                    }
                });

        return pvAndDuringTimeStream.union(uvStream, ujStream, svStream);
    }
}
