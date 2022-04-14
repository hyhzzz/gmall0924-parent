package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.BaseSqlApp;
import com.atguigu.gmall.realtime.bean.ProvinceStats;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * @author coderhyh
 * @create 2022-04-13 13:27
 */
public class ProvinceStatsApp extends BaseSqlApp {
    public static void main(String[] args) {
        //环境初始化
        new ProvinceStatsApp().init(4003,
                4,
                "ProvinceStatsApp");
    }

    //写具体的业务逻辑
    @Override
    protected void run(StreamTableEnvironment tEnv) {
        // 1. 建立动态表与Kafka的topic进行关联
        tEnv.executeSql("create table order_wide(" +
                " province_id bigint, " +
                " province_name string, " +
                " province_area_code string, " +
                " province_iso_code string, " +
                " province_3166_2_code string, " +
                " split_total_amount decimal(20, 2), " +
                " order_id bigint, " +
                " create_time string, " +
                " et as TO_TIMESTAMP(create_time), " +
                " watermark for et as et - interval '3' second " +
                ")with(" +
                "   'connector' = 'kafka', " +
                "   'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', " +
                "   'properties.group.id' = 'ProvinceStatsApp', " +
                "   'topic' = '" + Constant.TOPIC_DWM_ORDER_WIDE + "', " +
                //                "   'scan.startup.mode' = 'latest-offset', " +
                "   'scan.startup.mode' = 'earliest-offset', " +
                "   'format' = 'json' " +
                ")");

        //                tEnv.sqlQuery("select * from order_wide").execute().print();


        // 2. 开窗聚合
        Table table = tEnv.sqlQuery("select" +
                " province_id," +
                " province_name," +
                " province_area_code area_code," +
                " province_iso_code iso_code, " +
                " province_3166_2_code iso_3166_2," +
                " date_format(tumble_start(et, interval '5' second), 'yyyy-MM-dd HH:mm:ss') stt, " +
                " date_format(tumble_end(et, interval '5' second), 'yyyy-MM-dd HH:mm:ss') edt, " +
                " sum(split_total_amount) order_amount, " +
                " count(distinct(order_id)) order_count, " +
                " unix_timestamp()*1000 ts " +
                "from order_wide " +
                "group by province_id, province_name, province_area_code, province_iso_code, province_3166_2_code, " +
                " tumble(et, interval '5' second )");

//        table.execute().print();


        // 3. 把表转成流写出去
        tEnv
                .toRetractStream(table, ProvinceStats.class)
                .filter(t -> t.f0)
                .map((t -> t.f1))
                .addSink(FlinkSinkUtil.getClickHouseSink(Constant.CLICKHOUSE_DB,
                        Constant.CLICKHOUSE_TABLE_PROVINCE_STATS_2022,
                        ProvinceStats.class
                ));

    }
}
