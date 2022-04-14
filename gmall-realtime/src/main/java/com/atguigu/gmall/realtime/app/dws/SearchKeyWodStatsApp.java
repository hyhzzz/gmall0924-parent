package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.BaseSqlApp;
import com.atguigu.gmall.realtime.bean.KeywordStats;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.function.IkAnalyzer;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author coderhyh
 * @create 2022-04-13 18:43
 */
class SearchKeyWodStatsApp extends BaseSqlApp {
    public static void main(String[] args) {
        new SearchKeyWodStatsApp().init(4004, 4, "SearchKeyWodStatsApp");

    }

    @Override
    protected void run(StreamTableEnvironment tEnv) {
        // 1. 建立动态表与Kafka的topic进行关联
        tEnv.executeSql("create table page(" +
                "   page map<string, string>, " +
                "   ts bigint, " +
                "   et as to_timestamp_ltz(ts, 3), " +
                "   watermark for et as et - interval '3' second " +
                ")with(" +
                "   'connector' = 'kafka', " +
                "   'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', " +
                "   'properties.group.id' = 'SearchKeyWodStatsApp', " +
                "   'topic' = '" + Constant.TOPIC_DWD_PAGE + "', " +
                "   'scan.startup.mode' = 'latest-offset', " +
                "   'format' = 'json' " +
                ")");


        //        tEnv.sqlQuery("select * from page").execute().print();


        // 2. 过滤出来搜索计算, 把搜索关键词取出
             /*
          "page": {
            "page_id": "good_list",
            "item": "小米盒子",
            "during_time": 10675,
            "item_type": "keyword",
            "last_page_id": "home"
          },
          ts:...
         */
        Table t1 = tEnv.sqlQuery("select" +
                " page['item'] kw, " +
                " et " +
                "from page " +
                "where page['page_id']='good_list' " +
                "and page['item_type']='keyword' " +
                "and page['item'] is not null");
        tEnv.createTemporaryView("t1", t1);


        // 3. 对关键词进行分词
        // fink 中没有提供直接可用的函数, 所有需要自定义函数
        // scalar function
        // table function
        // aggregate function
        // tableaggregate function
        // 分词选择制表函数

        // 3.1 注册函数
        tEnv.createTemporaryFunction("ik_analyzer", IkAnalyzer.class);


        Table t2 = tEnv.sqlQuery("select" +
                " word, " +
                " et " +
                "from t1 " +
                "join lateral table( ik_analyzer(kw) ) on true");
        tEnv.createTemporaryView("t2", t2);

        // 4. 开窗聚合
        Table resultTable = tEnv.sqlQuery("select " +
                "  CONVERT_TZ(date_format(window_start, 'yyyy-MM-dd HH:mm:ss'), 'UTC', 'Asia/Shanghai') stt, " +
                "  CONVERT_TZ(date_format(window_end, 'yyyy-MM-dd HH:mm:ss'), 'UTC', 'Asia/Shanghai') edt, " +
                "  word keyword, " +
                "  'search' source, " +
                "  count(*) ct, " +
                "  unix_timestamp() * 1000 ts " +
                "from table( tumble(table t2, descriptor(et), interval  '5' second) )" +
                "group by word, window_start, window_end");


        tEnv
                .toRetractStream(resultTable, KeywordStats.class)
                .filter(t -> t.f0)
                .map(t -> t.f1)
                .addSink(FlinkSinkUtil.getClickHouseSink(
                        Constant.CLICKHOUSE_DB,
                        Constant.CLICKHOUSE_TABLE_KEYWORD_STATS_2022,
                        KeywordStats.class
                ));

    }
}
