package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.BaseSqlApp;
import com.atguigu.gmall.realtime.bean.KeywordStats;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.function.IkAnalyzer;
import com.atguigu.gmall.realtime.function.KwProduct;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author coderhyh
 * @create 2022-04-13 18:43
 */
class ProductKeyWodStatsApp extends BaseSqlApp {
    public static void main(String[] args) {
        new ProductKeyWodStatsApp().init(4006, 4, "ProductKeyWodStatsApp");

    }

    @Override
    protected void run(StreamTableEnvironment tEnv) {
        // 1. 建立动态表与Kafka的topic进行关联
        tEnv.executeSql("create table product(" +
                "   stt string, " +
                "   edt string, " +
                "   sku_name string, " +
                "   click_ct bigint, " +
                "   cart_ct bigint, " +
                "   order_ct bigint " +
                ")with(" +
                "   'connector' = 'kafka', " +
                "   'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092', " +
                "   'properties.group.id' = 'ProductKeyWodStatsApp', " +
                "   'topic' = '" + Constant.TOPIC_DWS_PRODUCT_STATS + "', " +
                "   'scan.startup.mode' = 'latest-offset', " +
                "   'format' = 'json' " +
                ")");

        //        tEnv.sqlQuery("select * from product ").execute().print();

        // 1.过滤出来三个ct中至少有一个不为0的记录
        Table t1 = tEnv.sqlQuery("select" +
                " * from " +
                "product " +
                "where click_ct > 0 " +
                "or cart_ct > 0 " +
                "or order_ct > 0");
        tEnv.createTemporaryView("t1", t1);


        // 2. 对sku_name 进行分词
        // 2.1 注册分词函数
        tEnv.createTemporaryFunction("ik_analyzer", IkAnalyzer.class);

        Table t2 = tEnv.sqlQuery("select " +
                "stt, edt, word, click_ct, cart_ct, order_ct " +
                "from t1, " +
                "lateral table( ik_analyzer(sku_name) )");
        tEnv.createTemporaryView("t2", t2);

        // 3. 把一行中的三个count 列, 变成3行, 自定义 table函数     click  10
        tEnv.createTemporaryFunction("kw_product", KwProduct.class);

        Table t3 = tEnv.sqlQuery("select" +
                " stt, " +
                " edt, " +
                " word keyword, " +
                " source, " +
                " ct " +
                "from t2 " +
                "join lateral table( kw_product(click_ct, cart_ct, order_ct) ) on true");
        tEnv.createTemporaryView("t3", t3);

        Table resultTable = tEnv.sqlQuery("select " +
                " stt, " +
                " edt, " +
                " keyword, " +
                " source, " +
                " sum(ct) ct," +
                " unix_timestamp() * 1000 ts " +
                "from t3 " +
                "group by stt, edt, keyword, source");


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
