package com.atguigu.gmall.realtime.common;

/**
 * @author coderhyh
 * @create 2022-04-07 0:07
 */
public class Constant {
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    public static final String CLICKHOUSE_DB = "gmall2022";
    public static final String CLICKHOUSE_TABLE_VISITOR_STATS_2022 = "visitor_stats_2022";
    public static final String CLICKHOUSE_TABLE_PRODUCT_STATS_2022 = "product_stats_2022";
    public static final String CLICKHOUSE_TABLE_PROVINCE_STATS_2022 = "province_stats_2022";
    public static final String CLICKHOUSE_TABLE_KEYWORD_STATS_2022 = "keyword_stats_2022";


    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String CLICKHOUSE_PRE_URL = "jdbc:clickhouse://hadoop102:8123/";



    public static Object KAFKA_BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";


    public static final String TOPIC_ODS_LOG = "ods_log";
    public static final String TOPIC_ODS_DB = "ods_db";

    public static final String TOPIC_DWD_START = "dwd_start";
    public static final String TOPIC_DWD_PAGE = "dwd_page";
    public static final String TOPIC_DWD_DISPLAY = "dwd_display";
    public static final String TOPIC_DWD_ORDER_DETAIL = "dwd_order_detail";
    public static final String TOPIC_DWD_PAYMENT_INFO = "dwd_payment_info";
    public static final String TOPIC_DWD_REFUND_PAYMENT = "dwd_refund_payment";
    public static final String TOPIC_DWD_FAVOR_INFO = "dwd_favor_info";
    public static final String TOPIC_DWD_CART_INFO = "dwd_cart_info";
    public static final String TOPIC_DWD_ORDER_INFO = "dwd_order_info";
    public static final String TOPIC_DWD_COMMENT_INFO = "dwd_comment_info";

    public static final String TOPIC_DWM_ORDER_WIDE = "dwm_order_wide";
    public static final String TOPIC_DWM_UV = "dwm_uv";
    public static final String TOPIC_DWM_UJ = "dwm_uj";
    public static final String TOPIC_DWM_PAYMENT_WIDE = "dwm_payment_wide";


    public static final String TOPIC_DWS_PRODUCT_STATS = "dws_product_stats";


    public static final String FIVE_STAR_GOOD_COMMENT = "1205";
    public static final String FOUR_STAR_GOOD_COMMENT = "1204";



}
