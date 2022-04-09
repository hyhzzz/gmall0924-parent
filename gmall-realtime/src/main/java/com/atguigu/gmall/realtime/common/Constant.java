package com.atguigu.gmall.realtime.common;

/**
 * @author coderhyh
 * @create 2022-04-07 0:07
 */
public class Constant {
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";


    public static final String TOPIC_ODS_LOG = "ods_log";
    public static final String TOPIC_ODS_DB = "ods_db";

    public static final String TOPIC_DWD_START = "dwd_start";
    public static final String TOPIC_DWD_PAGE = "dwd_page";
    public static final String TOPIC_DWD_DISPLAY = "dwd_display";

    public static final String TOPIC_DWM_UV = "dwm_uv";
    public static final String TOPIC_DWM_UJ = "dwm_uj";


    public static Object KAFKA_BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
}
