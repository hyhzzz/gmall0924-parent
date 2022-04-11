package com.atguigu.gmall.realtime.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.JdbcUtil;
import com.atguigu.gmall.realtime.util.RedisUtil;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import redis.clients.jedis.Jedis;

/**
 * @author coderhyh
 * @create 2022-04-08 13:18
 * 自定义实现phoenix sink
 */
public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    private Connection conn;
    private ValueState<Boolean> tableCreatedState;
    private Jedis redisClient;

    @Override
    public void open(Configuration parameters) throws Exception {

        //获取phoenix连接对象
        conn = JdbcUtil.getJdbcConnect(Constant.PHOENIX_DRIVER, Constant.PHOENIX_URL, null, null);

        //        conn.setAutoCommit(true);

        tableCreatedState = getRuntimeContext().getState(
                new ValueStateDescriptor<Boolean>("tableCreatedState", Boolean.class));

        redisClient = RedisUtil.getRedisClient();

    }

    @Override
    public void close() throws Exception {

        //关闭phoenix连接对象
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {

        //1. 建表
        checkTable(value);

        //2.把这条数据写入到对应的表中
        writeToPhoenix(value);

        //3.更新缓存
        updateCache(value);


    }

    private void updateCache(Tuple2<JSONObject, TableProcess> value) {
        // 1. 优雅
        // 1.1 如果存在就更新, 不存在, 就不用操作
        JSONObject dim = value.f0;
        TableProcess tp = value.f1;

        // key: table:id
        String key = tp.getSinkTable() + ":" + dim.getLong("id");
        if (redisClient.exists(key)) {
            // 先把字段名变成大写之后, 再去更新
            JSONObject upperDim = new JSONObject();
            for (Map.Entry<String, Object> entry : dim.entrySet()) {
                upperDim.put(entry.getKey().toUpperCase(), entry.getValue());
            }
            redisClient.setex(key, 2 * 24 * 60 * 60, upperDim.toJSONString());
        }

        // 2. 粗暴
        // 直接把缓存中的数据删除. 将来读到时候读不到, 自然就去数据读取最新的维度
        //redisClient.del(key);
    }

    private void writeToPhoenix(Tuple2<JSONObject, TableProcess> value) throws SQLException {
        JSONObject data = value.f0;
        TableProcess tp = value.f1;
        // 拼接一个sql语句
        // upsert into t(id, name, age) values(?,?,?);
        StringBuilder sql = new StringBuilder();
        sql
                .append("upsert into ")
                .append(tp.getSinkTable())
                .append("(")
                // 拼接字段名
                .append(tp.getSinkColumns())
                .append(") values(")
                //  拼接占位符 有几个字段, 就拼接几个问号
                .append(tp.getSinkColumns().replaceAll("[^,]+", "?"))
                .append(")");

        System.out.println("插入语句: " + sql.toString());
        PreparedStatement ps = conn.prepareStatement(sql.toString());
        // 使用数据中的每个字段的值, 给占位符赋值
        // upsert into t(id, name, age) values(?,?,?);
        // 取出字段的名字
        String[] columns = tp.getSinkColumns().split(",");
        // 列名就是data中的key, 根据key取出数据, 给占位符赋值
        for (int i = 0; i < columns.length; i++) {
            String key = columns[i];
            Object v = data.get(key);
            ps.setString(i + 1, v == null ? null : v.toString()); // 避免空指针
        }
        ps.execute();
        conn.commit();
        ps.close();

    }

    /**
     * 提前建表
     *
     * @param value
     */
    private void checkTable(Tuple2<JSONObject, TableProcess> value) throws SQLException, IOException {

        if (tableCreatedState.value() == null) {
            TableProcess tp = value.f1;

            // 建表 执行sql ddl 建表语句
            // create table if not exists t(id varchar, age  varchar, name varchar , constraint pk primary key(id, age))SALT_BUCKETS = 4;
            // phoenix中的表的表必须有主键!!!
            StringBuilder sql = new StringBuilder();
            // 拼接sql语句
            sql
                    .append("create table if not exists ")
                    .append(tp.getSinkTable())
                    .append("(")
                    .append(tp.getSinkColumns().replaceAll("([^,]+)", "$1 varchar"))
                    .append(", constraint pk primary key(")
                    .append(tp.getSinkPk() == null ? "id" : tp.getSinkPk())
                    .append("))")
                    //加盐表
                    .append(tp.getSinkExtend() == null ? "" : tp.getSinkExtend());

            System.out.println("建表语句: " + sql.toString());
            PreparedStatement ps = conn.prepareStatement(sql.toString());

            ps.execute();
            conn.commit();
            ps.close();

            tableCreatedState.update(true);
        }
    }
}
/*

SALT_BUCKETS = 4：给表提前做四个预分区
通过这个参数, 控制在phoenix建表的时候, 建  盐表

hbase 中的region

regionserver
region
 默认建表只有一个region, 所有的数据肯定都在这一个region中

 当region增长到一定程度的时候, 会自动分裂
    什么地方分裂?
    旧的算法(0.97之前)
        10G之后开发分裂

    新的算法(0.97之后)
        2 * 128*2^n

 region的自动全迁移
    什么时候迁移?

企业中, 一般不会让region自动分裂, 也就没有自动迁移

 预分区
    建表的时候, 就定义好这个表一共多少个region, 以后永远不变

--------

现在是用phoenix建表, 如何预分区?
     盐表

 */