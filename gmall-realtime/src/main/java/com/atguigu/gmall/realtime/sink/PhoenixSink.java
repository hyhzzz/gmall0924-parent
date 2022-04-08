package com.atguigu.gmall.realtime.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.JdbcUtil;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author coderhyh
 * @create 2022-04-08 13:18
 * 自定义实现phoenix sink
 */
public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    private Connection conn;
    private ValueState<Boolean> tableCreatedState;

    @Override
    public void open(Configuration parameters) throws Exception {

        //获取phoenix连接对象
        conn = JdbcUtil.getJdbcConnect(Constant.PHOENIX_DRIVER, Constant.PHOENIX_URL, null, null);

        //        conn.setAutoCommit(true);

        tableCreatedState = getRuntimeContext().getState(
                new ValueStateDescriptor<Boolean>("tableCreatedState", Boolean.class));


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