package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.sql.Connection;
import java.util.List;

import redis.clients.jedis.Jedis;

/**
 * @author coderhyh
 * @create 2022-04-09 19:11
 */
public class DimUtil {

    public static JSONObject readDimFromPhoenix(Connection phoenixConn,
                                                String tableName,
                                                Long id) throws Exception {
        String sql = "select * from " + tableName + " where id=?";

        //数组中存储所有的占位符的值. 占位符有几个, 数组的长度就应该是几
        String[] args = {id.toString()};

        // 执行sql, 把得到的结果封装到一个List返回
        List<JSONObject> list = JdbcUtil.queryList(phoenixConn, sql, args, JSONObject.class);

        return list.get(0);  // 查询到维度应该只有一条
    }

    public static JSONObject readDim(Jedis redisClient, Connection phoenixConn, String tableName, Long id) throws Exception {

        //1.先从redis读取维度信息
        JSONObject dim = readDimFromRedis(redisClient, tableName, id);
        if (dim == null) { //从缓存中没有读到维度数据
            //2.如果redis没有读到
            dim = readDimFromPhoenix(phoenixConn, tableName, id);
            //3.在从phoenix读，把维度信息写入缓存中
            writeDimToRedis(redisClient, tableName, id, dim);
            System.out.println("走数据库: " + tableName + "  " + id);
        } else {
            System.out.println("走缓存: " + tableName + "  " + id);
        }
        return dim;
    }

    private static void writeDimToRedis(Jedis redisClient, String tableName, Long id, JSONObject dim) {
        /*
        string
         key:  表名:id
         value:  dim数据, json'格式
        */
        String key = tableName + ":" + id;
        //        redisClient.set(key, dim.toJSONString());
        //        redisClient.expire(key, 2 * 24 * 60 * 60);  // ttl: 2天
        redisClient.setex(key, 2 * 24 * 60 * 60, dim.toJSONString());
    }

    private static JSONObject readDimFromRedis(Jedis redisClient, String tableName, Long id) {
        String key = tableName + ":" + id;
        String dimJson = redisClient.get(key);

        if (dimJson != null) {  // 缓存可能不存在
            return JSON.parseObject(dimJson);
        }
        return null;
    }
}
