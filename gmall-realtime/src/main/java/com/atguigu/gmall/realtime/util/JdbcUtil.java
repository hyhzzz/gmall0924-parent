package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.common.Constant;

import org.apache.commons.beanutils.BeanUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-08 13:26
 */
public class JdbcUtil {
    public static Connection getJdbcConnect(String driver,
                                            String url,
                                            String user,
                                            String password) throws Exception {
        // 获取jdbc连接, 步骤
        // 1. 加载驱动
        Class.forName(driver);
        // 2. 获取连接
        return DriverManager.getConnection(url, user, password);
    }

    public static <T> List<T> queryList(Connection conn,
                                        String sql,
                                        Object[] args,
                                        Class<T> tClass) throws Exception {
        ArrayList<T> result = new ArrayList<>();

        PreparedStatement ps = conn.prepareStatement(sql);
        // 给sql中的占位符进行赋值
        // select id as id_a,  form t where a=? and b=?;
        for (int i = 0; args != null && i < args.length; i++) {
            // 遍历每个参数, 给占位符赋值
            Object arg = args[i];
            ps.setObject(i + 1, arg);
        }

        ResultSet resultSet = ps.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();  // 获取这个结果集的元数据信息
        // next方法用来判断结果集中的数据, 如果有就把指针移懂到这一行
        while (resultSet.next()) {
            //循环每进来一次, 相当于遍历到了结果中的一行
            // 把这一行数据中的所有列全部取出, 封装到一个T类型的对象只
            T t = tClass.newInstance();  // 中T类型的无参构造器创建一个对象, 把这一行的数据封装到这个对象中, 每一列就是对象中的一个属性
            int columnsCount = metaData.getColumnCount();
            for (int i = 1; i <= columnsCount; i++) { // 列的索引从一开始
                // 获取列名就是 T对象的属性名
                // 获取列的值就是对应的 属性的值
                String columnName = metaData.getColumnLabel(i);
                Object columnValue = resultSet.getObject(i);
                BeanUtils.setProperty(t, columnName, columnValue);
            }
            result.add(t);
        }

        ps.close();
        return result;
    }

    public static void main(String[] args) throws Exception {
        // 先查mysql
        //        Connection conn = getJdbcConnect("com.mysql.jdbc.Driver", "jdbc:mysql://hadoop102:3306/gmall2022", "root", "123456");
        Connection conn = getJdbcConnect(Constant.PHOENIX_DRIVER, Constant.PHOENIX_URL, null, null);

        /*List<JSONObject> list = queryList(conn, "select * from order_info", null, JSONObject.class);
        for (JSONObject obj : list) {

            System.out.println(obj);
        }*/

        /*List<OrderInfo> list = queryList(conn, "select * from order_info", null, OrderInfo.class);
        for (OrderInfo orderInfo : list) {
            System.out.println(orderInfo);
        }*/

        List<JSONObject> list = queryList(conn, "select * from dim_sku_info where id=?", new Object[]{"1"}, JSONObject.class);
        for (JSONObject jsonObject : list) {
            System.out.println(jsonObject);
        }
    }
}
