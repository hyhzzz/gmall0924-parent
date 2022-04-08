package com.atguigu.gmall.realtime.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author coderhyh
 * @create 2022-04-08 13:26
 */
public class JdbcUtil {

    public static Connection getJdbcConnect(String driver, String url, String user, String password) throws ClassNotFoundException, SQLException {
        //获取jdbc连接
        //1. 加载驱动
        Class.forName(driver);
        //2.获取连接
        return DriverManager.getConnection(url, user, password);
    }
}
