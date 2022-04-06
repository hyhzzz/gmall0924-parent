package com.atguigu.gmall.realtime.app.dwd;

import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.common.Constant;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author coderhyh
 * @create 2022-04-06 23:50
 */
public class DwdLogApp extends BaseAppV1 {
    public static void main(String[] args) throws Exception {

        //环境初始化
        new DwdLogApp().init(2001,
                4,
                "DwdLogApp",
                "DwdLogApp",
                Constant.TOPIC_ODS_LOG);

    }

    //写具体的业务逻辑
    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {


//        stream.print();


    }
}
