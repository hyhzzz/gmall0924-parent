package com.atguigu.gmall.realtime.app.dwm;

import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.common.Constant;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author coderhyh
 * @create 2022-04-09 10:42
 */
public class DwmUserJumpDetailApp extends BaseAppV1 {
    public static void main(String[] args) {

        //环境初始化
        new DwmUvApp().init(
                2004,
                4,
                "DwmUserJumpDetailApp",
                "DwmUserJumpDetailApp", Constant.TOPIC_DWD_PAGE
        );
    }

    //写具体的业务逻辑
    @Override
    protected void run(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        /*stream =
            env.fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"home\"},\"ts\":39999} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"detail\"},\"ts\":50000} "
            );*/

        // 1. 数据封装, 添加水印, 按照mid分组


        // 2. 定义模式



        // 3. 把模式应用到流上


        // 4. 获取匹配到的结果: 提前超时数据


        // 5. 把侧输出流的结果写入到dwm_user_jump_detail


    }
}
