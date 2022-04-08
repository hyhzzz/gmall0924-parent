package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author coderhyh
 * @create 2022-04-07 16:13
 */
public class DwdDbApp extends BaseAppV1 {
    public static void main(String[] args) {

        //环境初始化
        new DwdDbApp().init(
                2002,
                4,
                "DwdDbApp",
                "DwdDbApp",
                Constant.TOPIC_ODS_DB
        );
    }

    //写具体的业务逻辑
    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {

        //        stream.print();
        //1.对业务数据做etl数据清洗
        SingleOutputStreamOperator<JSONObject> etldStream = etl(stream);
        //        etldStream.print();

        //2. 读取配置表的数据,使用cdc把数据做成流
        SingleOutputStreamOperator<TableProcess> processTableStream = readTableProcess(env);
        //        processTableStream.print();

        //3. 数据流和配置流进行connect，根据配置表数据进行动态分流
        connectStreams(etldStream, processTableStream);

        //4.不同的数据写到不同的sink


    }

    /**
     * 对数据流进行动态分流
     *
     * @param etldStream         业务数据流
     * @param processTableStream 动态配置流
     */
    private void connectStreams(SingleOutputStreamOperator<JSONObject> dbStream,
                                SingleOutputStreamOperator<TableProcess> processTableStream) {

        //1. 配置流做成广播流

        //2. 数据流去connect广播流

        //3.进行处理


    }

    /**
     * 读取的配置表的数据
     *
     * @param env
     * @return
     */
    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        //创建表的执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //配置表在mysql数据库中,使用flink-sql-cdc来实时监控配置表数据
        tEnv
                .executeSql("CREATE TABLE `table_process`( " +
                        "   `source_table`  string, " +
                        "   `operate_type`  string, " +
                        "   `sink_type`  string, " +
                        "   `sink_table`  string, " +
                        "   `sink_columns` string, " +
                        "   `sink_pk`  string, " +
                        "   `sink_extend`  string, " +
                        "   PRIMARY KEY (`source_table`,`operate_type`)  NOT ENFORCED" +
                        ")with(" +
                        "   'connector' = 'mysql-cdc', " +
                        "   'hostname' = 'hadoop102', " +
                        "   'port' = '3306', " +
                        "   'username' = 'root', " +
                        "   'password' = '123456', " +
                        "   'database-name' = 'gmall2022_realtime', " +
                        "   'table-name' = 'table_process'," +
                        "   'debezium.snapshot.mode' = 'initial' " +  // 读取mysql的全量,增量以及更新数据
                        ")");

     /*   tEnv.sqlQuery("select " +
                "  source_table sourceTable, " +
                "  sink_type sinkType, " +
                "  operate_type operateType, " +
                "  sink_table sinkTable, " +
                "  sink_columns sinkColumns, " +
                "  sink_pk sinkPk, " +
                "  sink_extend sinkExtend " +
                "from table_process ").execute().print();*/

        final Table table = tEnv.sqlQuery("select " +
                "  source_table sourceTable, " +
                "  sink_type sinkType, " +
                "  operate_type operateType, " +
                "  sink_table sinkTable, " +
                "  sink_columns sinkColumns, " +
                "  sink_pk sinkPk, " +
                "  sink_extend sinkExtend " +
                "from table_process ");

        return tEnv
                //把动态表转换成流
                .toRetractStream(table, TableProcess.class)
                .filter(t -> t.f0)
                .map(t -> t.f1);

    }

    /**
     * 对db数据进行etl
     *
     * @param stream
     * @return
     */
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {

        //转换为json对象  string->jsonobj
        // 根据实际业务, 对数据做一些过滤
        return stream.map(data -> JSON.parseObject(data.replaceAll("bootstrap-", "")))
                .filter(obj ->
                        obj.getString("database") != null
                                && obj.getString("database").equals("gmall2022")
                                && obj.getString("table") != null
                                && ("insert".equals(obj.getString("type")) || "update".equals(obj.getString("type")))
                                && obj.getString("data") != null
                                && obj.getString("data").length() > 3
                );
    }
}
