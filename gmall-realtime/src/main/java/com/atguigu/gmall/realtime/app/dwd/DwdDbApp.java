package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;
import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-07 16:13
 */
public class DwdDbApp extends BaseAppV1 {

    //可以放在外面 但是得实现序列化Serializable
    //    MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);

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
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStream = connectStreams(etldStream, processTableStream);
        //        connectedStream.print();


        //4.过滤掉不需要的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterColumnsStream = filterColumns(connectedStream);
        //        filterColumnsStream.print();

        //5.动态分流
        Tuple2<SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>>, DataStream<Tuple2<JSONObject, TableProcess>>> kafkaHbaseStreams = dynamicSplitStream(filterColumnsStream);

        kafkaHbaseStreams.f0.print("kafka");
        kafkaHbaseStreams.f1.print("hbase");

        //6.不同的数据写到不同的sink
        writeToKakfa(kafkaHbaseStreams.f0);
        writeToHbase(kafkaHbaseStreams.f1);


    }

    /**
     * 维度表数据写入到Hbase
     *
     * @param stream
     */
    private void writeToHbase(DataStream<Tuple2<JSONObject, TableProcess>> stream) {

         /*
         维度表的数据写入到HBase中, 通过phoenix写入
         1. phoenix的表不会自动创建, 需要我们创建
            a: 手动提前创建
               优点: 比较简单, 代码就不用考虑建表
               缺点: 不够灵活

            b: 通过代码自动创建:当某个维度表的第一条进来的时候, 执行一个sql, 创建这个维度表
                优点: 可以自动适应配置变化, 比较灵活.
                缺点: 代码实现起来比较复杂


        2. 采用 b 种方式
            1. 能不能 使用flink提供的jdbc sink

                这次想phoenix写数据的时候, 需要几个sql语句?
                    2个sql: 建表   插入数据的

                    jdbc sink 只能使用一个sql(插入数据集的sql), 所以不能使用jdbc sink
             2. 使用自定义sink
                 使用jdbc来建表和插入数据
         */

        stream
                //为了后面试用键控状态, 这里必须keyBy
                .keyBy(t -> t.f1.getSinkTable())
                .addSink(FlinkSinkUtil.getPhoenixSink());
    }

    /**
     * 事实数据写回到kafka中
     *
     * @param stream
     */
    private void writeToKakfa(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {

        stream
                .addSink(FlinkSinkUtil.getKafkaSink());

    }

    /**
     * 动态分流
     *
     * @param filterColumnsStream
     * @return
     */
    private Tuple2<SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>>, DataStream<Tuple2<JSONObject, TableProcess>>> dynamicSplitStream(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterColumnsStream) {

        OutputTag<Tuple2<JSONObject, TableProcess>> hbaseTag = new OutputTag<Tuple2<JSONObject, TableProcess>>("hbase") {
        };

        //数据一共两个sink 分为两个流 ：主流kafka(事实表) 和 侧输出流hbase(phoenix)(维度表)
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> kafkaStream = filterColumnsStream.process(new ProcessFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
            @Override
            public void processElement(Tuple2<JSONObject, TableProcess> value,
                                       ProcessFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>.Context ctx,
                                       Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {

                String sinkType = value.f1.getSinkType();
                if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                    out.collect(value);
                } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                    ctx.output(hbaseTag, value);
                }
            }
        });
        DataStream<Tuple2<JSONObject, TableProcess>> hbaseStream = kafkaStream.getSideOutput(hbaseTag);
        return Tuple2.of(kafkaStream, hbaseStream);

    }

    /**
     * 把数据中不需要的字段过滤掉
     *
     * @param connectedStream
     * @return
     */
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStream) {

        return connectedStream.map(
                new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> value) throws Exception {

                        JSONObject data = value.f0;
                        //存储了所有需要sink的列
                        List<String> columns = Arrays.asList(value.f1.getSinkColumns().split(","));

                        //遍历data中的每一个列名，如果存在于columns这个集合中，则保留，否则删除
                        //如何删除map中的key
                        data.keySet().removeIf(key -> !columns.contains(key));

                        return value;
                    }
                }
        );
    }

    /**
     * 数据流和配置流进行connect
     *
     * @param etldStream         业务数据流
     * @param processTableStream 动态配置流
     * @return
     */
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectStreams(SingleOutputStreamOperator<JSONObject> dbStream,
                                                                                        SingleOutputStreamOperator<TableProcess> processTableStream) {

        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);

        //1. 把配置流做成广播流
        //key 字符串类型 ：表名+操作类型
        BroadcastStream<TableProcess> bcStream = processTableStream.broadcast(tpStateDesc);

        //2. 数据流去connect广播流
        return dbStream.connect(bcStream)
                //3.进行处理
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {

                    //处理业务流中的数据
                    @Override
                    public void processElement(JSONObject value,
                                               BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>.ReadOnlyContext ctx,
                                               Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        //根据表名：操作类型 从广播状态中获取对应的配置信息。
                        ReadOnlyBroadcastState<String, TableProcess> tpState = ctx.getBroadcastState(tpStateDesc);

                        //拼接出key
                        String key = value.getString("table") + ":" + value.getString("type");
                        TableProcess tp = tpState.get(key);
                        //有些表在配置信息中并没有，表示这张表的数据不需要sink
                        //如果这张表的配置不存在，tp对象就是null
                        if (tp != null) {
                            //每条数据的元数据在tp中基本都有体现，所以数据中的元数据信息，可以去掉，只保留data字段中的数据
                            out.collect(Tuple2.of(value.getJSONObject("data"), tp));
                        }
                    }

                    //处理广播流中的数据
                    @Override
                    public void processBroadcastElement(TableProcess value,
                                                        BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>.Context ctx,
                                                        Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        //把流中的每个配置写入到广播状态中
                        BroadcastState<String, TableProcess> tpState = ctx.getBroadcastState(tpStateDesc);

                        String key = value.getSourceTable() + ":" + value.getOperateType();
                        tpState.put(key, value);
                    }

                });
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
