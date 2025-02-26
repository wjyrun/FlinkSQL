package com.cbxg.sql.connector.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author:cbxg
 * @date:2021/4/5
 * @description: table api file connector
 */
public class TableFileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("vc", DataTypes.INT());
        tableEnv.executeSql("CREATE TEMPORARY  TABLE sensor (\n" +
                "    id  STRING,\n" +
                "    ts  BIGINT,\n" +
                "    vc     INT\n" +
                ") WITH (\n" +
                "    'connector' = 'filesystem',\n" +
                "    'path'     = 'E:\\local_github_repository\\FlinkSQL\\src\\main\\resources\\sensor.csv',\n" +
                "    'format'    = 'csv'\n" +
                ")");
//        tableEnv.connect(new FileSystem().path("E:\\local_github_repository\\FlinkSQL\\src\\main\\resources\\sensor.csv"))
//                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
//                .withSchema(schema)
//                .createTemporaryTable("sensor");

        final Table sensor = tableEnv.from("sensor");
        final Table id = sensor.select($("id"),$("ts"),$("vc"));
        final DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(id, Row.class);
        tuple2DataStream.print();

        env.execute("FileConnector");
    }
}
