package com.cbxg.sql.connector.table;

import com.cbxg.sql.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author:cbxg
 * @date:2021/4/6
 * @description:
 */
public class TableKafkaSink {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<WaterSensor> streamSource = env.fromElements(
                new WaterSensor("sensor_01", 100000L, 100),
                new WaterSensor("sensor_02", 100000L, 100),
                new WaterSensor("sensor_01", 100000L, 120),
                new WaterSensor("sensor_03", 100000L, 110)
        );

        Table table = tableEnv.fromDataStream(streamSource);

        Table select = table.where($("id").isEqual("sensor_01"))
                .select($("id"), $("ts"), $("vc"));
//        tableEnv.connect(
//                new Kafka()
//                        .version("universal")
//                        .topic("transactions")
//                        .property("bootstrap.servers","10.168.100.15:9092"))
//                .withFormat(new Json())
//                .withSchema(
//                        new Schema()
//                                .field("id", DataTypes.STRING())
//                                .field("ts", DataTypes.BIGINT())
//                                .field("vc", DataTypes.INT()))
//                .createTemporaryTable("sensor");

        tableEnv.executeSql("CREATE TABLE sensor (\n" +
                "  `id` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `vc` INT\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'transactions',\n" +
                "  'properties.bootstrap.servers' = '10.168.100.15:9092',\n" +
                "  'properties.group.id' = 'test_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");


        select.executeInsert("sensor");

//        env.execute("TableKafkaSink");
    }
}
