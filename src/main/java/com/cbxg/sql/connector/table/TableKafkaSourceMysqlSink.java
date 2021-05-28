package com.cbxg.sql.connector.table;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author:cbxg
 * @date:2021/4/5
 * @description: table api kafka connector
 */
public class TableKafkaSourceMysqlSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE sensor (\n" +
                "    id  STRING,\n" +
                "    ts      BIGINT,\n" +
                "    vc INT\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'transactions',\n" +
                "    'properties.bootstrap.servers' = '10.168.100.15:9092,10.168.100.16:9092,10.168.100.17:9092',\n" +
                "    'format'    = 'json',\n" +
                "    'scan.startup.mode' = 'latest-offset'\n" +
                ")");

        Table sensor = tableEnv.from("sensor");
        Table select = sensor.where($("id").isEqual("sensor_01"))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("sum_vc"))
                .select($("id"), $("sum_vc"));

        tableEnv.executeSql("CREATE TABLE sensor1 (\n" +
                "    id STRING,\n" +
                "    sum_vc     INT,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n"       +
                ") WITH (\n" +
                "  'connector'  = 'jdbc',\n" +
                "  'url'        = 'jdbc:mysql://localhost:3306/world',\n" +
                "  'table-name' = 'spend_report',\n" +
                "  'username'   = 'root',\n" +
                "  'password'   = 'haier@2021'\n" +
                ")");

        select.executeInsert("sensor1");

    }
}
