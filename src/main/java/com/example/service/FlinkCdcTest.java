package com.example.service;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

/**
 * Description: Flink-CDC 测试
 * <br/>
 * Date: 2020/9/16 14:03
 *
 * @author ALion
 */
public class FlinkCdcTest {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        // 数据源表
        String sourceDDL =
                "CREATE TABLE mysql_binlog (\n" +
                        " id INT NOT NULL,\n" +
                        " name STRING,\n" +
                        " age INT,\n" +
                        " description STRING\n" +
                        ") WITH (\n" +
                        " 'connector' = 'mysql-cdc',\n" +
                        " 'hostname' = '121.4.56.178',\n" +
                        " 'port' = '4306',\n" +
                        " 'username' = 'flinkuser',\n" +
                        " 'password' = 'flinkpassword',\n" +
                        " 'database-name' = 'demo',\n" +
                        " 'table-name' = 'demo'\n" +
                        ")";
        // 输出目标表
        String printSink =
                "CREATE TABLE print_sink (\n" +
                        " id INT,\n" +
                        " name STRING,\n" +
                        " age INT,\n" +
                        " description STRING,\n" +
                        " PRIMARY KEY (name) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        " 'connector' = 'print'\n" +
                        ")";

        String esSink =
                "CREATE TABLE es_sink (\n" +
                        " id INT,\n" +
                        " name STRING,\n" +
                        " age INT,\n" +
                        " description STRING,\n" +
                        " PRIMARY KEY (name) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        " 'connector' = 'elasticsearch-7'\n" +
                        " 'hosts' = 'http://10.250.112.183:9200'\n" +
                        " 'index' = 'flink_test'\n" +
                        " 'sink.bulk-flush.backoff.strategy' = 'EXPONENTIAL'\n" +
                        ")";

        // 简单的聚合处理
        String transformPrintSQL =
                "INSERT INTO print_sink " +
                        "SELECT id, name, age, description " +
                        "FROM mysql_binlog ";

        String transformEsSQL =
                "INSERT INTO es_sink " +
                        "SELECT id, name, age, description " +
                        "FROM mysql_binlog ";

        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql(printSink);
        //tableEnv.executeSql(esSink);
        TableResult printResult = tableEnv.executeSql(transformPrintSQL);
        //TableResult esResult = tableEnv.executeSql(transformEsSQL);

        // 等待flink-cdc完成快照
        waitForSnapshotStarted("print_sink");
        //waitForSnapshotStarted("ez_sink");
        printResult.print();
        //esResult.print();

        printResult.getJobClient().get().cancel().get();
        //esResult.getJobClient().get().cancel().get();
    }

    private static void waitForSnapshotStarted(String sinkName) throws InterruptedException {
        while (sinkSize(sinkName) == 0) {
            Thread.sleep(100);
        }
    }

    private static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }

}

