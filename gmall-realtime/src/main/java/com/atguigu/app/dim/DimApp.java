package com.atguigu.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DimApp {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取kafka topic_db 主题数据创建流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer("topic_db", "dim_app_211027"));

        //3.过滤掉非JSON格式的数据，并将其写入测输出流
        OutputTag<String> dirtyDataTag = new OutputTag<String>("dirty") {};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyDataTag, s);
                }
            }
        });

        DataStream<String> sideOutput = jsonObjDS.getSideOutput(dirtyDataTag);
        sideOutput.print("dirty>>>>>>>>>>>>>");


        //4.使用FlinkCDC读取Mysql中的配置信息
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.10.100")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String>  mysqlSourceDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mySqlSource");

        //5.将配置信息流处理成广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlSourceDS.broadcast(mapStateDescriptor);


        //6.连接主流与广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);


        //7.根据广播流数据处理主流数据
        SingleOutputStreamOperator<JSONObject> hbaseDS = connectedStream.process(new TableProcessFunction(mapStateDescriptor));

        //8.将数据写出到Phoenix中
        hbaseDS.print(">>>>>>>>>>>>>>>>>>");
        hbaseDS.addSink(new DimSinkFunction());

        //9.启动任务
        env.execute();
    }
}
