package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import jdk.nashorn.internal.scripts.JS;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取kafka topic_log 主题的数据创建流
        DataStreamSource<String> topic_log = env.addSource(MyKafkaUtil.getKafkaConsumer("topic_log", "Base_Log_App"));

        //3.将数据转换为JSON格式，并过滤掉非JSON格式的数据
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonDS = topic_log.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyTag, s);
                }
            }
        });
        DataStream<String> sideOutput = jsonDS.getSideOutput(dirtyTag);
        sideOutput.print("dirty>>>>>>>>>>>>>>>>>>>>>>");

        //4.使用状态编程做新老用户校验
        KeyedStream<JSONObject, String> keyBy = jsonDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyBy.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("state", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {

                String is_new = jsonObject.getJSONObject("common").getString("is_new");
                String stateTs = valueState.value();
                Long ts = jsonObject.getLong("ts");

                if ("1".equals(is_new)) {

                    String curTs = DateFormatUtil.toDate(ts);
                    if (stateTs == null) {
                        valueState.update(curTs);
                    } else if (!stateTs.equals(curTs)) {
                        jsonObject.getJSONObject("common").put("is_new", "0");
                    }
                } else if (stateTs == null) {
                    String yesterday = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000);
                    valueState.update(yesterday);
                }
                return jsonObject;
            }
        });

        //5.使用测输出流对数据进行分流处理
        OutputTag<String> startTag = new OutputTag<String>("start") {};
        OutputTag<String> displayTag = new OutputTag<String>("display") {};
        OutputTag<String> actionTag = new OutputTag<String>("action") {};
        OutputTag<String> errorTag = new OutputTag<String>("error") {};

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {

                String value = jsonObject.toJSONString();

                String error = jsonObject.getString("err");
                if (error != null) {
                    context.output(errorTag, value);
                }

                String start = jsonObject.getString("start");
                if (start != null) {
                    context.output(startTag, value);
                } else {
                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                    Long ts = jsonObject.getLong("ts");
                    String common = jsonObject.getString("common");

                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            display.put("common", common);

                            context.output(displayTag, display.toJSONString());
                        }
                    }

                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("page_id", pageId);
                            action.put("ts", ts);


                            context.output(actionTag, action.toJSONString());
                        }
                    }

                    jsonObject.remove("displays");
                    jsonObject.remove("actions");
                    collector.collect(jsonObject.toJSONString());
                }

            }
        });


        //6.提取各个测输出流的数据
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        //7.将各个流的数据分别写出到Kafka对应的主题中
        pageDS.print("Page>>>>>>>>>");
        startDS.print("Start>>>>>>>>>");
        errorDS.print("Error>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>");
        actionDS.print("Action>>>>>>>>>>>");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.addSink(MyKafkaUtil.getKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getKafkaProducer(start_topic));
        errorDS.addSink(MyKafkaUtil.getKafkaProducer(error_topic));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getKafkaProducer(action_topic));

        //8.启动
        env.execute("BaseLogApp");
    }
}
