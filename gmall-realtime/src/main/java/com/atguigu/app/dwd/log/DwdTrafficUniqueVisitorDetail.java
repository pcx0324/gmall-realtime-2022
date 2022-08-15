package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取kafka dwd_traffic_page_log 主题数据创建流
        String topic ="dwd_traffic_page_log";
        String group_id = "Dwd_Traffic_Unique_Visitor_Detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, group_id));

        //3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(JSON::parseObject);

        //4.过滤掉上一跳页面id不等于null的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDS.filter(json -> json.getJSONObject("page").getString("last_page_id") == null);

        //5.按照mid分组
        KeyedStream<JSONObject, String> keyedByMidStream = filterDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //6.使用状态编程进行每日登录数据去重
        SingleOutputStreamOperator<JSONObject> uvDetailDS = keyedByMidStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("visit-state", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(ttlConfig);
                state = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String dt = state.value();
                String curTs = DateFormatUtil.toDate(jsonObject.getLong("ts"));

                if (dt == null || !dt.equals(curTs)) {
                    state.update(curTs);
                    return true;
                } else {
                    return false;
                }

            }
        });

        //7.将数据写入到kafka
        uvDetailDS.print(">>>>>>>>>>>>>>>>>>>");
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        uvDetailDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaProducer(targetTopic));

        //8.启动任务
        env.execute();
    }
}
