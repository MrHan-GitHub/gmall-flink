package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

public class LogBaseApp {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //最好为kafka主题的分区数
        env.setParallelism(1);
        //1.1设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        //1.2开启CK
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setAlignmentTimeout(60000L);

        //修改用户名(这里是因为设置了ck，要操作hdfs，所以要设置操作hdfs的用户)
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //2.读取kafka ods_base_log 主题数据
        DataStreamSource<String> kafkaSource = env.addSource(MyKafkaUtils.getKafkaSource("ods_base_log", "dwd_log"));

        //3.将每行数据转化成JsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSource.map(data -> JSONObject.parseObject(data));

        //4.按照Mid进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(value -> value.getJSONObject("common").getString("mid"));

        //5.使用状态编程做新老用户校验
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlagDS = keyedStream.map(new NewMidRichFunc());

        //6.分流，使用ProcessFunction讲ODS层数据拆分成启动、曝光以及页面数据
        SingleOutputStreamOperator<String> pageDS = jsonWithNewFlagDS.process(new SplitProcessFunc());
        DataStream<String> startDS = pageDS.getSideOutput(new OutputTag<String>("start") {
        });
        DataStream<String> displayDS = pageDS.getSideOutput(new OutputTag<String>("display") {
        });

        //7.将三个流写进kafka
        pageDS.addSink(MyKafkaUtils.getKafkaSink("dwd_page_log"));
        startDS.addSink(MyKafkaUtils.getKafkaSink("dwd_start_log"));
        displayDS.addSink(MyKafkaUtils.getKafkaSink("dwd_display_log"));

        //打印测试
        //jsonWithNewFlagDS.print();
        //pageDS.print("Page>>>>>>>>>>");
        //startDS.print("Start>>>>>>>>>>>>>>>");
        //displayDS.print("Display>>>>>>>>>>>>>");

        //8.执行任务
        env.execute();


    }

    public static class NewMidRichFunc extends RichMapFunction<JSONObject,JSONObject>{

        //声明状态用于表示当前Mid是否已经访问过
        private ValueState<String> firstVisitDateState;
        private SimpleDateFormat simpleFormatter;

        @Override
        public void open(Configuration parameters) throws Exception {
            firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("new-mid",String.class));
            simpleFormatter = new SimpleDateFormat("yyyy-MM-dd");
        }

        @Override
        public JSONObject map(JSONObject value) throws Exception {
            //取出新用户标记
            String isNew = value.getJSONObject("common").getString("is_new");

            //如果当前前端传输数据表示新用户，则进行校验
            if ("1".equals(isNew)) {
                //取出当前状态数据并取出当前访问时间
                String firstDate = firstVisitDateState.value();
                Long ts = value.getLong("ts");

                //判断状态数据是否为null
                if (firstDate != null) {
                    //修复
                    value.getJSONObject("common").put("is_new","0");
                } else {
                    //更新状态
                    firstVisitDateState.update(simpleFormatter.format(ts));
                }
            }
            //返回数据
            return value;
        }
    }

    public static class SplitProcessFunc extends ProcessFunction<JSONObject,String>{
        @Override
        public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
            //提取"start"字段
            String startStr = jsonObject.getString("start");

            //判断是否为启动数据
            if (startStr != null && startStr.length() > 0) {
                //将启动日志写入测输出流
                context.output(new OutputTag<String>("start"){},jsonObject.toJSONString());
            } else {
                //不是启动数据，继续判断是否为曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    //为曝光数据，遍历写入测输出流
                    for (int i = 0; i < displays.size(); i++) {
                        //取出单条曝光数据
                        JSONObject displayJson = displays.getJSONObject(i);
                        //添加页面id
                        displayJson.put("page_id",jsonObject.getJSONObject("page").getString("page_id"));
                        //输出到测输出流
                        context.output(new OutputTag<String>("display"){},displayJson.toString());
                    }
                } else {
                    //为页面数据，将数据输出到主流
                    collector.collect(jsonObject.toString());
                }
            }
        }
    }

}
