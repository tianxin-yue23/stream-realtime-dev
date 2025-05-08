package com.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.houbb.sensitive.word.core.SensitiveWordHelper;
import com.retailersv1.func.FilterBloomDeduplicatorFunc;
import com.retailersv1.func.MapCheckRedisSensitiveWordsFunc;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.ytx.retail.v1.realtime.common.constant.Constant;
import com.ytx.retail.v1.realtime.common.function.BeanToJsonStrMapFunction;
import com.ytx.retail.v1.realtime.common.util.FlinkSinkUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;
import java.util.List;

public class DbusBanBlackListUserInfo2Kafka {

//    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
//    private static final String kafka_db_fact_comment_topic = ConfigUtils.getString("kafka.db.fact.comment.topic");
//    private static final String kafka_result_sensitive_words_topic = ConfigUtils.getString("kafka.result.sensitive.words.topic");

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettingUtils.defaultParameter(env);

        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        Constant.KAFKA_BROKERS,
                        Constant.TOPICOMMONT,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "kafka_cdc_db_source"
        ).uid("kafka_fact_comment_source").name("kafka_fact_comment_source");

//    kafkaCdcDbSource.print();
//    JSON转换
      SingleOutputStreamOperator<JSONObject> mapJsonStr = kafkaCdcDbSource.map(JSON::parseObject).uid("to_json_string").name("to_json_string");
//      mapJsonStr.print();
//        布隆过滤器去重
//         按order_id进行分组，使用布隆过滤器过滤重复数据
        SingleOutputStreamOperator<JSONObject> bloomFilterDs = mapJsonStr.keyBy(data -> data.getLong("order_id"))
                .filter(new FilterBloomDeduplicatorFunc(1000000, 0.01));
//      bloomFilterDs.print();
//        //  检查敏感词
//        // 使用Redis中存储的敏感词进行过滤
        SingleOutputStreamOperator<JSONObject> SensitiveWordsDs = bloomFilterDs.map(new MapCheckRedisSensitiveWordsFunc())
                .uid("MapCheckRedisSensitiveWord")
                .name("MapCheckRedisSensitiveWord");
//       SensitiveWordsDs.print();

      SingleOutputStreamOperator<JSONObject> secondCheckMap = SensitiveWordsDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                if (jsonObject.getIntValue("is_violation") == 0) {
                    String msg = jsonObject.getString("msg");
                    List<String> msgSen = SensitiveWordHelper.findAll(msg);
                    if (msgSen.size() > 0) {
                        jsonObject.put("violation_grade", "P1");
                        jsonObject.put("violation_msg", String.join(", ", msgSen));
                    }
                }
                return jsonObject;
            }
        }).uid("second sensitive word check").name("second sensitive word check");
//        secondCheckMap.print();

     secondCheckMap.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("Ban_blacklist"));

       /* secondCheckMap.map(data -> data.toJSONString())
                        .sinkTo(
                                KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_result_sensitive_words_topic)
                        )
                        .uid("sink to kafka result sensitive words topic")
                        .name("sink to kafka result sensitive words topic");
        */


        env.execute();
    }
}
