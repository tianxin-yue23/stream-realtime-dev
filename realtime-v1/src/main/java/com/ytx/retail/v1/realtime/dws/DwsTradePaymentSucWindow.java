package com.ytx.retail.v1.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ytx.retail.v1.realtime.common.base.BaseApp;
import com.ytx.retail.v1.realtime.common.bean.TradePaymentBean;
import com.ytx.retail.v1.realtime.common.constant.Constant;
import com.ytx.retail.v1.realtime.common.function.BeanToJsonStrMapFunction;
import com.ytx.retail.v1.realtime.common.util.DateFormatUtil;
import com.ytx.retail.v1.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DwsTradePaymentSucWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradePaymentSucWindow().start(
                10027,
                4,
                "dws_trade_payment_suc_window",
                Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (s != null) {
                    JSONObject jsonObject = JSON.parseObject(s);
                    out.collect(jsonObject);
                }
            }
        });
//        jsonObjDs.print();
        //        按照用户的id进行分组
        KeyedStream<JSONObject, String> keyedDs = jsonObjDs.keyBy(jsonObject -> jsonObject.getString("user_id"));
        SingleOutputStreamOperator<TradePaymentBean> beanDs = keyedDs.process(new KeyedProcessFunction<String, JSONObject, TradePaymentBean>() {
            private ValueState<String> lastPayDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastPayDateState =
                        getRuntimeContext().getState(new ValueStateDescriptor<String>("lastPayDateState", String.class));
            }

            public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TradePaymentBean>.Context ctx, Collector<TradePaymentBean> out) throws Exception {
                String lastPayDate = lastPayDateState.value();
//                获取当前这次加购日期
                long ts = jsonObj.getLong("ts_ms");
                String curCartDate = DateFormatUtil.tsToDate(ts);
                long payUuCount = 0L;
                long payNewCount = 0L;


                if (!curCartDate.equals(lastPayDate)) {  // 今天第一次支付成功
                    lastPayDateState.update(curCartDate);
                    payUuCount = 1L;

                    if (lastPayDate == null) {
                        // 表示这个用户曾经没有支付过, 是一个新用户支付
                        payNewCount = 1L;
                    }
                }

                if (payUuCount == 1) {
                    out.collect(new TradePaymentBean(
                            "",
                            "",
                            "",
                            payUuCount,
                            payNewCount,
                            ts));
                }
            }
        });
        //水位线
        SingleOutputStreamOperator<TradePaymentBean> withWatermarkD = beanDs.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TradePaymentBean>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<TradePaymentBean>() {
                    @Override
                    public long extractTimestamp(TradePaymentBean jsonObj, long l) {
                        return jsonObj.getTs();
                    }
                }));
        AllWindowedStream<TradePaymentBean, TimeWindow> windowDS = withWatermarkD.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<TradePaymentBean> reduceDs = windowDS.reduce(new ReduceFunction<TradePaymentBean>() {
            @Override
            public TradePaymentBean reduce(TradePaymentBean value1, TradePaymentBean value2) throws Exception {
                value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                return value1;

            }
        }, new AllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<TradePaymentBean> values, Collector<TradePaymentBean> out) throws Exception {
                TradePaymentBean viewBean = values.iterator().next();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String curDate = DateFormatUtil.tsToDate(window.getStart());
                viewBean.setStt(stt);
                viewBean.setEdt(edt);
                viewBean.setCurDate(curDate);
                out.collect(viewBean);
            }
        });
        reduceDs.print();
//        reduceDs.map(new BeanToJsonStrMapFunction<>())
//                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_payment_suc_window"));
    }
}
