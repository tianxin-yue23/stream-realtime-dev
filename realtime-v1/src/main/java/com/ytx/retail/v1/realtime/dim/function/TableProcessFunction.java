package com.ytx.retail.v1.realtime.dim.function;


import com.alibaba.fastjson.JSONObject;
import com.ytx.retail.v1.realtime.common.bean.TableProcessDim;
import com.ytx.retail.v1.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    private MapStateDescriptor<String,TableProcessDim> mapStateDescriptor;
    private Map<String, TableProcessDim> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    //            @Override
    public void open(Configuration parameters) throws Exception {
//        Class.forName("com.mysql.cj.jdbc.Driver");
////               建立连接
//        java.sql.Connection conn= DriverManager.getConnection(Constant.MYSQL_URL,Constant.MYSQL_USER_NAME,Constant.MYSQL_PASSWORD);
//        String sql="select * from realtime_v1_config.table_process_dim";
//        PreparedStatement ps = conn.prepareStatement(sql);
////                执行sql语句
//        ResultSet rs = ps.executeQuery();
//        ResultSetMetaData metaData = rs.getMetaData();
////             处理结果集
//        while (rs.next()){
//            JSONObject jsonObj = new JSONObject();
//            for (int i = 1; i < metaData.getColumnCount(); i++) {
//                String catalogName = metaData.getCatalogName(i);
//                Object columnValue = rs.getObject(i);
//                jsonObj.put(catalogName,columnValue);
//            }
//            TableProcessDim tableProcessDim = jsonObj.toJavaObject(TableProcessDim.class);
//            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
//        }
//        rs.close();
//        ps.close();
//        conn.close();

        Connection mySQLConnection = JdbcUtil.getMySQLConnection();
        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(mySQLConnection, "select * from realtime_v1_config.table_process_dim", TableProcessDim.class);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeMySQLConnection(mySQLConnection);

    }

    //            主流数据
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject,TableProcessDim>> collector) throws Exception {
//        广播状态中获取当前处理数据对应的表配置信息
        ReadOnlyBroadcastState<String, TableProcessDim> state = readOnlyContext.getBroadcastState(mapStateDescriptor);
        // . 从输入数据中提取表名
        String table = jsonObject.getJSONObject("source").getString("table");
//        //  根据表名查找对应的配置信息
        TableProcessDim tableProcessDim = state.get(table);
//        // 如果配置存在，则处理数据并输出
        if (tableProcessDim != null) {
//             // 提取变更后的数据
            JSONObject after = jsonObject.getJSONObject("after");
            String op = jsonObject.getString("op");
            after.put("op",op);
            collector.collect(Tuple2.of(after,tableProcessDim));

        }
    }        //广播流
    @Override
    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        String op = tp.getOp();
//                获取广播状态
        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
//              维度表名称
        String sourceTable = tp.getSourceTable();
        //  根据操作类型处理配置变更
        if ("d".equals(op)) {
//             // 如果是删除操作，从广播状态和本地缓存中移除配置
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
//             // 如果是新增/更新操作，更新广播状态和本地缓存
            broadcastState.put(sourceTable, tp);
            configMap.put(sourceTable, tp);
        }
    }
}

