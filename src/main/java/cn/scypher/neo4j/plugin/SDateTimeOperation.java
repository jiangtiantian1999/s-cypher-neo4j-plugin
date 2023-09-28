package cn.scypher.neo4j.plugin;

import cn.scypher.neo4j.plugin.datetime.TimePoint;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.time.*;
import java.util.HashMap;
import java.util.Map;

public class SDateTimeOperation {

    @Context
    public Transaction tx;

    /**
     * @param intervalFromObject
     * @param intervalToObject
     * @return 返回Map类型（具有两个key：from和to，value为时间点类型）
     */
    @UserFunction("scypher.interval")
    public Object interval(@Name("intervalFrom") Object intervalFromObject, @Name("intervalFrom") Object intervalToObject) {
        if (intervalFromObject != null && intervalToObject != null) {
            Map<String, Object> interval = new HashMap<>();
            GlobalVariableManage globalVariableManage = new GlobalVariableManage(this.tx);
            String timeGranularity = globalVariableManage.getTimeGranularity();
            String timezone = globalVariableManage.getTimeZone();
            TimePoint intervalFrom;
            if (intervalFromObject instanceof String | intervalFromObject instanceof Map) {
                intervalFrom = new TimePoint(intervalFromObject, timeGranularity, timezone);
            } else {
                intervalFrom = new TimePoint(intervalFromObject);
            }
            interval.put("from", intervalFrom);
            TimePoint intervalTo;
            if (intervalToObject instanceof String | intervalToObject instanceof Map) {
                intervalTo = new TimePoint(intervalToObject, timeGranularity, timezone);
            } else {
                intervalTo = new TimePoint(intervalToObject);
            }
            interval.put("to", intervalTo);
            return interval;
        } else {
            throw new RuntimeException("Missing parameter intervalFrom or intervalTo.");
        }
    }


}
