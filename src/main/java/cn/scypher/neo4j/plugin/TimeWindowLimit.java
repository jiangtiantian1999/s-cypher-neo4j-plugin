package cn.scypher.neo4j.plugin;


import cn.scypher.neo4j.plugin.datetime.TimePoint;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import java.util.Map;

public class TimeWindowLimit {
    @Context
    public Transaction tx;

    /**
     * @param timePoint 为时间点类型
     */
    @Procedure(name = "scypher.snapshot", mode = Mode.WRITE)
    @Description("SNAPSHOT operation.")
    public void snapshot(@Name("timePoint") Object timePoint) {
        if (timePoint != null) {
            String timePointType = TimePoint.getTimePointType(timePoint.getClass().toString());
            if (timePointType != null) {
                GlobalVariableManage globalVariableManage = new GlobalVariableManage(this.tx);
                String timeGranularity = globalVariableManage.getTimeGranularity();
                if (timeGranularity.equals(timePointType)) {
                    globalVariableManage.setProperty("snapshot", timePoint);
                } else {
                    throw new RuntimeException("The time granularity can't match.The time granularity of database is '" + timeGranularity + "'.");
                }
            } else {
                throw new RuntimeException("Invalid call signature for SnapshotFunction: Provided input was " + timePoint.getClass() + ".");
            }
        } else {
            throw new RuntimeException("Missing parameter timePoint.");
        }
    }

    /**
     * @param interval 为Map类型（具有两个key：from和to，value为时间点类型）
     */
    @Procedure(name = "scypher.scope", mode = Mode.WRITE)
    @Description("SCOPE operation.")
    public void scope(@Name("interval") Map<String, Object> interval) {
        if (interval != null) {
            String timePointType;
            if (interval.containsKey("from") && interval.containsKey("to")) {
                Object intervalFrom = interval.get("from");
                Object intervalTo = interval.get("to");
                if (intervalFrom.getClass() == intervalTo.getClass()) {
                    timePointType = TimePoint.getTimePointType(intervalFrom.getClass().toString());
                    if (timePointType != null) {
                        GlobalVariableManage globalVariableManage = new GlobalVariableManage(this.tx);
                        String timeGranularity = globalVariableManage.getTimeGranularity();
                        if (timeGranularity.equals(timePointType)) {
                            globalVariableManage.setProperty("scopeFrom", interval.get("from"));
                            globalVariableManage.setProperty("scopeTo", interval.get("to"));
                        } else {
                            throw new RuntimeException("The time granularity can't match.The time granularity of database is '" + timeGranularity + "'.");
                        }
                    } else {
                        throw new RuntimeException("Invalid call signature for ScopeFunction: Provided input was " + intervalFrom.getClass() + ".");
                    }
                } else {
                    throw new RuntimeException("The type of interval.from and interval.to is different.");
                }
            } else {
                throw new RuntimeException("Missing key 'from' or 'to' for the interval.");
            }
        } else {
            throw new RuntimeException("Missing parameter interval.");
        }
    }
}