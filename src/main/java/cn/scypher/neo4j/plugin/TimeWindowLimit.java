package cn.scypher.neo4j.plugin;

import cn.scypher.neo4j.plugin.datetime.SInterval;
import cn.scypher.neo4j.plugin.datetime.STimePoint;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import java.time.*;
import java.util.List;
import java.util.Map;

public class TimeWindowLimit {

    /**
     * 用户不可用，用于在时态图查询语句中限制节点和关系的有效时间
     *
     * @param elements   节点/关系及其有效时间的限制
     * @param timeWindow at time/between子句指定的为时间点/时间区间
     * @return 节点和关系的有效时间是否满足限制条件
     */
    @UserFunction("scypher.limitEffectiveTime")
    @Description("Limit the effective time of nodes and relationships.")
    public boolean limitEffectiveTime(@Name("elements") List<List<Object>> elements, @Name("timeWindow") Object timeWindow) {
        if (elements != null && !elements.isEmpty()) {
            // snapshot/scope语句指定的时间区间
            STimePoint snapshotTimePoint = GlobalVariablesManager.getSnapshotTimePoint();
            SInterval scopeInterval = GlobalVariablesManager.getScopeInterval();
            // at time/between子句指定的时间区间
            STimePoint clauseTimePoint = null;
            SInterval clauseInterval = null;
            if (timeWindow != null) {
                if (timeWindow instanceof LocalDate | timeWindow instanceof OffsetTime | timeWindow instanceof LocalTime | timeWindow instanceof ZonedDateTime | timeWindow instanceof LocalDateTime) {
                    clauseTimePoint = new STimePoint(timeWindow);
                } else if (timeWindow instanceof Map) {
                    clauseInterval = new SInterval((Map<String, Object>) timeWindow);
                } else {
                    throw new RuntimeException("Type mismatch: expected Date, Time, LocalTime, LocalDateTime, DateTime or Interval but was " + timeWindow.getClass().getSimpleName());
                }
            }
            for (List<Object> element : elements) {
                // 节点或边的有效时间
                SInterval elementEffectiveTime;
                if (element.size() >= 1 && element.get(0) instanceof Node) {
                    Node node = (Node) element.get(0);
                    elementEffectiveTime = new SInterval(new STimePoint(node.getProperty("intervalFrom")), new STimePoint(node.getProperty("intervalTo")));
                } else if (element.size() >= 1 && element.get(0) instanceof Relationship) {
                    Relationship relationship = (Relationship) element.get(0);
                    elementEffectiveTime = new SInterval(new STimePoint(relationship.getProperty("intervalFrom")), new STimePoint(relationship.getProperty("intervalTo")));
                } else {
                    throw new RuntimeException("The element to limit must be a node or relationship");
                }
                // @T指定的时间区间
                STimePoint elementTimePoint = null;
                SInterval elementInterval = null;
                if (elements.size() >= 2) {
                    if (element.get(1) instanceof LocalDate | timeWindow instanceof OffsetTime | element.get(1) instanceof LocalTime | element.get(1) instanceof ZonedDateTime | element.get(1) instanceof LocalDateTime) {
                        elementTimePoint = new STimePoint(element.get(1));
                    } else if (element.get(1) instanceof Map) {
                        elementInterval = new SInterval((Map<String, Object>) element.get(1));
                    } else {
                        throw new RuntimeException("Type mismatch: expected Date, Time, LocalTime, LocalDateTime, DateTime or Interval but was " + element.get(1).getClass().getSimpleName());
                    }
                }
                if (elementTimePoint != null | elementInterval != null) {
                    // 优先受@T指定的时间区间限制
                    if (elementTimePoint != null && elementEffectiveTime.contains(elementTimePoint)) {
                        continue;
                    } else if (elementInterval != null && elementEffectiveTime.contains(elementInterval)) {
                        continue;
                    }
                    return false;
                } else if (clauseTimePoint != null | clauseInterval != null) {
                    // 次优先受at time/between子句指定的时间区间限制
                    if (clauseTimePoint != null && elementEffectiveTime.contains(clauseTimePoint)) {
                        continue;
                    } else if (clauseInterval != null && elementEffectiveTime.contains(clauseInterval)) {
                        continue;
                    }
                    return false;
                } else if (snapshotTimePoint != null | scopeInterval != null) {
                    // 最后受snapshot/scope语句指定的时间区间限制
                    if (scopeInterval != null && elementEffectiveTime.contains(scopeInterval)) {
                        // 时序图查询语法优先受SCOPE限制
                        continue;
                    } else if (snapshotTimePoint != null && elementEffectiveTime.contains(snapshotTimePoint)) {
                        continue;
                    }
                    return false;
                }
            }
            return true;
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param timePointObject snapshot设置的时间点
     */
    @Procedure(name = "scypher.snapshot", mode = Mode.READ)
    @Description("Do SNAPSHOT operation.")
    public void snapshot(@Name("timePoint") Object timePointObject) {
        if (timePointObject != null) {
            STimePoint timePoint = new STimePoint(timePointObject);
            String timePointType = GlobalVariablesManager.getTimePointType();
            if (timePointType.equals(timePoint.getTimePointType())) {
                GlobalVariablesManager.setSnapshotTimePoint(timePoint);
            } else {
                throw new RuntimeException("The time point type can't match the system. The time point type of database is " + timePointType);
            }
        } else {
            GlobalVariablesManager.setSnapshotTimePoint(null);
        }
    }

    /**
     * @param intervalMap scope设置的时间区间
     */
    @Procedure(name = "scypher.scope", mode = Mode.READ)
    @Description("Do SCOPE operation.")
    public void scope(@Name("interval") Map<String, Object> intervalMap) {
        if (intervalMap != null) {
            SInterval interval = new SInterval(intervalMap);
            String timePointType = GlobalVariablesManager.getTimePointType();
            if (timePointType.equals(interval.getTimePointType())) {
                GlobalVariablesManager.setScopeInterval(interval);
            } else {
                throw new RuntimeException("The time point type of the interval can't match the system中. The time point type of database is " + timePointType);
            }
        } else {
            GlobalVariablesManager.setScopeInterval(null);
        }
    }

    /**
     * @return 返回默认操作时间，若用snapshot设置过，则返回snapshot设置的时间点；若没有，则返回now()
     */
    @UserFunction("scypher.operateTime")
    @Description("Get the default operate time.")
    public Object operateTime() {
        if (GlobalVariablesManager.getSnapshotTimePoint() == null) {
            // 没有设置过默认操作时间，返回now()
            String timePointType = GlobalVariablesManager.getTimePointType();
            String timezone = GlobalVariablesManager.getTimezone();
            return (new STimePoint(timePointType, timezone)).getSystemTimePoint();
        } else {
            return GlobalVariablesManager.getSnapshotTimePoint().getSystemTimePoint();
        }
    }
}