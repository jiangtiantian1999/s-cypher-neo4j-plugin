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
     * @param elementEffectiveTime 节点/边的有效时间
     * @param timeWindow           在图模式中指定的时间窗口
     * @param clauseTimePoint      at time指定的时间点
     * @param clauseInterval       between指定的时间区间
     * @param snapshotTimePoint    snapshot指定的时间点
     * @param scopeInterval        scope指定的时间区间
     * @return
     */
    public boolean limitEffectiveTime(SInterval elementEffectiveTime, Object timeWindow, STimePoint clauseTimePoint, SInterval clauseInterval, STimePoint snapshotTimePoint, SInterval scopeInterval) {
        // @T指定的时间区间
        STimePoint elementTimePoint = null;
        SInterval elementInterval = null;
        if (timeWindow != null) {
            if (timeWindow instanceof LocalDate | timeWindow instanceof OffsetTime | timeWindow instanceof LocalTime | timeWindow instanceof ZonedDateTime | timeWindow instanceof LocalDateTime) {
                elementTimePoint = new STimePoint(timeWindow);
            } else if (timeWindow instanceof Map) {
                elementInterval = new SInterval((Map<String, Object>) timeWindow);
            } else {
                throw new RuntimeException("Type mismatch: expected Date, Time, LocalTime, LocalDateTime, DateTime or Interval but was " + timeWindow.getClass().getSimpleName());
            }
        }
        if (elementTimePoint != null | elementInterval != null) {
            // 优先受@T指定的时间区间限制
            if (elementTimePoint != null && elementEffectiveTime.contains(elementTimePoint)) {
                return true;
            } else if (elementInterval != null && elementEffectiveTime.overlaps(elementInterval)) {
                return true;
            }
            return false;
        } else if (clauseTimePoint != null | clauseInterval != null) {
            // 次优先受at time/between子句指定的时间区间限制
            if (clauseTimePoint != null && elementEffectiveTime.contains(clauseTimePoint)) {
                return true;
            } else if (clauseInterval != null && elementEffectiveTime.overlaps(clauseInterval)) {
                return true;
            }
            return false;
        } else if (snapshotTimePoint != null | scopeInterval != null) {
            // 最后受snapshot/scope语句指定的时间区间限制
            if (scopeInterval != null && elementEffectiveTime.overlaps(scopeInterval)) {
                // 时序图查询语法优先受SCOPE限制
                return true;
            } else if (snapshotTimePoint != null && elementEffectiveTime.contains(snapshotTimePoint)) {
                return true;
            }
            return false;
        }
        return true;
    }

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
                    if (!limitEffectiveTime(elementEffectiveTime, element.get(1), clauseTimePoint, clauseInterval, snapshotTimePoint, scopeInterval)) {
                        return false;
                    }
                } else if (element.size() >= 1 && element.get(0) instanceof Relationship) {
                    Relationship relationship = (Relationship) element.get(0);
                    elementEffectiveTime = new SInterval(new STimePoint(relationship.getProperty("intervalFrom")), new STimePoint(relationship.getProperty("intervalTo")));
                    if (!limitEffectiveTime(elementEffectiveTime, element.get(1), clauseTimePoint, clauseInterval, snapshotTimePoint, scopeInterval)) {
                        return false;
                    }
                } else if (element.size() >= 1 && element.get(0) instanceof List) {
                    for (Relationship relationship : (List<Relationship>) element.get(0)) {
                        elementEffectiveTime = new SInterval(new STimePoint(relationship.getProperty("intervalFrom")), new STimePoint(relationship.getProperty("intervalTo")));
                        if (!limitEffectiveTime(elementEffectiveTime, element.get(1), clauseTimePoint, clauseInterval, snapshotTimePoint, scopeInterval)) {
                            return false;
                        }
                    }
                } else {
                    throw new RuntimeException("The element to limit must be a node or relationship");
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
     * @return 返回默认操作时间，若用snapshot设置过，则返回snapshot设置的时间点；若没有，则返回timePoint.current()
     */
    @UserFunction("scypher.operateTime")
    @Description("Get the default operate time.")
    public Object operateTime() {
        if (GlobalVariablesManager.getSnapshotTimePoint() == null) {
            // 没有设置过默认操作时间，返回timePoint.current()
            String timePointType = GlobalVariablesManager.getTimePointType();
            String timezone = GlobalVariablesManager.getTimezone();
            return (new STimePoint(timePointType, timezone)).getSystemTimePoint();
        } else {
            return GlobalVariablesManager.getSnapshotTimePoint().getSystemTimePoint();
        }
    }
}