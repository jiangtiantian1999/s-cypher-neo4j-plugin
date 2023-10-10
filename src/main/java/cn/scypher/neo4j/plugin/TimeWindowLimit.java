package cn.scypher.neo4j.plugin;

import cn.scypher.neo4j.plugin.datetime.SInterval;
import cn.scypher.neo4j.plugin.datetime.STimePoint;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import java.util.Map;

public class TimeWindowLimit {
    @Context
    public Transaction tx;

    /**
     * @return 获取scope语句设置的时间区间
     */
    public SInterval getScopeInterval() {
        ResourceIterator<Node> nodes = tx.findNodes(Label.label("GlobalVariable"));
        if (nodes.hasNext()) {
            Node node = nodes.next();
            if (node.hasProperty("scope")) {
                return new SInterval(new STimePoint(node.getProperty("scopeFrom")), new STimePoint(node.getProperty("scopeTo")));
            }
        }
        return null;
    }

    /**
     * @return 获取snapshot语句设置的时间点
     */
    public STimePoint getSnapshotTimePoint() {
        ResourceIterator<Node> nodes = tx.findNodes(Label.label("GlobalVariable"));
        if (nodes.hasNext()) {
            Node node = nodes.next();
            if (node.hasProperty("snapshot")) {
                return new STimePoint(node.getProperty("snapshot"));
            }
        }
        return null;
    }


    /**
     * 用于在时态图查询语句中限制节点或边的有效时间
     *
     * @param element    节点或边，为Node或Relationship类型
     * @param timeWindow 限制对象节点的有效时间，为时间点类型或Map类型（具有两个key：FROM和TO，value为时间点类型）
     * @return 节点和边的有效时间是否满足限制条件
     */
    @UserFunction("scypher.limitInterval")
    @Description("Limit the effective time of node or relationship.")
    public boolean limitInterval(@Name("element") Object element, @Name("timeWindow") Object timeWindow) {
        if (element != null) {
            // 获取节点或边的有效时间
            SInterval elementInterval;
            if (element instanceof Node) {
                Object elementIntervalFrom = ((Node) element).getProperty("intervalFrom");
                Object elementIntervalTo = ((Node) element).getProperty("intervalTo");
                elementInterval = new SInterval(new STimePoint(elementIntervalFrom), new STimePoint(elementIntervalTo));
            } else if (element instanceof Relationship) {
                Object elementIntervalFrom = ((Relationship) element).getProperty("intervalFrom");
                Object elementIntervalTo = ((Relationship) element).getProperty("intervalTo");
                elementInterval = new SInterval(new STimePoint(elementIntervalFrom), new STimePoint(elementIntervalTo));
            } else {
                throw new RuntimeException("The element must be node or relationship.");
            }

            if (timeWindow != null) {
                // 优先受AT_TIME或BETWEEN语句限制
                if (timeWindow instanceof Map) {
                    SInterval limitInterval = new SInterval((Map<String, Object>) timeWindow);
                    return elementInterval.overlaps(limitInterval);
                } else {
                    // timeWindow为时间点类型
                    STimePoint limitTimePoint = new STimePoint(timeWindow);
                    return elementInterval.contains(limitTimePoint);
                }
            } else {
                // 受SNAPSHOT或SCOPE限制
                SInterval limitScopeInterval = getScopeInterval();
                if (limitScopeInterval != null) {
                    // 时序图查询语法优先受SCOPE限制
                    return elementInterval.overlaps(limitScopeInterval);
                } else {
                    // 受SNAPSHOT限制
                    STimePoint limitSnapshotTimePoint = getSnapshotTimePoint();
                    if (limitSnapshotTimePoint != null) {
                        return elementInterval.contains(limitSnapshotTimePoint);
                    }
                }
            }
            return true;
        } else {
            throw new RuntimeException("Missing parameter element(a node or relationship).");
        }
    }

    /**
     * @param timePoint 为时间点类型
     */
    @Procedure(name = "scypher.snapshot", mode = Mode.WRITE)
    @Description("SNAPSHOT operation.")
    public void snapshot(@Name("timePoint") Object timePoint) {
        if (timePoint != null) {
            String timePointType = STimePoint.getTimePointType(timePoint.getClass().toString());
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
                    timePointType = STimePoint.getTimePointType(intervalFrom.getClass().toString());
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