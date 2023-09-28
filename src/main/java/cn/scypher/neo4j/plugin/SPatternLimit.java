package cn.scypher.neo4j.plugin;

import cn.scypher.neo4j.plugin.datetime.SInterval;
import cn.scypher.neo4j.plugin.datetime.TimePoint;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.time.*;
import java.util.HashMap;
import java.util.Map;


public class SPatternLimit {

    @Context
    public Transaction tx;

    enum SRelationshipTypes implements RelationshipType {
        OBJECT_PROPERTY,
        PROPERTY_VALUE
    }

    /**
     * @return 获取scope语句设置的时间区间
     */
    public SInterval getScopeInterval() {
        ResourceIterator<Node> nodes = tx.findNodes(Label.label("GlobalVariable"));
        if (nodes.hasNext()) {
            Node node = nodes.next();
            if (node.hasProperty("scope")) {
                return new SInterval(new TimePoint(node.getProperty("scopeFrom")), new TimePoint(node.getProperty("scopeTo")));
            }
        }
        return null;
    }

    public TimePoint getSnapshotTimePoint() {
        ResourceIterator<Node> nodes = tx.findNodes(Label.label("GlobalVariable"));
        if (nodes.hasNext()) {
            Node node = nodes.next();
            if (node.hasProperty("snapshot")) {
                return new TimePoint(node.getProperty("snapshot"));
            }
        }
        return null;
    }

    /**
     * 用于在时态图查询语句中限制节点或边的有效时间
     *
     * @param element    节点或边，为Node或Relationship类型
     * @param atTime     限制对象节点的有效时间，为Map类型（具有两个key：FROM和TO，value为时间点类型）
     * @param timeWindow 时间窗口限制，为时间点类型或Map类型（具有两个key：FROM和TO，value为时间点类型）
     * @return 节点和边的有效时间是否满足限制条件
     */
    @UserFunction("scypher.limitInterval")
    @Description("Limit the effective time of node or relationship.")
    public boolean limitInterval(@Name("node") Object element, @Name("atTime") Map<String, Object> atTime,
                                 @Name("timeWindow") Object timeWindow) {
        if (element != null) {
            // 获取节点或边的有效时间
            Object elementIntervalFrom;
            Object elementIntervalTo;
            if (element instanceof Node) {
                elementIntervalFrom = ((Node) element).getProperty("intervalFrom");
                elementIntervalTo = ((Node) element).getProperty("intervalTo");
            } else if (element instanceof Relationship) {
                elementIntervalFrom = ((Relationship) element).getProperty("intervalFrom");
                elementIntervalTo = ((Relationship) element).getProperty("intervalTo");
            } else {
                throw new RuntimeException("The element must be node or relationship.");
            }
            SInterval elementInterval = new SInterval(new TimePoint(elementIntervalFrom), new TimePoint(elementIntervalTo));

            // 获取查询的时间窗口
            SInterval limitInterval = null;
            TimePoint limitTimePoint = null;
            if (atTime != null) {
                if (atTime.containsKey("from") && atTime.containsKey("to")) {
                    limitInterval = new SInterval(new TimePoint(atTime.get("from")), new TimePoint(atTime.get("to")));
                } else {
                    throw new RuntimeException("Missing key 'from' or 'to' for the atTimeInterval.");
                }
            }

            if (timeWindow != null) {
                // 优先受AT_TIME或BETWEEN语句限制
                if (timeWindow instanceof Map) {
                    Map<String, Object> TimeWindowMap = (Map<String, Object>) timeWindow;
                    if (TimeWindowMap.containsKey("from") && TimeWindowMap.containsKey("to")) {
                        SInterval limitTimeWindow = new SInterval(new TimePoint(TimeWindowMap.get("from")), new TimePoint(TimeWindowMap.get("to")));
                        if (limitInterval == null) {
                            limitInterval = limitTimeWindow;
                        } else {
                            limitInterval = limitInterval.intersection(limitTimeWindow);
                        }
                    } else {
                        throw new RuntimeException("Missing key 'from' or 'to' for the atTimeInterval.");
                    }
                } else if (timeWindow instanceof LocalDate | timeWindow instanceof OffsetTime | timeWindow instanceof LocalTime
                        | timeWindow instanceof ZonedDateTime | timeWindow instanceof LocalDateTime) {
                    limitTimePoint = new TimePoint(timeWindow);
                } else {
                    throw new RuntimeException("The timeWindow must be time point or interval.");
                }
            } else {
                // 受SNAPSHOT或SCOPE限制
                SInterval limitScopeInterval = getScopeInterval();
                if (limitScopeInterval != null) {
                    // 时序图查询语法优先受SCOPE限制
                    if (limitInterval == null) {
                        limitInterval = limitScopeInterval;
                    } else {
                        limitInterval = limitInterval.intersection(limitScopeInterval);
                    }
                } else {
                    // 受SNAPSHOT限制
                    TimePoint limitSnapshotTimePoint = getSnapshotTimePoint();
                    if (limitSnapshotTimePoint != null) {
                        limitTimePoint = limitSnapshotTimePoint;
                    }
                }
            }
            // 判断节点或边的有效时间是否满足限制条件
            if (limitInterval != null) {
                return elementInterval.overlaps(limitInterval);
            } else if (limitTimePoint != null) {
                return elementInterval.contains(limitTimePoint);
            } else {
                return true;
            }
        } else {
            throw new RuntimeException("Missing parameter element.");
        }
    }

    /**
     *
     * @param objectNode 对象节点
     * @return 返回对象节点的有效时间
     */
    @UserFunction("scypher.getObjectInterval")
    @Description("Get the effective time of the object node.")
    public Object getObjectInterval(@Name("objectNode") Node objectNode) {
        Map<String, Object> objectInterval = new HashMap<>();
        objectInterval.put("from", objectNode.getProperty("intervalFrom"));
        objectInterval.put("to", objectNode.getProperty("intervalTo"));
        return objectInterval;
    }


    /**
     * @param objectNode   对象节点
     * @param propertyName 属性名
     * @return 返回属性节点的有效时间
     */
//    @UserFunction("scypher.getPropertyValue")
//    @Description("Get the property value of the object node based on the property name.")
//    public Object getPropertyValue(@Name("objectNode") Node objectNode, @Name("propertyName") String propertyName) {
//        ResourceIterable<Relationship> relationships = objectNode.getRelationships(Direction.OUTGOING,
//                SRelationshipTypes.OBJECT_PROPERTY);
//        String timePointType = objectNode.getProperty("intervalFrom").getClass().toString();
//        String timezone = null;
//        SInterval limitInterval = null;
//        if (intervalFrom != null && intervalTo != null) {
//            limitInterval = new SInterval(new TimePoint(intervalFrom, timePointType, timezone),
//                    new TimePoint(intervalTo, timePointType, timezone));
//            if (timeWindow != null) {
//                if (timeWindow.getClass().toString().equals(Map.class.toString())) {
//
//                }
//            }
//        }
//
//        List<Node> propertyNodes = new ArrayList<>();
//        for (Relationship relationship : relationships) {
//            Node propertyNode = relationship.getEndNode();
//            if (propertyNode.getProperty("content").equals(propertyName)) {
//
//                propertyNodes.add(propertyNode);
//            }
//        }
//        return propertyNodes;
//    }
}
