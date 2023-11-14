package cn.scypher.neo4j.plugin;

import cn.scypher.neo4j.plugin.datetime.SInterval;
import cn.scypher.neo4j.plugin.datetime.STimePoint;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReadingQuery {

    public Node getPropertyNode(Node objectNode, String propertyName) {
        ResourceIterable<Relationship> relationships = objectNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("OBJECT_PROPERTY"));
        for (Relationship relationship : relationships) {
            Node endNode = relationship.getEndNode();
            if (endNode.getProperty("content").equals(propertyName)) {
                return endNode;
            }
        }
        return null;
    }


    /**
     * @param objectNode   对象节点
     * @param propertyName 对象节点的属性
     * @param timeWindow   值节点的有效时间限制
     * @return 返回对象节点的某个属性的属性值，如果在限制时间窗口内有多个属性值，返回一个列表；如果在限制时间窗口内有一个属性值，返回该值；如果在限制时间窗口内没有属性值，返回null。
     */
    @UserFunction("scypher.getPropertyValue")
    @Description("Get the property value of object node.")
    public Object getPropertyValue(@Name("node") Node objectNode, @Name("propertyName") String propertyName, @Name("timeWindow") Object timeWindow) {
        if (objectNode != null && propertyName != null) {
            Node propertyNode = getPropertyNode(objectNode, propertyName);
            if (propertyNode != null) {
                ResourceIterable<Relationship> relationships = propertyNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("PROPERTY_VALUE"));
                List<Object> propertyValueList = new ArrayList<>();
                // snapshot/scope语句指定的时间区间
                STimePoint snapshotTimePoint = GlobalVariablesManager.getSnapshotTimePoint();
                SInterval scopeInterval = GlobalVariablesManager.getScopeInterval();
                STimePoint valueNodeTimePoint = null;
                SInterval valueNodeInterval = null;
                if (timeWindow != null) {
                    if (timeWindow instanceof LocalDate | timeWindow instanceof OffsetTime | timeWindow instanceof LocalTime | timeWindow instanceof ZonedDateTime | timeWindow instanceof LocalDateTime) {
                        valueNodeTimePoint = new STimePoint(timeWindow);
                    } else if (timeWindow instanceof Map) {
                        valueNodeInterval = new SInterval((Map<String, Object>) timeWindow);
                    } else {
                        throw new RuntimeException("Type mismatch: expected Date, Time, LocalTime, LocalDateTime, DateTime or Interval but was " + timeWindow.getClass().getSimpleName());
                    }
                }
                for (Relationship relationship : relationships) {
                    Node valueNode = relationship.getEndNode();
                    SInterval valueNodeEffectiveTime = new SInterval(new STimePoint(valueNode.getProperty("intervalFrom")), new STimePoint(valueNode.getProperty("intervalTo")));
                    if (valueNodeTimePoint == null && valueNodeInterval == null && snapshotTimePoint == null && scopeInterval == null) {
                        propertyValueList.add(valueNode.getProperty("content"));
                    } else if (valueNodeTimePoint != null && valueNodeEffectiveTime.contains(valueNodeTimePoint)) {
                        propertyValueList.add(valueNode.getProperty("content"));
                    } else if (valueNodeInterval != null && valueNodeEffectiveTime.contains(valueNodeInterval)) {
                        propertyValueList.add(valueNode.getProperty("content"));
                    } else if (scopeInterval != null && valueNodeEffectiveTime.contains(scopeInterval)) {
                        propertyValueList.add(valueNode.getProperty("content"));
                    } else if (snapshotTimePoint != null && valueNodeEffectiveTime.contains(snapshotTimePoint)) {
                        propertyValueList.add(valueNode.getProperty("content"));
                    }
                }
                if (propertyValueList.size() == 0) {
                    return null;
                }
                if (propertyValueList.size() == 1) {
                    return propertyValueList.get(0);
                }
                return propertyValueList;
            } else {
                return null;
            }
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param objectNode   对象节点/Map类型数据
     * @param propertyName 属性名
     * @return 如果objectNode为对象节点，返回对应属性节点的有效时间；如果objectNode为Map类型数据，且objectNode.propertyName为对象节点/边，返回该对象节点/边的有效时间。
     */
    @UserFunction("scypher.getPropertyEffectiveTime")
    @Description("Get the effective time of property node.")
    public Object getPropertyEffectiveTime(@Name("node") Object objectNode, @Name("propertyName") String propertyName) {
        if (objectNode != null && propertyName != null) {
            if (objectNode instanceof Node) {
                Node propertyNode = getPropertyNode((Node) objectNode, propertyName);
                if (propertyNode != null) {
                    return new SInterval(new STimePoint(propertyNode.getProperty("intervalFrom")), new STimePoint(propertyNode.getProperty("intervalTo"))).getSystemInterval();
                } else {
                    return null;
                }
            } else if (objectNode instanceof Map) {
                Map<String, Object> objectMap = (Map<String, Object>) objectNode;
                if (objectMap.containsKey(propertyName)) {
                    if (objectMap.get(propertyName) instanceof Node node) {
                        return new SInterval(new STimePoint(node.getProperty("intervalFrom")), new STimePoint(node.getProperty("intervalTo"))).getSystemInterval();
                    } else if (objectMap.get(propertyName) instanceof Relationship relationship) {
                        return new SInterval(new STimePoint(relationship.getProperty("intervalFrom")), new STimePoint(relationship.getProperty("intervalTo"))).getSystemInterval();
                    }
                }
                return null;
            } else {
                throw new RuntimeException("Type mismatch: expected Node or Map but was " + objectNode.getClass().getSimpleName());
            }
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }
}
