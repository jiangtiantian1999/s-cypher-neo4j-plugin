package cn.scypher.neo4j.plugin;

import cn.scypher.neo4j.plugin.datetime.SInterval;
import cn.scypher.neo4j.plugin.datetime.STimePoint;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReadingQuery {

    @Context
    public Transaction tx;

    @UserFunction("scypher.getPropertyValue")
    @Description("Get the property value of object node.")
    public Object getPropertyValue(@Name("node") Node objectNode, @Name("propertyName") String propertyName, @Name("timeWindow") Object timeWindow) {
        if (objectNode != null && propertyName != null) {
            ResourceIterable<Relationship> relationships = objectNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("OBJECT_PROPERTY"));
            Node propertyNode = null;
            for (Relationship relationship : relationships) {
                Node endNode = relationship.getEndNode();
                if (endNode.getProperty("content").equals(propertyName)) {
                    propertyNode = endNode;
                    break;
                }
            }
            if (propertyNode != null) {
                relationships = propertyNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("PROPERTY_VALUE"));
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
                    throw new RuntimeException("The node does not have property `" + propertyName + "` at the specified time");
                }
                if (propertyValueList.size() == 1) {
                    return propertyValueList.get(0);
                }
                return propertyValueList;
            } else {
                throw new RuntimeException("The node does not have property `" + propertyName + "`");
            }
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }
}
