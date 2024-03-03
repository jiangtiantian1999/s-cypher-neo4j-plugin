package cn.scypher.neo4j.plugin;

import cn.scypher.neo4j.plugin.datetime.SInterval;
import cn.scypher.neo4j.plugin.datetime.STimePoint;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.time.*;
import java.util.*;

public class UpdatingQuery {
    /**
     * @param objectNode 对象节点
     * @return 返回某个对象节点的所有属性节点
     */
    public static List<Node> getPropertyNodes(Node objectNode) {
        List<Node> propertyNodeList = new ArrayList<>();
        ResourceIterable<Relationship> relationships = objectNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("OBJECT_PROPERTY"));
        for (Relationship relationship : relationships) {
            propertyNodeList.add(relationship.getEndNode());
        }
        return propertyNodeList;
    }

    /**
     * @param propertyNode 属性节点
     * @return 返回某个属性节点的所有值节点
     */
    public static List<Node> getValueNodes(Node propertyNode) {
        List<Node> valueNodeList = new ArrayList<>();
        ResourceIterable<Relationship> relationships = propertyNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("PROPERTY_VALUE"));
        for (Relationship relationship : relationships) {
            valueNodeList.add(relationship.getEndNode());
        }
        return valueNodeList;
    }

    /**
     * @param objectNode 对象节点
     * @param timePoint  时间点
     * @return 返回某个对象节点在某个时间点的属性节点
     */
    public static List<Node> getPropertyNodes(Node objectNode, Object timePoint) {
        ResourceIterable<Relationship> relationships = objectNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("OBJECT_PROPERTY"));
        String timePointType = GlobalVariablesManager.getTimePointType();
        String timezone = GlobalVariablesManager.getTimezone();
        STimePoint snapshotTimePoint = GlobalVariablesManager.getSnapshotTimePoint();
        STimePoint propertyNodeTimePoint = null;
        if (timePoint != null) {
            if (timePoint instanceof LocalDate | timePoint instanceof OffsetTime | timePoint instanceof LocalTime | timePoint instanceof ZonedDateTime | timePoint instanceof LocalDateTime) {
                propertyNodeTimePoint = new STimePoint(timePoint);
            } else {
                throw new RuntimeException("Type mismatch: expected Date, Time, LocalTime, LocalDateTime or DateTime but was " + timePoint.getClass().getSimpleName());
            }
        }
        List<Node> propertyNodes = new ArrayList<>();
        for (Relationship relationship : relationships) {
            Node propertyNode = relationship.getEndNode();
            SInterval propertyNodeEffectiveTime = new SInterval(new STimePoint(propertyNode.getProperty("intervalFrom")), new STimePoint(propertyNode.getProperty("intervalTo")));
            if (propertyNodeTimePoint != null && propertyNodeEffectiveTime.contains(propertyNodeTimePoint)) {
                propertyNodes.add(propertyNode);
            } else if (snapshotTimePoint != null && propertyNodeEffectiveTime.contains(snapshotTimePoint)) {
                propertyNodes.add(propertyNode);
            } else if (propertyNodeEffectiveTime.contains(new STimePoint(timePointType, timezone))) {
                propertyNodes.add(propertyNode);
            }
        }
        return propertyNodes;
    }

    /**
     * @param propertyNode 属性节点
     * @param timePoint    时间点
     * @return 返回某个属性节点在某个时间点的值节点
     */
    public static Node getValueNode(Node propertyNode, Object timePoint) {
        ResourceIterable<Relationship> relationships = propertyNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("PROPERTY_VALUE"));
        String timePointType = GlobalVariablesManager.getTimePointType();
        String timezone = GlobalVariablesManager.getTimezone();
        STimePoint snapshotTimePoint = GlobalVariablesManager.getSnapshotTimePoint();
        STimePoint valueNodeTimePoint = null;
        if (timePoint != null) {
            if (timePoint instanceof LocalDate | timePoint instanceof OffsetTime | timePoint instanceof LocalTime | timePoint instanceof ZonedDateTime | timePoint instanceof LocalDateTime) {
                valueNodeTimePoint = new STimePoint(timePoint);
            } else {
                throw new RuntimeException("Type mismatch: expected Date, Time, LocalTime, LocalDateTime or DateTime but was " + timePoint.getClass().getSimpleName());
            }
        }
        for (Relationship relationship : relationships) {
            Node valueNode = relationship.getEndNode();
            SInterval valueNodeEffectiveTime = new SInterval(new STimePoint(valueNode.getProperty("intervalFrom")), new STimePoint(valueNode.getProperty("intervalTo")));
            if (valueNodeTimePoint != null && valueNodeEffectiveTime.contains(valueNodeTimePoint)) {
                return valueNode;
            } else if (snapshotTimePoint != null && valueNodeEffectiveTime.contains(snapshotTimePoint)) {
                return valueNode;
            } else if (valueNodeEffectiveTime.contains(new STimePoint(timePointType, timezone))) {
                return valueNode;
            }
        }
        return null;
    }

    /**
     * @param objectNode 对象节点
     * @return 返回对象节点的所有属性节点和值节点
     */
    public static List<Object> deleteObjectNode(Node objectNode) {
        // 对象节点的所有属性节点
        List<Node> propertyNodes = getPropertyNodes(objectNode);
        // 对象节点的所有值节点
        List<Node> valueNodes = new ArrayList<>();
        for (Node propertyNode : propertyNodes) {
            valueNodes.addAll(getValueNodes(propertyNode));
        }
        List<Object> itemsToDelete = new ArrayList<>();
        itemsToDelete.addAll(valueNodes);
        itemsToDelete.addAll(propertyNodes);
        return itemsToDelete;
    }

    /**
     * @param object       对象节点/关系/路径
     * @param propertyName 属性名
     * @param timeWindow   时间点/时间区间/boolean类型，表示删除的值节点的范围/[是否仅删除值节点]
     * @return 以列表形式返回所有待物理删除的属性节点和值节点
     */
    @UserFunction("scypher.getItemsToDelete")
    @Description("Get the items to delete.")
    public List<Object> getItemsToDelete(@Name("object") Object object, @Name("propertyName") String propertyName, @Name("timeWindow") Object timeWindow) {
        if (object != null) {
            List<Object> itemsToDelete = new ArrayList<>();
            if (object instanceof Node objectNode) {
                if (propertyName != null) {
                    // 物理删除对象节点的属性
                    Node propertyNode = ReadingQuery.getPropertyNode(objectNode, propertyName);
                    if (propertyNode != null) {
                        List<Node> valueNodes;
                        if (timeWindow == null) {
                            // 物理删除属性节点和值节点
                            valueNodes = getValueNodes(propertyNode);
                            itemsToDelete.addAll(valueNodes);
                            itemsToDelete.add(propertyNode);
                        } else {
                            // 仅物理删除值节点
                            if (timeWindow instanceof Boolean) {
                                valueNodes = ReadingQuery.getValueNodes(propertyNode, null);
                            } else {
                                valueNodes = ReadingQuery.getValueNodes(propertyNode, timeWindow);
                            }
                            itemsToDelete.addAll(valueNodes);
                        }
                    }
                } else {
                    // 物理删除对象节点的所有属性节点和值节点以及相连边
                    itemsToDelete.addAll(deleteObjectNode(objectNode));
                }
                return itemsToDelete;
            } else if (object instanceof Relationship) {
                return itemsToDelete;
            } else if (object instanceof Path path) {
                // 物理删除路径，并删除路径上所有对象节点的所有属性节点和值节点
                for (Node objectNode : path.nodes()) {
                    itemsToDelete.addAll(deleteObjectNode(objectNode));
                }
                return itemsToDelete;
            } else {
                throw new RuntimeException("Type mismatch: expected Node, Relationship or Path but was " + object.getClass().getSimpleName());
            }
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * @param object            对象节点/关系
     * @param propertyName      属性名
     * @param deleteValueNode   是否仅逻辑删除值节点
     * @param operateTimeObject 逻辑删除时间
     * @return 以列表形式返回所有待逻辑删除的元素
     */
    @UserFunction("scypher.getItemsToStale")
    @Description("Get the items to stale.")
    public List<Object> getItemsToStale(@Name("object") Object object, @Name("propertyName") String propertyName, @Name("deleteValueNode") boolean deleteValueNode, @Name("operateTime") Object operateTimeObject) {
        if (object != null) {
            List<Object> itemsToStale = new ArrayList<>();
            String timePointType = GlobalVariablesManager.getTimePointType();
            String timezone = GlobalVariablesManager.getTimezone();
            STimePoint NOW = new STimePoint("NOW", timePointType, timezone);
            STimePoint operateTime;
            if (operateTimeObject != null) {
                operateTime = new STimePoint(operateTimeObject);
            } else {
                operateTime = new STimePoint(timePointType, timezone);
            }
            if (object instanceof Node objectNode) {
                if (objectNode.getProperty("intervalTo").equals(NOW.getSystemTimePoint())) {
                    if (propertyName != null) {
                        // 逻辑删除对象节点的属性
                        Node propertyNode = ReadingQuery.getPropertyNode(objectNode, propertyName);
                        if (propertyNode != null && propertyNode.getProperty("intervalTo").equals(NOW.getSystemTimePoint())) {
                            if (!deleteValueNode) {
                                // 逻辑删除属性节点和值节点
                                if ((new STimePoint(propertyNode.getProperty("intervalFrom"))).isBefore(operateTime)) {
                                    itemsToStale.add(propertyNode);
                                } else {
                                    throw new RuntimeException("The operate time must be latter than the start time of current property node. Please alter the operate time");
                                }
                            }
                            List<Node> valueNodes = ReadingQuery.getValueNodes(propertyNode, NOW.getSystemTimePoint());
                            if (valueNodes.size() == 1) {
                                if ((new STimePoint(valueNodes.get(0).getProperty("intervalFrom"))).isBefore(operateTime)) {
                                    itemsToStale.add(valueNodes.get(0));
                                } else {
                                    throw new RuntimeException("The operate time must be latter than the start time of current value node. Please alter the operate time");
                                }
                            } else if (valueNodes.size() > 1) {
                                throw new RuntimeException("Temporal Graph Database System Error");
                            }
                        }
                    } else {
                        // 逻辑删除对象节点，并逻辑删除其所有属性节点、值节点和相连关系
                        if ((new STimePoint(objectNode.getProperty("intervalFrom"))).isBefore(operateTime)) {
                            itemsToStale.add(objectNode);
                            List<Node> propertyNodes = getPropertyNodes(objectNode);
                            for (Node propertyNode : propertyNodes) {
                                if (propertyNode.getProperty("intervalTo").equals(NOW.getSystemTimePoint())) {
                                    if ((new STimePoint(propertyNode.getProperty("intervalFrom"))).isBefore(operateTime)) {
                                        itemsToStale.add(propertyNode);
                                        List<Node> valueNodes = ReadingQuery.getValueNodes(propertyNode, NOW.getSystemTimePoint());
                                        if (valueNodes.size() == 1) {
                                            if ((new STimePoint(valueNodes.get(0).getProperty("intervalFrom"))).isBefore(operateTime)) {
                                                itemsToStale.add(valueNodes.get(0));
                                            } else {
                                                throw new RuntimeException("The operate time must be latter than the start time of current value node. Please alter the operate time");
                                            }
                                        } else if (valueNodes.size() > 1) {
                                            throw new RuntimeException("Temporal Graph Database System Error");
                                        }
                                    } else {
                                        throw new RuntimeException("The operate time must be latter than the start time of current property node. Please alter the operate time");
                                    }
                                }
                            }
                            ResourceIterable<Relationship> relationships = objectNode.getRelationships();
                            for (Relationship relationship : relationships) {
                                if (relationship.hasProperty("intervalTo") && relationship.getProperty("intervalTo").equals(NOW.getSystemTimePoint())) {
                                    if ((new STimePoint(relationship.getProperty("intervalFrom"))).isBefore(operateTime)) {
                                        itemsToStale.add(relationship);
                                    } else {
                                        throw new RuntimeException("The operate time must be latter than the start time of current relationship. Please alter the operate time");
                                    }
                                }
                            }
                        } else {
                            throw new RuntimeException("The operate time must be latter than the start time of current object node. Please alter the operate time");
                        }
                    }
                }
            } else if (object instanceof Relationship relationship) {
                // 逻辑删除关系
                if (relationship.getProperty("intervalTo").equals(NOW.getSystemTimePoint())) {
                    if ((new STimePoint(relationship.getProperty("intervalFrom"))).isBefore(operateTime)) {
                        itemsToStale.add(relationship);
                    } else {
                        throw new RuntimeException("The operate time must be latter than the start time of current relationship. Please alter the operate time");
                    }
                }
            } else {
                throw new RuntimeException("Type mismatch: expected Node or Relationship but was " + object.getClass().getSimpleName());
            }
            return itemsToStale;
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param objectInfo            对象节点/关系及其有效时间，为map类型，有两个key：object和effectiveTime
     * @param propertyInfo          属性名及属性节点的有效时间，为map类型，有两个key：propertyName和effectiveTime
     * @param valueEffectiveTimeMap 值节点的有效时间，为时间区间
     * @param operateTimeObject     set语句的操作时间，为时间点。修改值节点的有效时间时，修改在该时间有效的值节点的有效时间
     * @return 返回对象节点/关系/属性节点/值节点及其待设置的有效时间，返回前需检查有效时间的约束
     */
    @UserFunction("scypher.getItemsToSetEffectiveTime")
    @Description("Get the info of items to set effective time.")
    public List<Map<String, Object>> getItemsToSetEffectiveTime(@Name("objectInfo") Map<String, Object> objectInfo, @Name("propertyInfo") Map<String, Object> propertyInfo, @Name("valueEffectiveTime") Map<String, Object> valueEffectiveTimeMap, @Name("operateTime") Object operateTimeObject) {
        if (objectInfo != null) {
            List<Map<String, Object>> itemsToSetEffectiveTime = new ArrayList<>();
            // 修改对象节点/属性节点/值节点的有效时间
            if (objectInfo.get("object") instanceof Node objectNode) {
                SInterval objectEffectiveTime;
                if (objectInfo.containsKey("effectiveTime") && objectInfo.get("effectiveTime") != null) {
                    objectEffectiveTime = new SInterval((Map<String, Object>) objectInfo.get("effectiveTime"));
                    // 检查对象节点的有效时间是否满足约束
                    // 对象节点的有效时间是否覆盖其所有边的有效时间
                    ResourceIterable<Relationship> relationships = objectNode.getRelationships();
                    for (Relationship relationship : relationships) {
                        if (!relationship.isType(RelationshipType.withName("OBJECT_PROPERTY")) && !relationship.isType(RelationshipType.withName("PROPERTY_VALUE"))) {
                            SInterval relationshipEffectiveTime = new SInterval(new STimePoint(relationship.getProperty("intervalFrom")), new STimePoint(relationship.getProperty("intervalTo")));
                            if (!objectEffectiveTime.contains(relationshipEffectiveTime)) {
                                throw new RuntimeException("The effective time of object node must contain the effective time of it's relationships");
                            }
                        }
                    }
                    // 对象节点的有效时间是否覆盖其所有属性节点的有效时间
                    List<Node> propertyNodes = UpdatingQuery.getPropertyNodes(objectNode);
                    for (Node propertyNode : propertyNodes) {
                        SInterval propertyEffectiveTime = new SInterval(new STimePoint(propertyNode.getProperty("intervalFrom")), new STimePoint(propertyNode.getProperty("intervalTo")));
                        if (propertyInfo == null | (propertyInfo != null && !propertyNode.getProperty("content").equals(propertyInfo.get("propertyName")))) {
                            if (!objectEffectiveTime.contains(propertyEffectiveTime)) {
                                throw new RuntimeException("The effective time of object node must contain the effective time of it's property nodes");
                            }
                        }
                    }
                    // 对象节点的有效时间满足约束
                    Map<String, Object> objectNodeInfo = new HashMap<>();
                    objectNodeInfo.put("item", objectNode);
                    objectNodeInfo.put("intervalFrom", objectEffectiveTime.getIntervalFrom().getSystemTimePoint());
                    objectNodeInfo.put("intervalTo", objectEffectiveTime.getIntervalTo().getSystemTimePoint());
                    itemsToSetEffectiveTime.add(objectNodeInfo);
                } else {
                    objectEffectiveTime = new SInterval(new STimePoint(objectNode.getProperty("intervalFrom")), new STimePoint(objectNode.getProperty("intervalTo")));
                }
                if (propertyInfo != null) {
                    Node propertyNode = ReadingQuery.getPropertyNode(objectNode, (String) propertyInfo.get("propertyName"));
                    if (propertyNode != null) {
                        SInterval propertyEffectiveTime;
                        if (propertyInfo.containsKey("effectiveTime") && propertyInfo.get("effectiveTime") != null) {
                            propertyEffectiveTime = new SInterval((Map<String, Object>) (propertyInfo.get("effectiveTime")));
                            // 检查属性节点的有效时间是否满足约束
                            if (objectEffectiveTime.contains(propertyEffectiveTime)) {
                                // 属性节点的有效时间是否覆盖其所有值节点的有效时间
                                List<Node> valueNodes = UpdatingQuery.getPropertyNodes(objectNode);
                                STimePoint operateTime = new STimePoint(operateTimeObject);
                                for (Node valueNode : valueNodes) {
                                    SInterval valueEffectiveTime = new SInterval(new STimePoint(valueNode.getProperty("intervalFrom")), new STimePoint(valueNode.getProperty("intervalTo")));
                                    if (valueEffectiveTimeMap == null | !valueEffectiveTime.contains(operateTime)) {
                                        if (!propertyEffectiveTime.contains(valueEffectiveTime)) {
                                            throw new RuntimeException("The effective time of property node must contain the effective time of it's value nodes");
                                        }
                                    }
                                }
                                // 属性节点的有效时间满足约束
                                Map<String, Object> propertyNodeInfo = new HashMap<>();
                                propertyNodeInfo.put("item", propertyNode);
                                propertyNodeInfo.put("intervalFrom", propertyEffectiveTime.getIntervalFrom().getSystemTimePoint());
                                propertyNodeInfo.put("intervalTo", propertyEffectiveTime.getIntervalTo().getSystemTimePoint());
                                itemsToSetEffectiveTime.add(propertyNodeInfo);
                            } else {
                                throw new RuntimeException("The effective time of property node must in the effective time of it's object node");
                            }
                        } else {
                            propertyEffectiveTime = new SInterval(new STimePoint(propertyNode.getProperty("intervalFrom")), new STimePoint(propertyNode.getProperty("intervalTo")));
                        }
                        if (valueEffectiveTimeMap != null) {
                            SInterval valueEffectiveTime = new SInterval(valueEffectiveTimeMap);
                            STimePoint operateTime = new STimePoint(operateTimeObject);
                            // 检查值节点的有效时间是否满足约束
                            if (propertyEffectiveTime.contains(valueEffectiveTime)) {
                                for (Node valueNode : UpdatingQuery.getValueNodes(propertyNode)) {
                                    SInterval otherEffectiveTime = new SInterval(new STimePoint(valueNode.getProperty("intervalFrom")), new STimePoint(valueNode.getProperty("intervalTo")));
                                    if (!otherEffectiveTime.contains(operateTime) && otherEffectiveTime.overlaps(valueEffectiveTime)) {
                                        throw new RuntimeException("The effective time of value nodes of the same property nodes can't overlap");
                                    }
                                }
                                // 值节点的有效时间满足约束
                                List<Node> valueNodes = ReadingQuery.getValueNodes(propertyNode, operateTimeObject);
                                if (valueNodes.size() == 1) {
                                    Map<String, Object> valueNodeInfo = new HashMap<>();
                                    valueNodeInfo.put("item", valueNodes.get(0));
                                    valueNodeInfo.put("intervalFrom", valueEffectiveTime.getIntervalFrom().getSystemTimePoint());
                                    valueNodeInfo.put("intervalTo", valueEffectiveTime.getIntervalTo().getSystemTimePoint());
                                    itemsToSetEffectiveTime.add(valueNodeInfo);
                                } else if (valueNodes.size() > 1) {
                                    throw new RuntimeException("Temporal Graph Database System Error");
                                }
                            } else {
                                throw new RuntimeException("The effective time of value node must in the effective time of it's property node");
                            }
                        }
                    } else {
                        throw new RuntimeException("The node hasn't property `" + propertyInfo.get("propertyName") + "`");
                    }
                }
            } else if (objectInfo.get("object") instanceof Relationship relationship) {
                // 修改关系的有效时间
                if (propertyInfo == null && valueEffectiveTimeMap == null) {
                    // 检查关系的有效时间是否满足约束
                    SInterval relationshipEffectiveTime = new SInterval((Map<String, Object>) objectInfo.get("effectiveTime"));
                    Node startNode = relationship.getStartNode();
                    Node endNode = relationship.getEndNode();
                    SInterval startNodeEffectiveTime = new SInterval(new STimePoint(startNode.getProperty("intervalFrom")), new STimePoint(startNode.getProperty("intervalTo")));
                    SInterval endNodeEffectiveTime = new SInterval(new STimePoint(endNode.getProperty("intervalFrom")), new STimePoint(endNode.getProperty("intervalTo")));
                    if (startNodeEffectiveTime.contains(relationshipEffectiveTime) && endNodeEffectiveTime.contains(relationshipEffectiveTime)) {
                        // 关系的有效时间满足约束
                        Map<String, Object> relationshipInfo = new HashMap<>();
                        relationshipInfo.put("item", relationship);
                        relationshipInfo.put("intervalFrom", relationshipEffectiveTime.getIntervalFrom().getSystemTimePoint());
                        relationshipInfo.put("intervalTo", relationshipEffectiveTime.getIntervalTo().getSystemTimePoint());
                        itemsToSetEffectiveTime.add(relationshipInfo);
                    } else {
                        throw new RuntimeException("The effective time of relationship must in the effective time of it's start node and end node at the same time");
                    }
                } else {
                    throw new RuntimeException("The properties of relationship don't have effective time");
                }
            } else {
                throw new RuntimeException("Type mismatch: expected Node or Relationship but was " + objectInfo.get("object").getClass().getSimpleName());
            }
            return itemsToSetEffectiveTime;
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param objectNode    对象节点
     * @param propertyName  属性名
     * @param propertyValue 属性值（用于确定是否为null）
     * @param timeWindow    时间点/时间区间，用于限定值节点的有效时间。如果没有满足条件的值节点，则将新建一个值节点（属性节点），该值节点的有效时间由timeWindow确定
     * @return 返回valueNodesToAlter，createPropertyNode，createValueNode
     */
    @UserFunction("scypher.getItemsToSetValue")
    @Description("Get the items to be set when setting the value")
    public List<Map<String, List>> getItemsToSetValue(@Name("node") Node objectNode, @Name("propertyName") String propertyName, @Name("propertyValue") Object propertyValue, @Name("timeWindow") Object timeWindow) {
        if (objectNode != null && propertyName != null && timeWindow != null) {
            if (!propertyName.equals("intervalFrom") && !propertyName.equals("intervalTo")) {
                List<Map<String, List>> itemsToSetValue = new ArrayList<>();
                String timePointType = GlobalVariablesManager.getTimePointType();
                String timezone = GlobalVariablesManager.getTimezone();
                STimePoint NOW = new STimePoint("NOW", timePointType, timezone);
                Node propertyNode = ReadingQuery.getPropertyNode(objectNode, propertyName);
                if (propertyNode != null) {
                    List<Node> valueNodes = ReadingQuery.getValueNodes(propertyNode, timeWindow);
                    if (valueNodes.size() != 0) {
                        // 存在符合要求的值节点
                        if (propertyValue != null) {
                            // 修改这些值节点的内容
                            Map<String, List> valueNodesToAlter = new HashMap<>();
                            valueNodesToAlter.put("valueNodesToAlter", valueNodes);
                            itemsToSetValue.add(valueNodesToAlter);
                            return itemsToSetValue;
                        } else {
                            throw new RuntimeException("The content of value nodes can't be null");
                        }
                    }
                }
                // 不存在符合要求的值节点，创建新的值节点（和属性节点）
                SInterval objectEffectiveTime = new SInterval(new STimePoint(objectNode.getProperty("intervalFrom")), new STimePoint(objectNode.getProperty("intervalTo")));
                SInterval propertyInterval;
                if (timeWindow instanceof LocalDate | timeWindow instanceof OffsetTime | timeWindow instanceof LocalTime | timeWindow instanceof ZonedDateTime | timeWindow instanceof LocalDateTime) {
                    propertyInterval = new SInterval(new STimePoint(timeWindow), NOW);
                } else if (timeWindow instanceof Map) {
                    propertyInterval = new SInterval((Map<String, Object>) timeWindow);
                } else {
                    throw new RuntimeException("Type mismatch: expected Date, Time, LocalTime, LocalDateTime, DateTime or Interval but was " + timeWindow.getClass().getSimpleName());
                }
                if (objectEffectiveTime.contains(propertyInterval)) {
                    if (propertyNode != null) {
                        SInterval propertyEffectiveTime = new SInterval(new STimePoint(propertyNode.getProperty("intervalFrom")), new STimePoint(propertyNode.getProperty("intervalTo")));
                        if (propertyEffectiveTime.contains(propertyInterval)) {
                            // 仅创建值节点，返回所连接的属性节点
                            Map<String, List> CreateValueNode = new HashMap<>();
                            List<Node> propertyNodeList = new ArrayList<>();
                            propertyNodeList.add(propertyNode);
                            CreateValueNode.put("createValueNode", propertyNodeList);
                            itemsToSetValue.add(CreateValueNode);
                        } else {
                            throw new RuntimeException("The effective time of property node must contain the effective time of it's value nodes. Please alter the time window");
                        }
                    } else {
                        // 创建属性节点和值节点节点，返回属性节点所连接的对象节点
                        Map<String, List> createPropertyNode = new HashMap<>();
                        List<Node> objectNodeList = new ArrayList<>();
                        objectNodeList.add(objectNode);
                        createPropertyNode.put("createPropertyNode", objectNodeList);
                        itemsToSetValue.add(createPropertyNode);
                    }
                    return itemsToSetValue;
                } else {
                    throw new RuntimeException("The effective time of object node must contain the effective time of it's value nodes. Please alter the time window");
                }
            } else {
                throw new RuntimeException("User can't set `intervalFrom` or `intervalTo` directly");
            }
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param object            对象节点/关系
     * @param propertyName      属性名
     * @param propertyValue     属性值（用于确定是否为null）
     * @param operateTimeObject set的操作时间
     * @return 返回staleValueNode，createValueNode，createPropertyNode，setRelationshipProperty
     */
    @UserFunction("scypher.getItemsToSetProperty")
    @Description("Get the items to be set when setting the property")
    public List<Map<String, List>> getItemsToSetProperty(@Name("object") Object object, @Name("propertyName") String propertyName, @Name("propertyValue") Object propertyValue, @Name("operateTime") Object operateTimeObject) {
        if (object != null && propertyName != null) {
            if (!propertyName.equals("intervalFrom") && !propertyName.equals("intervalTo")) {
                List<Map<String, List>> itemsToSetProperty = new ArrayList<>();
                Map<String, List> itemToSetProperty = new HashMap<>();
                String timePointType = GlobalVariablesManager.getTimePointType();
                String timezone = GlobalVariablesManager.getTimezone();
                STimePoint NOW = new STimePoint("NOW", timePointType, timezone);
                STimePoint operateTime = new STimePoint(operateTimeObject);
                if (object instanceof Node objectNode) {
                    SInterval objectEffectiveTime = new SInterval(new STimePoint(objectNode.getProperty("intervalFrom")), new STimePoint(objectNode.getProperty("intervalTo")));
                    if (objectEffectiveTime.getIntervalTo().getSystemTimePoint().equals(NOW.getSystemTimePoint())) {
                        if (objectEffectiveTime.contains(operateTime)) {
                            // 设置实体的某个属性
                            Node propertyNode = ReadingQuery.getPropertyNode(objectNode, propertyName);
                            if (propertyNode != null) {
                                SInterval propertyEffectiveTime = new SInterval(new STimePoint(propertyNode.getProperty("intervalFrom")), new STimePoint(propertyNode.getProperty("intervalTo")));
                                if (propertyEffectiveTime.getIntervalTo().getSystemTimePoint().equals(NOW.getSystemTimePoint())) {
                                    if (propertyEffectiveTime.contains(operateTime)) {
                                        Node valueNode = getValueNode(propertyNode, operateTimeObject);
                                        if (valueNode != null) {
                                            SInterval valueEffectiveTime = new SInterval(new STimePoint(valueNode.getProperty("intervalFrom")), new STimePoint(valueNode.getProperty("intervalTo")));
                                            if (valueEffectiveTime.getIntervalTo().getSystemTimePoint().equals(NOW.getSystemTimePoint())) {
                                                // 物理删除已有值节点，staleValueNode返回待物理删除的值节点
                                                if (valueEffectiveTime.getIntervalFrom().isBefore(operateTime)) {
                                                    Map<String, List> staleValueNode = new HashMap<>();
                                                    List<Node> valueNodeList = new ArrayList<>();
                                                    valueNodeList.add(valueNode);
                                                    staleValueNode.put("staleValueNode", valueNodeList);
                                                    itemsToSetProperty.add(staleValueNode);
                                                } else {
                                                    throw new RuntimeException("The operate time must be latter than the start time of current value node. Please alter the operate time");
                                                }
                                            } else {
                                                throw new RuntimeException("The operate time fall in the effective time of a historical value node. Please alter the operate time");
                                            }
                                        }
                                        if (propertyValue != null) {
                                            // 仅创建值节点，createValueNode返回待创建的值节点所连接的属性节点
                                            List<Node> propertyNodeList = new ArrayList<>();
                                            propertyNodeList.add(propertyNode);
                                            itemToSetProperty.put("createValueNode", propertyNodeList);
                                        }
                                    } else {
                                        throw new RuntimeException("The effective time of property node must contain the effective time of it's value nodes. Please alter the operate time");
                                    }
                                } else {
                                    throw new RuntimeException("Can't add value nodes for historical property nodes");
                                }
                            } else {
                                // 创建属性节点和值节点，createPropertyNode返回待创建的属性节点所连接的对象节点
                                List<Node> objectNodeList = new ArrayList<>();
                                objectNodeList.add(objectNode);
                                itemToSetProperty.put("createPropertyNode", objectNodeList);
                            }
                        } else {
                            throw new RuntimeException("The effective time of object node must contain the effective time of it's value nodes. Please alter the operate time");
                        }
                    } else {
                        throw new RuntimeException("Can't set properties for historical nodes");
                    }
                } else if (object instanceof Relationship relationship) {
                    // 设置关系的某个属性，setRelationshipProperty返回待修改的关系
                    List<Relationship> relationshipList = new ArrayList<>();
                    relationshipList.add(relationship);
                    itemToSetProperty.put("setRelationshipProperty", relationshipList);
                } else {
                    throw new RuntimeException("Type mismatch: expected Node or Relationship but was " + object.getClass().getSimpleName());
                }
                itemsToSetProperty.add(itemToSetProperty);
                return itemsToSetProperty;
            } else {
                throw new RuntimeException("User can't set `intervalFrom` or `intervalTo` directly");
            }
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }


    /**
     * @param object            对象节点/关系
     * @param propertyMap       属性的键值对
     * @param operateTimeObject set的操作时间
     * @param isAdd             是否为+=
     * @return 返回staleNodes，createValueNodes，createPropertyNodes，setRelationshipProperties
     */
    @UserFunction("scypher.getItemsToSetProperties")
    @Description("Get the items to set their property.")
    public List<Map<String, List>> getItemsToSetProperties(@Name("object") Object object, @Name("propertyMap") Map<String, Object> propertyMap, @Name("operateTime") Object operateTimeObject, @Name("isAdd") boolean isAdd) {
        if (object != null) {
            List<Map<String, List>> itemsToSetProperties = new ArrayList<>();
            Map<String, List> itemToSetProperties = new HashMap<>();
            String timePointType = GlobalVariablesManager.getTimePointType();
            String timezone = GlobalVariablesManager.getTimezone();
            STimePoint NOW = new STimePoint("NOW", timePointType, timezone);
            STimePoint operateTime = new STimePoint(operateTimeObject);
            if (object instanceof Node objectNode) {
                // 设置实体的一组属性
                SInterval objectEffectiveTime = new SInterval(new STimePoint(objectNode.getProperty("intervalFrom")), new STimePoint(objectNode.getProperty("intervalTo")));
                if (objectEffectiveTime.getIntervalTo().getSystemTimePoint().equals(NOW.getSystemTimePoint())) {
                    if (objectEffectiveTime.contains(operateTime)) {
                        List<Node> nodeList = new ArrayList<>();
                        List<Node> propertyNodes = new ArrayList<>();
                        if (!isAdd) {
                            // 逻辑删除所有原来的值节点
                            propertyNodes = getPropertyNodes(objectNode, NOW.getSystemTimePoint());
                        } else {
                            // 逻辑删除被修改的值节点
                            for (String propertyName : propertyMap.keySet()) {
                                Node propertyNode = ReadingQuery.getPropertyNode(objectNode, propertyName);
                                if (propertyNode != null) {
                                    SInterval propertyEffectiveTime = new SInterval(new STimePoint(propertyNode.getProperty("intervalFrom")), new STimePoint(propertyNode.getProperty("intervalTo")));
                                    if (propertyEffectiveTime.getIntervalTo().getSystemTimePoint().equals(NOW.getSystemTimePoint())) {
                                        propertyNodes.add(propertyNode);
                                    }
                                }
                            }
                        }
                        for (Node propertyNode : propertyNodes) {
                            Node valueNode = getValueNode(propertyNode, NOW.getSystemTimePoint());
                            if (valueNode != null) {
                                SInterval valueEffectiveTime = new SInterval(new STimePoint(valueNode.getProperty("intervalFrom")), new STimePoint(valueNode.getProperty("intervalTo")));
                                if (valueEffectiveTime.getIntervalFrom().isBefore(operateTime)) {
                                    nodeList.add(valueNode);
                                } else {
                                    throw new RuntimeException("The operate time must be latter than the start time of current value node. Please alter the operate time");
                                }
                            }
                        }
                        if (nodeList.size() != 0) {
                            itemToSetProperties.put("staleNodes", nodeList);
                        }
                        // 创建新的值节点（和属性节点）
                        List<Node> createValueNodeList = new ArrayList<>();
                        List<Map<String, Object>> createPropertyNodeList = new ArrayList<>();
                        for (Map.Entry<String, Object> entry : propertyMap.entrySet()) {
                            if (!entry.getKey().equals("intervalFrom") && !entry.getKey().equals("intervalTo")) {
                                // 设置实体的某个属性
                                Node propertyNode = ReadingQuery.getPropertyNode(objectNode, entry.getKey());
                                if (propertyNode != null) {
                                    SInterval propertyEffectiveTime = new SInterval(new STimePoint(propertyNode.getProperty("intervalFrom")), new STimePoint(propertyNode.getProperty("intervalTo")));
                                    if (propertyEffectiveTime.getIntervalTo().getSystemTimePoint().equals(NOW.getSystemTimePoint())) {
                                        if (propertyEffectiveTime.contains(operateTime)) {
                                            Node valueNode = getValueNode(propertyNode, operateTimeObject);
                                            if (valueNode != null) {
                                                SInterval valueEffectiveTime = new SInterval(new STimePoint(valueNode.getProperty("intervalFrom")), new STimePoint(valueNode.getProperty("intervalTo")));
                                                if (!valueEffectiveTime.getIntervalTo().getSystemTimePoint().equals(NOW.getSystemTimePoint())) {
                                                    throw new RuntimeException("The operate time fall in the effective time of a historical value node. Please alter the operate time");
                                                }
                                            }
                                            if (entry.getValue() != null) {
                                                // 仅创建值节点，createValueNodes返回相连属性节点
                                                createValueNodeList.add(propertyNode);
                                            }
                                        } else {
                                            throw new RuntimeException("The effective time of property node must contain the effective time of it's value nodes. Please alter the operate time");
                                        }
                                    } else {
                                        throw new RuntimeException("Can't add value nodes for historical property nodes");
                                    }
                                } else {
                                    // 创建属性节点和值节点，createPropertyNodes返回属性节点的属性名和值节点的值
                                    Map<String, Object> createPropertyNode = new HashMap<>();
                                    createPropertyNode.put("propertyName", entry.getKey());
                                    createPropertyNode.put("propertyValue", entry.getValue());
                                    List<Node> objectNodeList = new ArrayList<>();
                                    objectNodeList.add(objectNode);
                                    createPropertyNode.put("objectNode", objectNodeList);
                                    createPropertyNodeList.add(createPropertyNode);
                                }
                            } else {
                                throw new RuntimeException("User can't set `intervalFrom` or `intervalTo` directly");
                            }
                        }
                        if (createValueNodeList.size() > 0) {
                            itemToSetProperties.put("createValueNodes", createValueNodeList);
                        }
                        if (createPropertyNodeList.size() > 0) {
                            itemToSetProperties.put("createPropertyNodes", createPropertyNodeList);
                        }
                    } else {
                        throw new RuntimeException("The effective time of object node must contain the effective time of it's value nodes. Please alter the operate time");
                    }
                } else {
                    throw new RuntimeException("Can't set properties for historical nodes");
                }
            } else if (object instanceof Relationship relationship) {
                // 设置关系的一组属性
                if (isAdd) {
                    // setRelationshipProperties返回关系
                    List<Relationship> relationshipList = new ArrayList<>();
                    relationshipList.add(relationship);
                    itemToSetProperties.put("setRelationshipProperties", relationshipList);
                } else {
                    // setRelationshipProperties返回待设置的属性map，将intervalFrom和intervalTo也添加到propertyMap
                    propertyMap.put("intervalFrom", relationship.getProperty("intervalFrom"));
                    propertyMap.put("intervalTo", relationship.getProperty("intervalTo"));
                    List<Map<String, Object>> propertyMapList = new ArrayList<>();
                    propertyMapList.add(propertyMap);
                    itemToSetProperties.put("setRelationshipProperties", propertyMapList);
                }
            } else {
                throw new RuntimeException("Type mismatch: expected Node or Relationship but was " + object.getClass().getSimpleName());
            }
            System.out.println(itemToSetProperties);
            itemsToSetProperties.add(itemToSetProperties);
            return itemsToSetProperties;
        } else {
            throw new RuntimeException("Missing parameter");
        }

    }

    /**
     * @param superiorNode            所属对象节点/属性节点
     * @param subordinateIntervalFrom 待设置的属性节点/值节点的开始时间
     * @return 如果属性节点/值节点的开始时间满足约束，则返回subordinateIntervalFrom；反之，则报错。
     */
    @UserFunction("scypher.getIntervalFromOfSubordinateNode")
    @Description("Get the start time of property node or value node.")
    public Object getIntervalFromOfSubordinateNode(@Name("superiorIntervalFrom") Node superiorNode, @Name("subordinateIntervalFrom") Object subordinateIntervalFrom) {
        if (superiorNode != null && subordinateIntervalFrom != null) {
            // 检查subordinateIntervalFrom的合法性
            if ((new STimePoint(superiorNode.getProperty("intervalFrom"))).isAfter(new STimePoint(subordinateIntervalFrom))) {
                throw new RuntimeException("The effective time of subordinate node must in the effective time of it's superior node");
            } else {
                return subordinateIntervalFrom;
            }
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param superiorNode          所属对象节点/属性节点
     * @param subordinateIntervalTo 待设置的属性节点/值节点的结束时间
     * @return 如果属性节点/值节点的结束时间满足约束，则返回subordinateIntervalTo；反之，则报错。
     */
    @UserFunction("scypher.getIntervalToOfSubordinateNode")
    @Description("Get the end time of property node or value node.")
    public Object getIntervalToOfSubordinateNode(@Name("superiorNode") Node superiorNode, @Name("subordinateIntervalTo") Object subordinateIntervalTo) {
        if (superiorNode != null && subordinateIntervalTo != null) {
            // 检查subordinateIntervalTo的合法性
            if ((new STimePoint(superiorNode.getProperty("intervalTo"))).isBefore(new STimePoint(subordinateIntervalTo))) {
                throw new RuntimeException("The effective time of subordinate node must in the effective time of it's superior node");
            } else {
                return subordinateIntervalTo;
            }
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param startNode                      开始节点
     * @param endNode                        结束节点
     * @param relationshipType               关系的类型
     * @param direction                      关系的方向
     * @param relationshipIntervalFromObject 待设置的关系的开始时间
     * @return 如果关系的开始时间满足约束，则返回relationshipIntervalFrom；反之，则报错。
     */
    @UserFunction("scypher.getIntervalFromOfRelationship")
    @Description("Get the start time of relationship.")
    public Object getIntervalFromOfRelationship(@Name("startNode") Node startNode, @Name("endNode") Node endNode, @Name("relationshipType") String relationshipType, @Name("direction") String direction, @Name("relationshipIntervalFrom") Object relationshipIntervalFromObject) {
        if (startNode != null && endNode != null && relationshipType != null && relationshipIntervalFromObject != null) {
            STimePoint startNodeIntervalFrom = new STimePoint(startNode.getProperty("intervalFrom"));
            STimePoint endNodeIntervalFrom = new STimePoint(endNode.getProperty("intervalFrom"));
            STimePoint relationshipIntervalFrom = new STimePoint(relationshipIntervalFromObject);
            // 检查relationshipIntervalFrom的合法性
            if (!startNodeIntervalFrom.isAfter(relationshipIntervalFrom) && !endNodeIntervalFrom.isAfter(relationshipIntervalFrom)) {
                if (direction.equalsIgnoreCase("RIGHT") | direction.equalsIgnoreCase("UNDIRECTED")) {
                    List<Relationship> relationships = startNode.getRelationships(Direction.OUTGOING, RelationshipType.withName(relationshipType)).stream().toList();
                    for (Relationship relationship : relationships) {
                        if (relationship.hasProperty("intervalFrom") && relationship.hasProperty("intervalTo")) {
                            SInterval relationInterval = new SInterval(new STimePoint(relationship.getProperty("intervalFrom")), new STimePoint(relationship.getProperty("intervalTo")));
                            if (relationship.getEndNode().equals(endNode) && relationInterval.contains(relationshipIntervalFrom)) {
                                throw new RuntimeException("For relationships with the same content, start node and end node, their effective times do not overlap with each other");
                            }
                        }
                    }
                }
                if (direction.equalsIgnoreCase("LEFT") | direction.equalsIgnoreCase("UNDIRECTED")) {
                    List<Relationship> relationships = endNode.getRelationships(Direction.OUTGOING, RelationshipType.withName(relationshipType)).stream().toList();
                    for (Relationship relationship : relationships) {
                        if (relationship.hasProperty("intervalFrom") && relationship.hasProperty("intervalTo")) {
                            SInterval relationInterval = new SInterval(new STimePoint(relationship.getProperty("intervalFrom")), new STimePoint(relationship.getProperty("intervalTo")));
                            if (relationship.getEndNode().equals(startNode) && relationInterval.contains(relationshipIntervalFrom)) {
                                throw new RuntimeException("For relationships with the same content, start node and end node, their effective times do not overlap with each other");
                            }
                        }
                    }
                }
                return relationshipIntervalFromObject;
            } else {
                throw new RuntimeException("The effective time of relationship must in the effective time of it's start node and end node at the same time");
            }
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param startNode                    开始节点
     * @param endNode                      结束节点
     * @param relationshipType             关系的类型
     * @param direction                    关系的方向
     * @param relationshipIntervalToObject 待设置的关系的结束时间
     * @return 如果关系的结束时间满足约束，则返回relationshipIntervalTo；反之，则报错。
     */
    @UserFunction("scypher.getIntervalToOfRelationship")
    @Description("Get the end time of relationship.")
    public Object getIntervalToOfRelationship(@Name("startNode") Node startNode, @Name("endNode") Node endNode, @Name("relationshipType") String relationshipType, @Name("direction") String direction, @Name("relationshipIntervalTo") Object relationshipIntervalToObject) {
        if (startNode != null && endNode != null && relationshipType != null && relationshipIntervalToObject != null) {
            STimePoint startNodeIntervalTo = new STimePoint(startNode.getProperty("intervalTo"));
            STimePoint endNodeIntervalTo = new STimePoint(endNode.getProperty("intervalTo"));
            STimePoint relationshipIntervalTo = new STimePoint(relationshipIntervalToObject);
            // 检查relationshipIntervalTo的合法性
            if (!startNodeIntervalTo.isBefore(relationshipIntervalTo) && !endNodeIntervalTo.isBefore(relationshipIntervalTo)) {
                if (direction.equalsIgnoreCase("RIGHT") | direction.equalsIgnoreCase("UNDIRECTED")) {
                    List<Relationship> relationships = startNode.getRelationships(Direction.OUTGOING, RelationshipType.withName(relationshipType)).stream().toList();
                    for (Relationship relationship : relationships) {
                        if (relationship.hasProperty("intervalFrom") && relationship.hasProperty("intervalTo")) {
                            SInterval relationInterval = new SInterval(new STimePoint(relationship.getProperty("intervalFrom")), new STimePoint(relationship.getProperty("intervalTo")));
                            if (relationship.getEndNode().equals(endNode) && relationInterval.contains(relationshipIntervalTo)) {
                                throw new RuntimeException("For relationships with the same content, start node and end node, their effective times do not overlap with each other");
                            }
                        }
                    }
                }
                if (direction.equalsIgnoreCase("LEFT") | direction.equalsIgnoreCase("UNDIRECTED")) {
                    List<Relationship> relationships = endNode.getRelationships(Direction.OUTGOING, RelationshipType.withName(relationshipType)).stream().toList();
                    for (Relationship relationship : relationships) {
                        if (relationship.hasProperty("intervalFrom") && relationship.hasProperty("intervalTo")) {
                            SInterval relationInterval = new SInterval(new STimePoint(relationship.getProperty("intervalFrom")), new STimePoint(relationship.getProperty("intervalTo")));
                            if (relationship.getEndNode().equals(startNode) && relationInterval.contains(relationshipIntervalTo)) {
                                throw new RuntimeException("For relationships with the same content, start node and end node, their effective times do not overlap with each other");
                            }
                        }
                    }
                }
                return relationshipIntervalToObject;
            } else {
                throw new RuntimeException("The effective time of relationship must in the effective time of it's start node and end node at the same time");
            }
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }
}
