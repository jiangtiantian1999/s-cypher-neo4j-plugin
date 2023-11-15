package cn.scypher.neo4j.plugin;

import cn.scypher.neo4j.plugin.datetime.SInterval;
import cn.scypher.neo4j.plugin.datetime.STimePoint;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpdatingQuery {

    public static List<Node> getPropertyNodes(Node objectNode) {
        List<Node> propertyNodeList = new ArrayList<>();
        ResourceIterable<Relationship> relationships = objectNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("OBJECT_PROPERTY"));
        for (Relationship relationship : relationships) {
            propertyNodeList.add(relationship.getEndNode());
        }
        return propertyNodeList;
    }

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
     * @return 返回对象节点的所有属性节点和值节点以及相连边
     */
    public static List<Object> deleteObjectNode(Node objectNode) {
        // 对象节点的所有属性节点
        List<Node> propertyNodes = getPropertyNodes(objectNode);
        // 对象节点与其所有属性节点之间的边
        List<Relationship> objectPropertyEdges = new ArrayList<>();
        // 对象节点的所有值节点
        List<Node> valueNodes = new ArrayList<>();
        // 属性节点与其所有值节点之间的边
        List<Relationship> propertyValueEdges = new ArrayList<>();
        for (Node propertyNode : propertyNodes) {
            objectPropertyEdges.add(propertyNode.getSingleRelationship(RelationshipType.withName("OBJECT_PROPERTY"), Direction.INCOMING));
            valueNodes.addAll(getValueNodes(propertyNode));
        }
        for (Node valueNode : valueNodes) {
            propertyValueEdges.add(valueNode.getSingleRelationship(RelationshipType.withName("PROPERTY_VALUE"), Direction.INCOMING));
        }
        List<Object> itemsToDelete = new ArrayList<>();
        itemsToDelete.addAll(propertyValueEdges);
        itemsToDelete.addAll(valueNodes);
        itemsToDelete.addAll(objectPropertyEdges);
        itemsToDelete.addAll(propertyNodes);
        return itemsToDelete;
    }

    /**
     * @param object       对象节点/关系/路径
     * @param propertyName 属性名
     * @param timeWindow   删除的值节点的范围/[是否仅删除值节点]
     * @return 以列表形式返回所有待物理删除的元素
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
                        } else {
                            // 仅物理删除值节点
                            if (timeWindow instanceof Boolean) {
                                valueNodes = ReadingQuery.getValueNodes(propertyNode, null);
                            } else {
                                valueNodes = ReadingQuery.getValueNodes(propertyNode, timeWindow);
                            }
                        }
                        // 属性节点与其所有值节点之间的边
                        List<Relationship> propertyValueEdges = new ArrayList<>();
                        for (Node valueNode : valueNodes) {
                            propertyValueEdges.add(valueNode.getSingleRelationship(RelationshipType.withName("PROPERTY_VALUE"), Direction.INCOMING));
                        }
                        itemsToDelete.addAll(propertyValueEdges);
                        itemsToDelete.addAll(valueNodes);
                        if (timeWindow == null) {
                            // 物理删除属性节点和值节点
                            itemsToDelete.add(propertyNode.getSingleRelationship(RelationshipType.withName("OBJECT_PROPERTY"), Direction.INCOMING));
                            itemsToDelete.add(propertyNode);
                        }
                    }
                } else {
                    // 物理删除对象节点，并删除其所有属性节点和值节点以及相连边
                    itemsToDelete.addAll(deleteObjectNode(objectNode));
                    itemsToDelete.add(objectNode);
                }
            } else if (object instanceof Relationship relationship) {
                // 物理删除关系
                itemsToDelete.add(relationship);
            } else if (object instanceof Path path) {
                // 物理删除路径，并删除路径上所有对象节点的所有属性节点和值节点以及相连边
                for (Node objectNode : path.nodes()) {
                    itemsToDelete.addAll(deleteObjectNode(objectNode));
                }
                itemsToDelete.add(path);
            } else {
                throw new RuntimeException("Type mismatch: expected Node, Relationship or Path but was " + object.getClass().getSimpleName());
            }
            return itemsToDelete;
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param object          对象节点/关系
     * @param propertyName    属性名
     * @param deleteValueNode 是否仅逻辑删除值节点
     * @param operateTime     逻辑删除时间
     * @return 以列表形式返回所有待逻辑删除的元素
     */
    @UserFunction("scypher.getItemsToStale")
    @Description("Get the items to stale.")
    public List<Object> getItemsToStale(@Name("object") Object object, @Name("propertyName") String propertyName, @Name("deleteValueNode") boolean deleteValueNode, @Name("operateTime") Object operateTime) {
        if (object != null) {
            List<Object> itemsToStale = new ArrayList<>();
            if (object instanceof Node objectNode) {
                String timePointType = GlobalVariablesManager.getTimePointType();
                String timezone = GlobalVariablesManager.getTimezone();
                STimePoint NOW = new STimePoint("NOW", timePointType, timezone);
                if (propertyName != null) {
                    // 逻辑删除对象节点的属性
                    Node propertyNode = ReadingQuery.getPropertyNode(objectNode, propertyName);
                    if (propertyNode != null && propertyNode.getProperty("intervalTo").equals(NOW.getSystemTimePoint())) {
                        List<Node> valueNodes;
                        if (!deleteValueNode) {
                            // 逻辑删除属性节点和值节点
                            valueNodes = getValueNodes(propertyNode);
                            itemsToStale.add(propertyNode);
                        } else {
                            // 仅逻辑删除值节点
                            valueNodes = ReadingQuery.getValueNodes(propertyNode, operateTime);
                        }
                        for (Node valueNode : valueNodes) {
                            if (valueNode.getProperty("intervalTo").equals(NOW.getSystemTimePoint())) {
                                itemsToStale.add(valueNode);
                                break;
                            }
                        }
                    }
                } else {
                    // 逻辑删除对象节点，并逻辑删除其所有属性节点和值节点
                    if (objectNode.getProperty("intervalTo").equals(NOW.getSystemTimePoint())) {
                        itemsToStale.add(objectNode);
                        List<Node> propertyNodes = getPropertyNodes(objectNode);
                        for (Node propertyNode : propertyNodes) {
                            if (propertyNode.getProperty("intervalTo").equals(NOW.getSystemTimePoint())) {
                                itemsToStale.add(propertyNode);
                                List<Node> valueNodes = getValueNodes(propertyNode);
                                for (Node valueNode : valueNodes) {
                                    if (valueNode.getProperty("intervalTo").equals(NOW.getSystemTimePoint())) {
                                        itemsToStale.add(valueNode);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            } else if (object instanceof Relationship relationship) {
                // 逻辑删除关系
                itemsToStale.add(relationship);
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
     * @param operateTime           set语句的操作时间，为时间点。修改值节点的有效时间时，修改在该时间有效的值节点的有效时间
     * @return 返回对象节点/关系/属性节点/值节点及其待设置的有效时间，返回前需检查有效时间的约束
     */
    @UserFunction("scypher.getItemsToSetEffectiveTime")
    @Description("Get the info of items to set effective time.")
    public List<Map<String, Object>> getItemsToSetEffectiveTime(@Name("objectInfo") Map<String, Object> objectInfo, @Name("propertyInfo") Map<String, Object> propertyInfo, @Name("valueEffectiveTime") Map<String, Object> valueEffectiveTimeMap, @Name("operateTime") Object operateTime) {
        if (objectInfo != null) {
            List<Map<String, Object>> itemsToSetEffectiveTime = new ArrayList<>();
            // 修改对象节点/属性节点/值节点的有效时间
            if (objectInfo.get("object") instanceof Node objectNode) {
                SInterval objectEffectiveTime = null;
                if (objectInfo.containsKey("effectiveTime")) {
                    objectEffectiveTime = new SInterval((Map<String, Object>) objectInfo.get("effectiveTime"));
                    // 检查对象节点的有效时间是否满足约束
                    ResourceIterable<Relationship> relationships = objectNode.getRelationships();
                    for (Relationship relationship : relationships) {
                        if (!relationship.isType(RelationshipType.withName("OBJECT_PROPERTY")) && !relationship.isType(RelationshipType.withName("PROPERTY_VALUE"))) {
                            SInterval relationshipEffectiveTime = new SInterval(new STimePoint(relationship.getProperty("intervalFrom")), new STimePoint(relationship.getProperty("intervalTo")));
                            if (!objectEffectiveTime.contains(relationshipEffectiveTime)) {
                                throw new RuntimeException("The effective time of object node must contain the effective time of it's relationships");
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
                        if (propertyInfo.containsKey("effectiveTime")) {
                            propertyEffectiveTime = new SInterval((Map<String, Object>) (propertyInfo.get("effectiveTime")));
                            // 检查属性节点的有效时间是否满足约束
                            if (objectEffectiveTime.contains(propertyEffectiveTime)) {
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
                            // 检查值节点的有效时间是否满足约束
                            if (propertyEffectiveTime.contains(valueEffectiveTime)) {
                                // 值节点的有效时间满足约束
                                List<Node> valueNodes = ReadingQuery.getValueNodes(propertyNode, operateTime);
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
     * @param object        对象节点/关系
     * @param propertyName  属性名
     * @param operateTime   set的操作时间，修改在该时刻有效的值节点，或添加开始时间为该时刻的值节点
     * @param isAdd         是否为+=，默认=
     * @param propertyValue 要赋予的属性值
     * @return 修改节点/关系的属性
     */
    @UserFunction("scypher.getItemsToSetExpression")
    @Description("Get the items to set their property.")
    public List<Map<String, Object>> getItemsToSetExpression(@Name("object") Object object, @Name("propertyName") String propertyName, @Name("operateTime") Object operateTime, @Name("isAdd") boolean isAdd, @Name("propertyValue") Object propertyValue) {
        if (object != null) {
            List<Map<String, Object>> itemsToSetExpression = new ArrayList<>();
            //TODO
            return itemsToSetExpression;
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
    public Object getIntervalFromOfSubordinateNode(@Name("superiorNode") Node superiorNode, @Name("subordinateIntervalFrom") Object subordinateIntervalFrom) {
        if (superiorNode != null && subordinateIntervalFrom != null) {
            STimePoint intervalFrom = new STimePoint(subordinateIntervalFrom);
            //TODO 检查subordinateIntervalFrom的合法性
            return subordinateIntervalFrom;
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
            STimePoint intervalTo = new STimePoint(subordinateIntervalTo);
            //TODO 检查subordinateIntervalTo的合法性
            return subordinateIntervalTo;
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param startNode                开始节点
     * @param endNode                  结束节点
     * @param relationshipIntervalFrom 待设置的关系的开始时间
     * @return 如果关系的开始时间满足约束，则返回relationshipIntervalFrom；反之，则报错。
     */
    @UserFunction("scypher.getIntervalFromOfRelationship")
    @Description("Get the start time of relationship.")
    public Object getIntervalFromOfRelationship(@Name("startNode") Node startNode, @Name("endNode") Node endNode, @Name("relationshipIntervalFrom") Object relationshipIntervalFrom) {
        if (startNode != null && endNode != null && relationshipIntervalFrom != null) {
            STimePoint intervalFrom = new STimePoint(relationshipIntervalFrom);
            //TODO 检查relationshipIntervalFrom的合法性
            return relationshipIntervalFrom;
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param startNode              开始节点
     * @param endNode                结束节点
     * @param relationshipIntervalTo 待设置的关系的结束时间
     * @return 如果关系的结束时间满足约束，则返回relationshipIntervalTo；反之，则报错。
     */
    @UserFunction("scypher.getIntervalToOfRelationship")
    @Description("Get the end time of relationship.")
    public Object getIntervalToOfRelationship(@Name("startNode") Node startNode, @Name("endNode") Node endNode, @Name("relationshipIntervalTo") Object relationshipIntervalTo) {
        if (startNode != null && endNode != null && relationshipIntervalTo != null) {
            STimePoint intervalTo = new STimePoint(relationshipIntervalTo);
            //TODO 检查relationshipIntervalTo的合法性
            return relationshipIntervalTo;
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }
}
