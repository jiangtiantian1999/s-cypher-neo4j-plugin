package cn.scypher.neo4j.plugin;

import cn.scypher.neo4j.plugin.datetime.STimePoint;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UpdatingQuery {
    /**
     * @param object       对象节点/路径/关系
     * @param propertyName 属性名
     * @param timeWindow   删除的值节点的范围
     * @return 以列表形式返回所有待物理删除的元素
     */
    @UserFunction("scypher.getItemsToDelete")
    @Description("Get the items to delete.")
    public List<Object> getItemsToDelete(@Name("object") Object object, @Name("propertyName") String propertyName, @Name("timeWindow") Object timeWindow) {
        if (object != null) {
            List<Object> itemsToDelete = new ArrayList<>();
            //TODO
            return itemsToDelete;
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param object          对象节点/关系
     * @param propertyName    属性名
     * @param deleteValueNode 是否仅逻辑删除值节点
     * @param timeWindow      删除的值节点的范围
     * @return 以列表形式返回所有待逻辑删除的元素
     */
    @UserFunction("scypher.getItemsToStale")
    @Description("Get the items to stale.")
    public List<Object> getItemsToStale(@Name("object") Object object, @Name("propertyName") String propertyName, @Name("deleteValueNode") boolean deleteValueNode, @Name("timeWindow") Object timeWindow) {
        if (object != null) {
            List<Object> itemsToStale = new ArrayList<>();
            //TODO
            return itemsToStale;
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param objectInfo         对象节点/关系及其有效时间，为map类型，有两个key：objectNode和effectiveTime
     * @param propertyInfo       属性名及属性节点的有效时间，为map类型，有两个key：propertyName和effectiveTime
     * @param valueEffectiveTime 值节点的有效时间，为时间区间
     * @param operateTime        set语句的操作时间，为时间点。修改值节点的有效时间时，修改在该时间有效的值节点的有效时间
     * @return 以列表形式返回所有待设置的键值对，返回前需检查有效时间的约束
     */
    @UserFunction("scypher.getItemsToSetEffectiveTime")
    @Description("Get the items to set their effective time.")
    public List<Map<String, Object>> getItemsToSetEffectiveTime(@Name("objectInfo") Map<String, Object> objectInfo, @Name("propertyInfo") Map<String, Object> propertyInfo, @Name("valueEffectiveTime") Map<String, Object> valueEffectiveTime, @Name("operateTime") Object operateTime) {
        if (objectInfo != null) {
            List<Map<String, Object>> itemsToSetEffectiveTime = new ArrayList<>();
            //TODO
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
    @UserFunction("scypher.getIntervalFromOfRelationship")
    @Description("Get the start time of relationship.")
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
