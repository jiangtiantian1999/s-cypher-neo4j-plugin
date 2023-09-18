package cn.scypher.neo4j.plugin;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import scala.None;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class SPattern {
    enum SRelationshipTypes implements RelationshipType {
        OBJECT_PROPERTY,
        PROPERTY_VALUE
    }

    @UserFunction("scypher.getPropertyNode")
    @Description("Get the property node of the object node based on the property name.")
    public List<Node> getPropertyNode(@Name("objectNode") Node objectNode,
                                      @Name("propertyName") String propertyName) {
        ResourceIterable<Relationship> relationships = objectNode.getRelationships(Direction.OUTGOING,
                SRelationshipTypes.OBJECT_PROPERTY);
        List<Node> propertyNodes = new ArrayList<>();
        for (Relationship relationship : relationships) {
            Node propertyNode = relationship.getEndNode();
            if (propertyNode.getProperty("content").equals(propertyName)) {
                propertyNodes.add(propertyNode);
            }
        }
        return propertyNodes;
    }
}
