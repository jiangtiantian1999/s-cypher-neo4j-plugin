package cn.scypher.neo4j.plugin;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.ArrayList;
import java.util.List;

public class ReadingQuery {

    @Context
    public Transaction tx;

    @UserFunction("scypher.getPropertyValue")
    @Description("Get the property value of object node.")
    public List<Object> getPropertyValue(@Name("node") Node objectNode, @Name("propertyName") String propertyName, @Name("timeWindow") Object timeWindow) {
        if (objectNode != null && propertyName != null) {
            List<Object> propertyValueList = new ArrayList<>();
            //TODO
            return propertyValueList;
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }
}
