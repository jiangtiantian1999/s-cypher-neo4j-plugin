package cn.scypher.neo4j.plugin;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.HashMap;
import java.util.Map;

public class SPatternMatch {

    @Context
    public Transaction tx;

    enum SRelationshipTypes implements RelationshipType {
        OBJECT_PROPERTY,
        PROPERTY_VALUE
    }
}
