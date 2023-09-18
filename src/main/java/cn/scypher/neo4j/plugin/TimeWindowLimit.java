package cn.scypher.neo4j.plugin;


import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.*;
import java.util.HashMap;
import java.util.Map;

public class TimeWindowLimit {
    @Context
    public Transaction tx;


    @Procedure(name = "scypher.snapshot", mode = Mode.WRITE)
    @Description("SNAPSHOT operation.")
    public void snapshot(@Name("time_point") Object time_point) {
        String time_granularity;
        if (time_point instanceof LocalDate) {
            time_granularity = "date";
        } else if (time_point instanceof OffsetTime) {
            time_granularity = "time";
        } else if (time_point instanceof LocalTime) {
            time_granularity = "localtime";
        } else if (time_point instanceof ZonedDateTime) {
            time_granularity = "datetime";
        } else if (time_point instanceof LocalDateTime) {
            time_granularity = "localdatetime";
        } else {
            throw new RuntimeException("Invalid call signature for SnapshotFunction: Provided input was " + time_point.getClass() + ".");
        }
        ResourceIterator<Node> nodes = tx.findNodes(Label.label("GlobalVariable"));
        Node node;
        if (nodes.hasNext()) {
            node = nodes.next();
            if (node.hasProperty("time_granularity")) {
                if (!node.getProperty("time_granularity").toString().equals(time_granularity)) {
                    throw new RuntimeException("The time granularity can't match.The time granularity of database is '" + time_granularity + "'.");
                }
            } else {
                node.setProperty("time_granularity", time_granularity);
            }
        } else {
            node = tx.createNode(Label.label("GlobalVariable"));
            node.setProperty("time_granularity", time_granularity);
        }
        node.setProperty("snapshot", time_granularity);
    }

    @Procedure(name = "scypher.scope", mode = Mode.WRITE)
    @Description("SCOPE operation.")
    public void scope(@Name("interval") Map<String, Object> interval) {
        String time_granularity;
        String time_point_type;
        if (interval.containsKey("from") && interval.containsKey("to")) {
            if (interval.get("from").getClass() == interval.get("to").getClass()) {
                time_point_type = interval.get("from").getClass().toString();
                if (time_point_type.equals(LocalDate.class.toString())) {
                    time_granularity = "date";
                } else if (time_point_type.equals(OffsetTime.class.toString())) {
                    time_granularity = "time";
                } else if (time_point_type.equals(LocalTime.class.toString())) {
                    time_granularity = "localtime";
                } else if (time_point_type.equals(ZonedDateTime.class.toString())) {
                    time_granularity = "datetime";
                } else if (time_point_type.equals(LocalDateTime.class.toString())) {
                    time_granularity = "localdatetime";
                } else {
                    throw new RuntimeException("Invalid call signature for SnapshotFunction: Provided input was " + time_point_type + ".");
                }
            } else {
                throw new RuntimeException("The type of from and to of the interval is different.");
            }
        } else {
            throw new RuntimeException("The interval without from or to");
        }
        ResourceIterator<Node> nodes = tx.findNodes(Label.label("GlobalVariable"));
        Node node;
        if (nodes.hasNext()) {
            node = nodes.next();
            if (node.hasProperty("time_granularity")) {
                if (!node.getProperty("time_granularity").toString().equals(time_granularity)) {
                    throw new RuntimeException("The time granularity can't match.The time granularity of database is '" + time_granularity + "'.");
                }
            } else {
                node.setProperty("time_granularity", time_granularity);
            }
        } else {
            node = tx.createNode(Label.label("GlobalVariable"));
            node.setProperty("time_granularity", time_granularity);
        }
        node.setProperty("scope_from", interval.get("from"));
        node.setProperty("scope_to", interval.get("to"));
    }
}