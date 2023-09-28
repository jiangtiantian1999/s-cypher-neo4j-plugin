package cn.scypher.neo4j.plugin;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

public class GlobalVariableManage {

    public Transaction tx;

    private Node globalVariableNode;

    GlobalVariableManage(Transaction tx) {
        this.tx = tx;
        ResourceIterator<Node> nodes = this.tx.findNodes(Label.label("GlobalVariable"));
        if (nodes.hasNext()) {
            this.globalVariableNode = nodes.next();
        } else {
            throw new RuntimeException("The initial parameters of the database were not set.");
        }
    }

    public Node getGlobalVariableNode() {
        return this.globalVariableNode;
    }

    /**
     * @return 获取默认时区
     */
    public String getTimeZone() {
        if (this.globalVariableNode.hasProperty("timezone")) {
            return (String) this.globalVariableNode.getProperty("timezone");
        }
        return null;
    }

    public String getTimeGranularity() {
        if (this.globalVariableNode.hasProperty("timeGranularity")) {
            return (String) this.globalVariableNode.getProperty("timeGranularity");
        } else {
            throw new RuntimeException("The Time granularity not set.");
        }
    }

    public void setProperty(String key, Object value) {
        this.globalVariableNode.setProperty(key, value);
    }

}
