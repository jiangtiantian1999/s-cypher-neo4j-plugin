package cn.scypher.neo4j.plugin;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class UpdatingQueryTest {
    private Driver driver;
    private Neo4j embeddedDatabaseServer;
    private Session session;

    @BeforeAll
    void initializeNeo4j() {
        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .withFunction(UpdatingQuery.class)
                .withFunction(ReadingQuery.class)
                .withFunction(SDateTimeOperation.class)
                .build();
        this.driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI());
        this.session = driver.session();
    }

    @AfterAll
    void closeNeo4j() {
        this.driver.close();
        this.embeddedDatabaseServer.close();
    }

    @Test
    public void testGetItemsToDelete() {
        System.out.println("testGetItemsToDelete");
        this.session.run("CREATE (n:Person:Object {intervalFrom:scypher.timePoint('2010'), intervalTo:scypher.timePoint('NOW')})-[:OBJECT_PROPERTY]->" +
                "(p:Property {content:'name', intervalFrom:scypher.timePoint('2010'), intervalTo:scypher.timePoint('NOW')})-[:PROPERTY_VALUE]->" +
                "(v1:Value {content:'Nick', intervalFrom:scypher.timePoint('2010'), intervalTo:scypher.timePoint('2022')})," +
                "(p)-[:PROPERTY_VALUE]->(v2:Value {content:'Tom', intervalFrom:scypher.timePoint('2023'), intervalTo:scypher.timePoint('NOW')})");
        List<Record> records = this.session.run("MATCH (n:Person)" +
                "RETURN scypher.getPropertyValue(n,'name',NULL)").list();
        for (Record record : records) {
            System.out.println(record);
        }
        this.session.run("MATCH (n:Person)" +
                "FOREACH(item in scypher.getItemsToDelete(n,'name',NULL)|delete item)");
        records = this.session.run("MATCH (n:Person)" +
                "RETURN scypher.getPropertyValue(n,'name',NULL)").list();
        for (Record record : records) {
            System.out.println(record);
        }
    }

    @Test
    public void testGetItemsToStale() {
        System.out.println("testGetItemsToStale");
        this.session.run("CREATE (n:Person:Object {intervalFrom:scypher.timePoint('2010'), intervalTo:scypher.timePoint('NOW')})-[:OBJECT_PROPERTY]->" +
                "(p:Property {content:'name', intervalFrom:scypher.timePoint('2010'), intervalTo:scypher.timePoint('NOW')})-[:PROPERTY_VALUE]->" +
                "(v1:Value {content:'Nick', intervalFrom:scypher.timePoint('2010'), intervalTo:scypher.timePoint('2022')})," +
                "(p)-[:PROPERTY_VALUE]->(v2:Value {content:'Tom', intervalFrom:scypher.timePoint('2023'), intervalTo:scypher.timePoint('NOW')})");
        List<Record> records = this.session.run("MATCH (n:Person)-->(p:Property)-->(v:Value)" +
                "RETURN n.intervalTo, p.intervalTo, v.intervalTo").list();
        for (Record record : records) {
            System.out.println(record);
        }
        this.session.run("MATCH (n:Person)" +
                "FOREACH(item in scypher.getItemsToStale(n,'name',true,NULL)| set item.intervalTo = scypher.timePoint.current() )");
        records = this.session.run("MATCH (n:Person)-->(p:Property)-->(v:Value)" +
                "RETURN n.intervalTo, p.intervalTo, v.intervalTo").list();
        for (Record record : records) {
            System.out.println(record);
        }
    }
}
