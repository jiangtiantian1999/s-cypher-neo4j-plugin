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
public class ReadingQueryTest {
    private Driver driver;
    private Neo4j embeddedDatabaseServer;
    private Session session;

    @BeforeAll
    void initializeNeo4j() {
        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
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
    public void testGetPropertyValue() {
        System.out.println("testGetPropertyValue");
        this.session.run("CREATE (n:Person:Object {intervalFrom: scypher.timePoint('2010'), intervalTo: scypher.timePoint('NOW')})-[:OBJECT_PROPERTY]->" +
                "(p:Property {content:'name', intervalFrom: scypher.timePoint('2010'), intervalTo: scypher.timePoint('NOW')})-[:PROPERTY_VALUE]->" +
                "(v1:Value {content:'Nick', intervalFrom: scypher.timePoint('2010'), intervalTo: scypher.timePoint('2022')})," +
                "(p)-[:PROPERTY_VALUE]->(v2:Value {content:'Tom', intervalFrom: scypher.timePoint('2023'), intervalTo: scypher.timePoint('NOW')})");
        List<Record> records = this.session.run("MATCH (n:Person)" +
                "RETURN scypher.getPropertyValue(n,'name',NULL)").list();
        for (Record record : records) {
            System.out.println(record);
        }
        records = this.session.run("MATCH (n:Person)" +
                "RETURN scypher.getPropertyValue(n, 'name', scypher.interval('2010','2022'))").list();
        for (Record record : records) {
            System.out.println(record);
        }
        records = this.session.run("RETURN scypher.getPropertyValue(duration({months:10}), 'quarters', NULL)").list();
        for (Record record : records) {
            System.out.println(record);
        }
        records = this.session.run("RETURN scypher.getPropertyValue(scypher.timePoint('NOW'), 'timezone', NULL)").list();
        for (Record record : records) {
            System.out.println(record);
        }
        records = this.session.run("RETURN scypher.getPropertyValue(point({latitude: 12, longitude: 56, height: 1000}), 'x', null)").list();
        for (Record record : records) {
            System.out.println(record);
        }
    }

    @Test
    public void testGetObjectEffectiveTime() {
        System.out.println("testGetObjectEffectiveTime");
        this.session.run("CREATE (n:Person:Object {intervalFrom: scypher.timePoint('2010'), intervalTo: scypher.timePoint('NOW')})-[:OBJECT_PROPERTY]->" +
                "(p:Property {content:'name', intervalFrom: scypher.timePoint('2010'), intervalTo: scypher.timePoint('NOW')})-[:PROPERTY_VALUE]->" +
                "(v1:Value {content:'Nick', intervalFrom: scypher.timePoint('2010'), intervalTo: scypher.timePoint('2022')})," +
                "(p)-[:PROPERTY_VALUE]->(v2:Value {content:'Tom', intervalFrom:scypher.timePoint('2023'), intervalTo:scypher.timePoint('NOW')})");
        List<Record> records = this.session.run("MATCH (n:Person)" +
                "RETURN scypher.getObjectEffectiveTime(n)").list();
        for (Record record : records) {
            System.out.println(record);
        }
    }

    @Test
    public void testGetPropertyEffectiveTime() {
        System.out.println("testGetPropertyEffectiveTime");
        this.session.run("CREATE (n:Person:Object {intervalFrom: scypher.timePoint('2010'), intervalTo: scypher.timePoint('NOW')})-[:OBJECT_PROPERTY]->" +
                "(p:Property {content:'name', intervalFrom: scypher.timePoint('2010'), intervalTo: scypher.timePoint('NOW')})-[:PROPERTY_VALUE]->" +
                "(v1:Value {content:'Nick', intervalFrom: scypher.timePoint('2010'), intervalTo: scypher.timePoint('2022')})," +
                "(p)-[:PROPERTY_VALUE]->(v2:Value {content:'Tom', intervalFrom: scypher.timePoint('2023'), intervalTo: scypher.timePoint('NOW')})");
        List<Record> records = this.session.run("MATCH (n:Person)" +
                "RETURN scypher.getPropertyEffectiveTime(n, 'name')").list();
        for (Record record : records) {
            System.out.println(record);
        }
    }

    @Test
    public void testGetValueEffectiveTime() {
        System.out.println("testGetValueEffectiveTime");
        this.session.run("CREATE (n:Person:Object {intervalFrom: scypher.timePoint('2010'), intervalTo: scypher.timePoint('NOW')})-[:OBJECT_PROPERTY]->" +
                "(p:Property {content:'name', intervalFrom: scypher.timePoint('2010'), intervalTo: scypher.timePoint('NOW')})-[:PROPERTY_VALUE]->" +
                "(v1:Value {content:'Nick', intervalFrom: scypher.timePoint('2010'), intervalTo: scypher.timePoint('2022')})," +
                "(p)-[:PROPERTY_VALUE]->(v2:Value {content:'Tom', intervalFrom: scypher.timePoint('2023'), intervalTo:scypher.timePoint('NOW')})");
        List<Record> records = this.session.run("MATCH (n:Person)" +
                "RETURN scypher.getValueEffectiveTime(n, 'name', NULL)").list();
        for (Record record : records) {
            System.out.println(record);
        }
        records = this.session.run("MATCH (n:Person)" +
                "RETURN scypher.getValueEffectiveTime(n, 'name', scypher.interval('2010','2022'))").list();
        for (Record record : records) {
            System.out.println(record);
        }
    }
}
