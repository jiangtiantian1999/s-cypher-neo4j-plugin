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
                .withFunction(TimeWindowLimit.class)
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

    @Test
    public void testGetItemsToSetEffectiveTime() {
        System.out.println("testGetItemsToSetEffectiveTime");
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
                "FOREACH(item in scypher.getItemsToSetEffectiveTime({object:n},{propertyName:'name'},scypher.interval('2022','2023'),scypher.operateTime())" +
                "| set item.item.intervalFrom = item.intervalFrom, item.item.intervalTo = item.intervalTo )");
        records = this.session.run("MATCH (n:Person)-->(p:Property)-->(v:Value)" +
                "RETURN n.intervalTo, p.intervalTo, v.intervalTo").list();
        for (Record record : records) {
            System.out.println(record);
        }
    }

    @Test
    public void testGetItemsToSetExpression() {
        System.out.println("testGetItemsToSetExpression");
        this.session.run("CREATE (n:Person:Object {intervalFrom:scypher.timePoint('2010'), intervalTo:scypher.timePoint('NOW')})-[:OBJECT_PROPERTY]->" +
                "(p:Property {content:'name', intervalFrom:scypher.timePoint('2010'), intervalTo:scypher.timePoint('NOW')})-[:PROPERTY_VALUE]->" +
                "(v1:Value {content:'Nick', intervalFrom:scypher.timePoint('2010'), intervalTo:scypher.timePoint('2021')})," +
                "(p)-[:PROPERTY_VALUE]->(v2:Value {content:'Tom', intervalFrom:scypher.timePoint('2022'), intervalTo:scypher.timePoint('NOW')})");
        List<Record> records = this.session.run("MATCH (n:Person)-->(p:Property)-->(v:Value)" +
                "RETURN n, p.content, v.content, v.intervalFrom, v.intervalTo").list();
        for (Record record : records) {
            System.out.println(record);
        }

        this.session.run("MATCH (n:Person)" +
//                "FOREACH(item in scypher.getItemsToSetExpression(n, 'name', scypher.operateTime(), false, 'John')" +
//                "FOREACH(item in scypher.getItemsToSetExpression(n, NULL, scypher.operateTime(), false, {name: 'John'})" +
                "FOREACH(item in scypher.getItemsToSetExpression(n, NULL, scypher.operateTime(), true, {name: 'John'})" +
//                "FOREACH(item in scypher.getItemsToSetExpression(n, NULL, scypher.operateTime(), true, {age: 20})" +
                "| " +
                "FOREACH( t in item.deleteItems | DELETE t) " +
                "FOREACH (t in item.createPropertyNode | CREATE (t)-[:OBJECT_PROPERTY]->(:Property{content:'age',intervalFrom:scypher.operateTime(),intervalTo:scypher.timePoint('NOW')})" +
                "-[:PROPERTY_VALUE]->(:Value{content:30,intervalFrom:scypher.operateTime(),intervalTo:scypher.timePoint('NOW')}))" +
                "FOREACH (t in item.createPropertyNodes | CREATE (n)-[:OBJECT_PROPERTY]->(:Property{content:t.propertyName,intervalFrom:scypher.operateTime(),intervalTo:scypher.timePoint('NOW')})" +
                "-[:PROPERTY_VALUE]->(:Value{content:t.propertyValue,intervalFrom:scypher.operateTime(),intervalTo:scypher.timePoint('NOW')}))" +
                "FOREACH (t in item.createValueNode | CREATE (t)-[:PROPERTY_VALUE]->(:Value{content:30,intervalFrom:scypher.operateTime(),intervalTo:scypher.timePoint('NOW')}))" +
                "FOREACH (t in item.createValueNodes | MERGE (n)-[:OBJECT_PROPERTY]->(:Property{content:t.propertyName})" +
                "-[:PROPERTY_VALUE]->(:Value{content:t.propertyValue,intervalFrom:scypher.operateTime(),intervalTo:scypher.timePoint('NOW')}))" +
                "FOREACH (t in item.staleValueNodes | SET t.intervalTo = scypher.operateTime())" +
                "FOREACH (t in item.setRelationshipProperty | SET t.type = 2 )" +
                "FOREACH (t in item.setRelationshipProperties | SET n = t)" +
                ")");
        records = this.session.run("MATCH (n:Person)-->(p:Property)-->(v:Value)" +
                "RETURN n, p.content, v.content, v.intervalFrom, v.intervalTo").list();
        for (Record record : records) {
            System.out.println(record);
        }
    }

    @Test
    public void testGetIntervalOfSubordinateNode() {
        System.out.println("testGetIntervalOfSubordinateNode");
        this.session.run("CREATE (n:Person:Object {intervalFrom:scypher.timePoint('2010'), intervalTo:scypher.timePoint('NOW')})-[:OBJECT_PROPERTY]->" +
                "(p:Property {content:'name', intervalFrom:scypher.getIntervalFromOfSubordinateNode(scypher.timePoint('2010'),scypher.timePoint('2010'))," +
                " intervalTo:scypher.getIntervalToOfSubordinateNode(scypher.timePoint('NOW'),scypher.timePoint('NOW'))})-[:PROPERTY_VALUE]->" +
                "(v1:Value {content:'Nick', intervalFrom:scypher.getIntervalFromOfSubordinateNode(scypher.timePoint('2010'),scypher.timePoint('2010')), " +
                "intervalTo:scypher.getIntervalToOfSubordinateNode(scypher.timePoint('NOW'),scypher.timePoint('2022'))})");
        List<Record> records = this.session.run("MATCH (n:Person)-->(p:Property)-->(v:Value)" +
                "RETURN n, p.intervalTo, v.intervalTo").list();
        for (Record record : records) {
            System.out.println(record);
        }
    }

    @Test
    public void testGetIntervalOfRelationship() {
        System.out.println("testGetIntervalOfRelationship");
        this.session.run("CREATE (n:Person {name:'Nick', intervalFrom:scypher.timePoint('2010'), intervalTo:scypher.timePoint('NOW')}), " +
                "(m:Person {name:'Tim',intervalFrom:scypher.timePoint('2010'), intervalTo:scypher.timePoint('NOW')})" +
                "CREATE (n)-[e:FRIEND {intervalFrom:scypher.getIntervalFromOfRelationship(scypher.timePoint('2010'), scypher.timePoint('2010'), n, m, 'FRIEND', scypher.timePoint('2010')), " +
                "intervalTo: scypher.getIntervalToOfRelationship(scypher.timePoint('NOW'), scypher.timePoint('NOW'), n, m, 'FRIEND', scypher.timePoint('NOW'))}]->(m)");
        List<Record> records = this.session.run("MATCH (n:Person)-[e:FRIEND]->(m:Person)" +
                "RETURN n.name, m.name, e.intervalFrom, e.intervalTo").list();
        for (Record record : records) {
            System.out.println(record);
        }
        this.session.run("MATCH (n:Person {name:'Nick'}), " +
                "(m:Person {name:'Tim'})" +
                "CREATE (n)-[:FRIEND {intervalFrom:scypher.getIntervalFromOfRelationship(scypher.timePoint('2010'), scypher.timePoint('2010'), n, m, 'FRIEND', scypher.timePoint('2010')), " +
                "intervalTo: scypher.getIntervalToOfRelationship(scypher.timePoint('NOW'), scypher.timePoint('NOW'), n, m, 'FRIEND', scypher.timePoint('NOW'))}]->(m)");
    }
}
