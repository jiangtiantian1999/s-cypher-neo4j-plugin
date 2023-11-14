package cn.scypher.neo4j.plugin;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SDateTimeOperationTest {
    private Driver driver;
    private Neo4j embeddedDatabaseServer;
    private Session session;

    @BeforeAll
    void initializeNeo4j() {
        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .withFunction(SDateTimeOperation.class)
                .withProcedure(TimeWindowLimit.class)
                .build();
        this.driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI());
        this.session = driver.session();
        this.session.run("CREATE (n:GlobalVariable{timeGranularity: 'localdatetime'})");
    }

    @AfterAll
    void closeNeo4j() {
        this.driver.close();
        this.embeddedDatabaseServer.close();
    }

    @Test
    public void testNow() {
        System.out.println("testNow");
        Record record = this.session.run("RETURN scypher.now()").single();
        System.out.println(record);
    }

    @Test
    public void testTimePoint() {
        System.out.println("testTimePoint");
        Record record = this.session.run("RETURN scypher.timePoint('2010')").single();
        System.out.println(record);
    }

    @Test
    public void testInterval() {
        System.out.println("testInterval");
        Record record = this.session.run("RETURN scypher.interval('2010', 'NOW')").single();
        System.out.println(record);
    }

    @Test
    public void testIntervalIntersection() {
        System.out.println("testIntervalIntersection");
        Record record = this.session.run("WITH scypher.interval('2000', '2015') AS interval1, scypher.interval('2010', 'NOW') AS interval2" +
                "\nRETURN scypher.interval.intersection([interval1, interval2])").single();
        System.out.println(record);
    }

    @Test
    public void testIntervalRange() {
        System.out.println("testIntervalRange");
        Record record = this.session.run("WITH scypher.interval('2000', '2001') AS interval1, scypher.interval('2010', 'NOW') AS interval2" +
                "\nRETURN scypher.interval.range([interval1, interval2])").single();
        System.out.println(record);
    }

    @Test
    public void testIntervalDifference() {
        System.out.println("testIntervalDifference");
        Record record = this.session.run("WITH scypher.interval('2000', '2001') AS interval1, scypher.interval('2010', 'NOW') AS interval2" +
                "\nRETURN scypher.interval.difference(interval1, interval2)").single();
        System.out.println(record);
    }

    @Test
    public void testDuring() {
        System.out.println("testDuring");
        Record record = this.session.run("RETURN scypher.during(scypher.timePoint('2010'), scypher.interval('2010', 'NOW'))").single();
        System.out.println(record);
        record = this.session.run("RETURN scypher.during(scypher.timePoint('2009'), scypher.interval('2010', 'NOW'))").single();
        System.out.println(record);
    }

    @Test
    public void testOverlaps() {
        System.out.println("testOverlaps");
        Record record = this.session.run("RETURN scypher.overlaps(scypher.interval('2010', 'NOW'), scypher.interval('2005', '2009'))").single();
        System.out.println(record);
    }
}
