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
public class TimeWindowLimitTest {
    private Driver driver;
    private Neo4j embeddedDatabaseServer;
    private Session session;

    @BeforeAll
    void initializeNeo4j() {
        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .withFunction(TimeWindowLimit.class)
                .withProcedure(TimeWindowLimit.class)
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
    public void testLimitInterval() {
        System.out.println("limitEffectiveTime");
        this.session.run("CREATE (n:Person {intervalFrom:scypher.timePoint('2010'), intervalTo:scypher.timePoint('NOW')})");
        Record record = this.session.run("MATCH (n:Person) WHERE scypher.limitEffectiveTime([[n,localdatetime('2011')]],null) RETURN n").single();
        System.out.println(record);
        record = this.session.run("MATCH (n:Person) WHERE scypher.limitEffectiveTime([[n,null]],scypher.interval('2002', '2008')) RETURN count(n)").single();
        System.out.println(record);
    }

    @Test
    public void testSnapshot() {
        System.out.println("testSnapshot");
        this.session.run("CALL scypher.snapshot(localdatetime('2015'))");
        Record record = this.session.run("RETURN scypher.operateTime()").single();
        System.out.println(record);
    }

    @Test
    public void testScope() {
        System.out.println("testScope");
        this.session.run("CALL scypher.scope({from: localdatetime('2015'),to: localdatetime('2023')})");
        Record record = this.session.run("RETURN scypher.operateTime()").single();
        System.out.println(record);
    }
}
