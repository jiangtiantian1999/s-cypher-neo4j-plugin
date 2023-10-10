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
        this.session.run("CREATE (n:GlobalVariable {timeGranularity: 'localdatetime'})");
        this.session.run("CALL scypher.scope({from: localdatetime('2015'), to: localdatetime('2023')})");
    }

    @AfterAll
    void closeNeo4j() {
        this.driver.close();
        this.embeddedDatabaseServer.close();
    }

    @Test
    public void testLimitInterval() {
        System.out.println("limitInterval");
        this.session.run("CREATE (n:Person {intervalFrom:localdatetime('2010'), intervalTo:localdatetime('+999999999-12-31T23:59:59.999999999')})");
        Record record = this.session.run("MATCH (n:Person) WHERE scypher.limitInterval(n,null) RETURN n").single();
        System.out.println(record);
        record = this.session.run("MATCH (n:Person) WHERE scypher.limitInterval(n,scypher.interval('2002', '2008')) RETURN count(n)").single();
        System.out.println(record);
    }

    @Test
    public void testSnapshot() {
        System.out.println("testSnapshot");
        this.session.run("CALL scypher.snapshot(localdatetime('2015'))");
        Record record = this.session.run("MATCH (n:GlobalVariable) RETURN n.snapshot").single();
        System.out.println(record);
    }

    @Test
    public void testScope() {
        System.out.println("testScope");
        this.session.run("CALL scypher.scope({from: localdatetime('2015'),to: localdatetime('2023')})");
        Record record = this.session.run("MATCH (n:GlobalVariable) RETURN n.scopeFrom, n.scopeTo").single();
        System.out.println(record);
    }
}
