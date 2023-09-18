package cn.scypher.neo4j.plugin;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.time.LocalDate;
import java.time.OffsetTime;
import java.time.ZonedDateTime;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TimeWindowLimitTest {
    private Driver driver;
    private Neo4j embeddedDatabaseServer;

    @BeforeAll
    void initializeNeo4j() {
        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .withProcedure(TimeWindowLimit.class)
                .build();
        this.driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI());
    }

    @AfterAll
    void closeNeo4j() {
        this.driver.close();
        this.embeddedDatabaseServer.close();
    }

    @Test
    public void testSnapshot() {
        System.out.println("testSnapshot");
        try (Session session = driver.session()) {
            session.run("CALL scypher.snapshot(date('2015'))");
            Record record = session.run("MATCH (n:GlobalVariable) RETURN n.snapshot").single();
            System.out.println(record);
        }
    }

    @Test
    public void testScope() {
        System.out.println("testScope");
        try (Session session = driver.session()) {
            session.run("CALL scypher.scope({from:date('2015'),to:date('2023')})");
            Record record = session.run("MATCH (n:GlobalVariable) RETURN n.scope_from,n.scope_to").single();
            System.out.println(record);
        }
    }
}
