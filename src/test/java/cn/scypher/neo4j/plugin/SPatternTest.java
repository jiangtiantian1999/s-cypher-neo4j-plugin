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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SPatternTest {
    private Driver driver;
    private Neo4j embeddedDatabaseServer;

    @BeforeAll
    void initializeNeo4j() {
        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .withFunction(SPattern.class)
                .build();
        this.driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI());
    }

    @AfterAll
    void closeNeo4j() {
        this.driver.close();
        this.embeddedDatabaseServer.close();
    }

    @Test
    public void testGetPropertyNode() {
        System.out.println("start");
        try (Session session = driver.session()) {
            session.run("CREATE (:Object:Person)-[:OBJECT_PROPERTY]->(:Property{content:'name'})-[:PROPERTY_VALUE]->(:Value{content:'Nick'})");
            Record record = session.run("MATCH (p:Person) RETURN scypher.getPropertyNode(p,'name')").single();
            System.out.println(record);
        }
    }
}
