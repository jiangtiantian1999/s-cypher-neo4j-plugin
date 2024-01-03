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
public class TemporalPathQueryTest {

    private Driver driver;
    private Neo4j embeddedDatabaseServer;
    private Session session;

    @BeforeAll
    void initializeNeo4j() {
        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .withFunction(SDateTimeOperation.class)
                .withProcedure(TemporalPathQuery.class)
                .build();
        this.driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI());
        this.session = driver.session();
        this.session.run("CREATE (s0:Station{name:\"Zhenzhoudong Railway Station\"})" +
                "CREATE (s1:Station{name:\"Lianyungang Railway Station\"})" +
                "CREATE (s2:Station{name:\"Xi'anbei Railway Station\"})" +
                "CREATE (s3:Station{name:\"Xuzhoudong Railway Station\"})" +
                "CREATE (s4:Station{name:\"Nanjingnan Railway Station\"})" +
                "CREATE (s5:Station{name:\"Hefeinan Railway Station\"})" +
                "CREATE (s6:Station{name:\"Shanghai Hongqiao Railway Station\"})" +
                "CREATE (s7:Station{name:\"Wuhan Railway Station\"})" +
                "CREATE (s8:Station{name:\"Hangzhoudong Railway Station\"})" +
                "CREATE (s3)-[r0:G1323{intervalFrom:scypher.timePoint(\"2023-02-06T12:44\"), intervalTo:scypher.timePoint(\"2023-02-06T14:32\")}]->(s0)" +
                "CREATE (s3)-[r1:G289{intervalFrom:scypher.timePoint(\"2023-02-06T13:46\"), intervalTo:scypher.timePoint(\"2023-02-06T14:54\")}]->(s1)" +
                "CREATE (s6)-[r2:G7540{intervalFrom:scypher.timePoint(\"2023-02-06T13:03\"), intervalTo:scypher.timePoint(\"2023-02-06T16:40\")}]->(s1)" +
                "CREATE (s6)-[r3:G116{intervalFrom:scypher.timePoint(\"2023-02-06T09:26\"), intervalTo:scypher.timePoint(\"2023-02-06T12:28\")}]->(s3)" +
                "CREATE (s4)-[r4:G178{intervalFrom:scypher.timePoint(\"2023-02-06T11:12\"), intervalTo:scypher.timePoint(\"2023-02-06T12:32\")}]->(s3)" +
                "CREATE (s5)-[r5:G3190{intervalFrom:scypher.timePoint(\"2023-02-06T12:09\"), intervalTo:scypher.timePoint(\"2023-02-06T17:39\")}]->(s2)" +
                "CREATE (s6)-[r6:G1204{intervalFrom:scypher.timePoint(\"2023-02-06T09:33\"), intervalTo:scypher.timePoint(\"2023-02-06T11:04\")}]->(s4)" +
                "CREATE (s8)-[r7:G7602{intervalFrom:scypher.timePoint(\"2023-02-06T09:54\"), intervalTo:scypher.timePoint(\"2023-02-06T11:58\")}]->(s5)" +
                "CREATE (s8)-[r8:G7349{intervalFrom:scypher.timePoint(\"2023-02-06T08:15\"), intervalTo:scypher.timePoint(\"2023-02-06T09:29\")}]->(s6)" +
                "CREATE (s8)-[r9:G3190{intervalFrom:scypher.timePoint(\"2023-02-06T09:42\"), intervalTo:scypher.timePoint(\"2023-02-06T17:39\")}]->(s2)" +
                "CREATE (s7)-[r10:G822{intervalFrom:scypher.timePoint(\"2023-02-06T14:25\"), intervalTo:scypher.timePoint(\"2023-02-06T19:22\")}]->(s2)" +
                "CREATE (s8)-[r11:G590{intervalFrom:scypher.timePoint(\"2023-02-06T09:13\"), intervalTo:scypher.timePoint(\"2023-02-06T14:04\")}]->(s7)" +
                "CREATE (s8)-[r12:G7372{intervalFrom:scypher.timePoint(\"2023-02-06T08:07\"), intervalTo:scypher.timePoint(\"2023-02-06T09:23\")}]->(s6)");
        this.session.run("CREATE (p1:Person{name:\"Pauline Boutler\"})" +
                "CREATE (p2:Person{name:\"Cathy Van\"})" +
                "CREATE (p3:Person{name:\"Sandra Carter\"})" +
                "CREATE (p4:Person{name:\"Peter Burton\"})" +
                "CREATE (p5:Person{name:\"Daniel Yang\"})" +
                "CREATE (p6:Person{name:\"Mary Smith\"})" +
                "CREATE (c1:City{name:\"London\"})" +
                "CREATE (c2:City{name:\"Brussels\"})" +
                "CREATE (c3:City{name:\"Paris\"})" +
                "CREATE (c4:City{name:\"Antwerp\"})" +
                "CREATE (c5:City{name:\"New York\"})" +
                "CREATE (b1:Brand{name:\"Samsung\"})" +
                "CREATE (b2:Brand{name:\"Lucky Goldstar\"})" +
                "CREATE (p1)-[:FRIEND{intervalFrom: scypher.timePoint(\"2002\"), intervalTo: scypher.timePoint(\"2017\")}]->(p2)" +
                "CREATE (p4)-[:FRIEND{intervalFrom: scypher.timePoint(\"1993\"), intervalTo: scypher.timePoint('2016')}]->(p5) " +
                "CREATE (p5)-[:FRIEND{intervalFrom: scypher.timePoint(\"2015\"), intervalTo: scypher.timePoint(\"2018\")}]->(p3)" +
                "CREATE (p6)-[:FRIEND{intervalFrom: scypher.timePoint(\"1993\"), intervalTo: scypher.timePoint('NOW')}]->(p4), " +
                "(p6)-[:FRIEND{intervalFrom: scypher.timePoint(\"2010\"), intervalTo: scypher.timePoint(\"2018\")}]->(p1)");
    }

    @AfterAll
    void closeNeo4j() {
        this.driver.close();
        this.embeddedDatabaseServer.close();
    }

    @Test
    public void testCPath() {
        System.out.println("testCPath");
        List<Record> records = this.session.run("MATCH (n1:Person),(n2:Person) " +
                "CALL scypher.cPath(n1,n2,False,{labels:['FRIEND'], minLength:2, maxLength:2}) " +
                "YIELD path " +
                "RETURN path").list();
        for (Record record : records) {
            System.out.println(record);
        }
        records = this.session.run("MATCH (n1:Station),(n2:Station) " +
                "CALL scypher.cPath(n1,n2,False,{minLength:2, maxLength:2}) " +
                "YIELD path " +
                "RETURN path").list();
        for (Record record : records) {
            System.out.println(record);
        }
    }

    @Test
    public void testPairCPath() {
        System.out.println("testPairCPath");
        List<Record> records = this.session.run("MATCH (n1:Person),(n2:Person) " +
                "CALL scypher.pairCPath(n1,n2,False,{labels:['FRIEND'], minLength:2, maxLength:3}) " +
                "YIELD path " +
                "RETURN path").list();
        for (Record record : records) {
            System.out.println(record);
        }
    }

    @Test
    public void testEarliestSPath() {
        System.out.println("testEarliestSPath");
        List<Record> records = this.session.run("MATCH (n1:Station{name:'Hangzhoudong Railway Station'}), (n2:Station{name:'Xuzhoudong Railway Station'}) " +
                "CALL scypher.earliestSPath(n1,n2,False,NULL) " +
                "YIELD path " +
                "RETURN path").list();
        for (Record record : records) {
            System.out.println(record);
        }
    }

    @Test
    public void testLatestSPath() {
        System.out.println("testLatestSPath");
        List<Record> records = this.session.run("MATCH (n1:Station{name:'Hangzhoudong Railway Station'}), (n2:Station{name:'Xuzhoudong Railway Station'}) " +
                "CALL scypher.latestSPath(n1,n2,False,NULL) " +
                "YIELD path " +
                "RETURN path").list();
        for (Record record : records) {
            System.out.println(record);
        }
    }

    @Test
    public void testFastestSPath() {
        System.out.println("testFastestSPath");
        List<Record> records = this.session.run("MATCH (n1:Station{name:'Hangzhoudong Railway Station'}), (n2:Station{name:\"Xi'anbei Railway Station\"}) " +
                "CALL scypher.fastestSPath(n1,n2,False,NULL) " +
                "YIELD path " +
                "RETURN path").list();
        for (Record record : records) {
            System.out.println(record);
        }
    }

    @Test
    public void testShortestSPath() {
        System.out.println("testShortestSPath");
        List<Record> records = this.session.run("MATCH (n1:Station{name:'Hangzhoudong Railway Station'}), (n2:Station{name:\"Xi'anbei Railway Station\"}) " +
                "CALL scypher.shortestSPath(n1,n2,False,NULL) " +
                "YIELD path " +
                "RETURN path").list();
        for (Record record : records) {
            System.out.println(record);
        }
    }
}
