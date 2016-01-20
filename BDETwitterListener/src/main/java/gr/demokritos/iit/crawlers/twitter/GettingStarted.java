/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

/**
 * simple examples of cassandra-java driver
 *
 * @author George K. <gkiom@scify.org>
 */
public class GettingStarted {

    public static void main(String[] args) {

        Cluster cluster;
        Session session;
        ResultSet results;
        Row rows;

        // Connect to the cluster and keyspace "demo"
        cluster = Cluster
                .builder()
                .addContactPoint("127.0.0.1")
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(
                        new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                .build();
        session = cluster.connect("demo");

        // Insert one record into the users table
        PreparedStatement statement = session.prepare(
                "INSERT INTO users" + "(lastname, age, city, email, firstname)"
                + "VALUES (?,?,?,?,?);");

        BoundStatement boundStatement = new BoundStatement(statement);

        session.execute(boundStatement.bind("Jones", 35, "Austin",
                "bob@example.com", "Bob"));

        // Use select to get the user we just entered
        Statement select = QueryBuilder.select().all().from("demo", "users")
                .where(eq("lastname", "Jones"));
        results = session.execute(select);
        for (Row row : results) {
            System.out.format("%s %d \n", row.getString("firstname"),
                    row.getInt("age"));
        }
// Update the same user with a new age
        Statement update = QueryBuilder.update("demo", "users")
                .with(QueryBuilder.set("age", 36))
                .where((QueryBuilder.eq("lastname", "Jones")));
        session.execute(update);
// Select and show the change
        select = QueryBuilder.select().all().from("demo", "users")
                .where(eq("lastname", "Jones"));
        results = session.execute(select);
        for (Row row : results) {
            System.out.format("%s %d \n", row.getString("firstname"),
                    row.getInt("age"));
        }
        // Delete the user from the users table
        Statement delete = QueryBuilder.delete().from("users")
                .where(QueryBuilder.eq("lastname", "Jones"));
        results = session.execute(delete);
        // Show that the user is gone
        select = QueryBuilder.select().all().from("demo", "users");
        results = session.execute(select);
        for (Row row : results) {
            System.out.format("%s %d %s %s %s\n", row.getString("lastname"),
                    row.getInt("age"), row.getString("city"),
                    row.getString("email"), row.getString("firstname"));
        }
        // Clean up the connection by closing it
        cluster.close();

    }

    private static void simple() {
        Cluster cluster;
        Session session;

        // Connect to the cluster and keyspace "demo"
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("demo");

        session.execute("INSERT INTO users (lastname, age, city, email, firstname) VALUES ('Kiom', 36, 'Athens', 'gkiom@scify.org', 'George')");
        // Insert one record into the users table
        session.execute("INSERT INTO users (lastname, age, city, email, firstname) VALUES ('Jones', 35, 'Austin', 'bob@example.com', 'Bob')");

        // Use select to get the user we just entered
        ResultSet results = session.execute("SELECT * FROM users WHERE lastname='Jones'");
        for (Row row : results) {
            System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));
        }

        // Update the same user with a new age
        session.execute("update users set age = 36 where lastname = 'Jones'");
        // Select and show the change
        results = session.execute("select * from users where lastname='Jones'");
        for (Row row : results) {
            System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));

        }

        // Delete the user from the users table
        session.execute("DELETE FROM users WHERE lastname = 'Jones'");
        // Show that the user is gone
        results = session.execute("SELECT * FROM users");
        for (Row row : results) {
            System.out.format("%s %d %s %s %s\n", row.getString("lastname"), row.getInt("age"), row.getString("city"), row.getString("email"), row.getString("firstname"));
        }
        // Clean up the connection by closing it
        cluster.close();
    }
}
