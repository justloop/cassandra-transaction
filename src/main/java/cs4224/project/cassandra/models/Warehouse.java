package cs4224.project.cassandra.models;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class Warehouse {

	private Session session;
	public Warehouse(Session session) {
		this.session = session;
	}

	public void Insert() {
		ResultSet results;
		// Insert one record into the users table
		PreparedStatement statement = session.prepare(
				"INSERT INTO users" + "(lastname, age, city, email, firstname)" + "VALUES (?,?,?,?,?);");
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind("Jones", 35, "Austin", "bob@example.com", "Bob"));
		// Use select to get the user we just entered
		Statement select = QueryBuilder.select().all().from("mydata", "users").where(eq("lastname", "Jones"));
		results = session.execute(select);
		for (Row row : results) {
			System.out.format("%s %d \n", row.getString("firstname"), row.getInt("age"));
		}
	}

	public void Update() {
		ResultSet results;
		// Update the same user with a new age
		session.execute("update users set age = 36 where lastname = 'Jones'");
		// Select and show the change
		results = session.execute("select * from users where lastname='Jones'");
		for (Row row : results) {
			System.out.format("%s %d\n", row.getString("firstname"), row.getInt("age"));
		}
	}

	public void delete() {
		ResultSet results;
		// Delete the user from the users table
		session.execute("DELETE FROM users WHERE lastname = 'Jones'");
		// Show that the user is gone
		results = session.execute("SELECT * FROM users");
		for (Row row : results) {
			System.out.format("%s %d %s %s %s\n", row.getString("lastname"), row.getInt("age"), row.getString("city"),
					row.getString("email"), row.getString("firstname"));
		}
	}

}
