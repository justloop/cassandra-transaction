package cs4224.project.cassandra.models;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class Order {
	private Session session;
	private String tablename = "order2";
	public Order(Session session) {
		this.session = session;
	}

	public void Insert() {
		ResultSet results;
		PreparedStatement statement = session.prepare(
				"INSERT INTO " + tablename + "(lastname, age, city, email, firstname)" + "VALUES (?,?,?,?,?);");
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind("Jones", 35, "Austin", "bob@example.com", "Bob"));
		// Use select to get the user we just entered
		Statement select = QueryBuilder.select().all().from("mydata", "users").where(eq("lastname", "Jones"));
		results = session.execute(select);
		for (Row row : results) {
			System.out.format("%s %d \n", row.getString("firstname"), row.getInt("age"));
		}
	}
}
