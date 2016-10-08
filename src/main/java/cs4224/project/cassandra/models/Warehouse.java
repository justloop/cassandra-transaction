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

	public void Insert(int w_id, double w_ytd) {
		
		session.execute(String.format("INSERT INTO warehouse (w_id, w_ytd) VALUES (%d, %f);", w_id, w_ytd));
	}

	public void Update(int w_id, double w_ytd){
		session.execute(String.format("UPDATE warehouse set w_ytd = %f where w_id = %d;", w_ytd, w_id));
	}
	
	public void Delete(int w_id){
		session.execute(String.format("DELETE FROM warehouse where w_id = %d;", w_id));
	}
	
	public void PrintAll(){
		System.out.println("Printing tables...");
		ResultSet results = session.execute("SELECT * FROM warehouse");
		for (Row row : results) {
			System.out.format("%d %f\n", row.getInt("w_id"), row.getDouble("w_ytd"));
		}
	}
	
	/*
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
	*/

}
