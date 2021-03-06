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
}
