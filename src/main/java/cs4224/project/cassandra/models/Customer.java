package cs4224.project.cassandra.models;

import com.datastax.driver.core.Session;

public class Customer {
	private Session session;
	private String tablename = "customer";
	
	public Customer(Session session) {
		this.session = session;
	}
	public void Insert(int w_id, int d_id, int c_id, String c_first, String c_middle, String c_last) {
		String query = String.format("INSERT INTO " + tablename + 
				" (w_id, d_id, c_id, c_first, c_middle, c_last) VALUES (%d, %d, %d, '%s', '%s', '%s');"
				, w_id, d_id, c_id, c_first, c_middle, c_last);
		System.out.println(query);
		session.execute(query);
	}
}
