package cs4224.project.cassandra.models;

import com.datastax.driver.core.Session;

public class District {
	private Session session;
	private String tablename = "district";
	
	public District(Session session) {
		this.session = session;
	}
	public void Insert(int w_id, int d_id, int d_next_oid, double d_ytd) {
		session.execute(String.format("INSERT INTO " + tablename + " (w_id, d_id, d_next_oid, d_ytd) VALUES (%d, %d, %d, %f);", w_id, d_id, d_next_oid, d_ytd));
	}
}
