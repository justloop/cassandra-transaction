package cs4224.project.cassandra.models.wm;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class District {
	
	/**
	 * Get next available order id.
	 * @param session
	 * @param w_id
	 * @param d_id
	 * @return
	 */
	public static long getNextOid(Session session, int w_id, int d_id) {
		PreparedStatement statement;
		ResultSet results;
		
		// Find next available order id
		statement = session.prepare("SELECT d_next_oid FROM district WHERE w_id = ? AND d_id = ?");
		results = session.execute(statement.bind(w_id, d_id));
		
		Row district = results.one();
		if (district == null) {
			System.out.println("The required district does not exsit!");
			return -1;
		}
		
		return district.getLong("d_next_oid");
	}

}
