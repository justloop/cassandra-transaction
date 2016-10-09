package cs4224.project.cassandra.transactions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;

import cs4224.project.cassandra.models.wm.District;

public class StockLevel {

	/**
	 * Execute a stock level transaction.
	 * @param session
	 * @param w_id
	 * @param d_id
	 * @param t	Stock threshold
	 * @param l	Last L items to be examined
	 * @return
	 */
	public static boolean execute(Session session, int w_id, int d_id, int t, int l) {
		PreparedStatement statement;
		ResultSet results;
		
		// Find next available order id
		long nextOid = District.getNextOid(session, w_id, d_id);
		if (nextOid < 0) {
			System.out.println("The required district does not exsit!");
			return false;
		}
		
		// Find last L orders
		statement = session.prepare(
			"SELECT ols FROM order2 WHERE o_w_id = ? AND o_d_id >= ? AND o_d_id < ?"
		);
		
		results = session.execute(statement.bind(w_id, d_id, nextOid - l, nextOid));
		
		// Collect all appeared items
		Set<Integer> items = new HashSet<>();
		for (Row order : results) {
			Set<UDTValue> orderlines = order.getSet("ols", UDTValue.class);
			for (UDTValue item : orderlines) {
				items.add(item.getInt("ol_i_id"));
			}
		}
		
		// Filter by stock level
		statement = session.prepare(
			"SELECT i_id, s_quantity FROM item WHERE i_w_id = ? AND i_id IN ?"
		);
		
		results = session.execute(statement.bind(w_id, new ArrayList<Integer>(items)));
		for (Row row : results) {
			if (row.getInt("s_quantity") >= t) {
				items.remove(row.getInt("i_id"));
			}
		}
		
		System.out.println("Total number of items where stock is lower than threshould: " + items.size());
		
		return true;
	}
}
