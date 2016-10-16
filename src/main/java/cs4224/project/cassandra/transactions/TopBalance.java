package cs4224.project.cassandra.transactions;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class TopBalance {
	
	private static PreparedStatement selectTop10;
	
	/**
	 * Execute a top balance transaction.
	 * @param session
	 * @return
	 */
	public static boolean execute(Session session) {
		ResultSet results;
		// Top balance customers
		List<Row> tops = new ArrayList<>();
		
		if (selectTop10 == null) {
			selectTop10 = session.prepare(
				"SELECT * FROM top10 WHERE d_id = ? LIMIT 10"
			);
		}
		
		// Select top 10 at each district for all warehouses
		for (int i = 1; i <= 10; i++) {
			results = session.execute(selectTop10.bind(i));
			tops.addAll(results.all());
		}
		
		// Sort
		tops.sort((o1, o2) -> Double.compare(o2.getDouble("c_balance"), o1.getDouble("c_balance")));
		for (int i = 0; i < 10 && i < tops.size(); i++) {
			Row customer = tops.get(i);
			System.out.println("C_ID: " + customer.getInt("c_id"));
			System.out.println("C_BALANCE: " + customer.getDouble("c_balance"));
		}
		
		return true;
	}

}
