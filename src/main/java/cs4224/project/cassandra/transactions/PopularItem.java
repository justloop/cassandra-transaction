package cs4224.project.cassandra.transactions;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;

public class PopularItem {
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * Execute a popular item transaction.
	 * @param session
	 * @param w_id
	 * @param d_id
	 * @param l	Number of last orders to be examined.
	 * @return
	 */
	public static boolean execute(Session session, int w_id, int d_id, int l) {
		PreparedStatement statement;
		ResultSet results;
		
		System.out.println("W_ID: " + w_id);
		System.out.println("D_ID: " + d_id);
		System.out.println("Number of last orders to be examined: " + l);
		
		// Find next available order id
		statement = session.prepare("SELECT d_next_oid FROM district WHERE w_id = ? AND d_id = ?");
		results = session.execute(statement.bind(w_id, d_id));
		Row district = results.one();
		
		int d_next_oid = district.getInt("d_next_oid");
		
		// Find last L orders
		statement = session.prepare(
			"SELECT * FROM order2 WHERE o_w_id = ? AND o_d_id = ? AND o_id >= ? AND o_id < ?"
		);
		results = session.execute(statement.bind(w_id, d_id, d_next_oid - l, d_next_oid));
		
		// Distinct popular items
		Set<String> popItems = new HashSet<>();
		// All items ordered in each order
		List<Set<String>> orderItems = new ArrayList<>();
		
		for (Row order : results) {
			System.out.println("O_ID: " + order.getInt("o_id"));
			System.out.println("O_ENTRY_D: " + df.format(order.getTimestamp("o_entry_d")));
			
			System.out.println("C_FIRST: " + order.getString("c_first"));
			System.out.println("C_MIDDLE: " + order.getString("c_middle"));
			System.out.println("C_LAST: " + order.getString("c_last"));
			
			// Find most popular items in this order
			int max = 0;
			Set<String> items = new HashSet<>();
			// All ordered items
			Set<String> allItems = new HashSet<>();
			
			Set<UDTValue> orderlines = order.getSet("ols", UDTValue.class);
			for (UDTValue line : orderlines) {
				String ol_i_name = line.getString("ol_i_name");
				allItems.add(ol_i_name);
				
				int quantity = line.getInt("ol_quantity");
				if (quantity > max) {
					max = quantity;
					items.clear();
					items.add(ol_i_name);
				} else if (quantity == max) {
					items.add(ol_i_name);
				}
			}
			
			for (String item : items) {
				System.out.println("I_NAME: " + item);
				System.out.println("OL_QUANTITY: " + max);
			}
			
			popItems.addAll(items);
			orderItems.add(allItems);
		}
		
		// The percentage of examined orders that contains each popular item
		Map<String, Integer> counterMap = new HashMap<>();
		for (String pop : popItems) {
			for (Set<String> items : orderItems) {
				if (items.contains(pop)) {
					counterMap.put(pop, counterMap.containsKey(pop) 
							? counterMap.get(pop) + 1 : 1);
				}
			}
		}
		
		for (Map.Entry<String, Integer> entry : counterMap.entrySet()) {
			System.out.println("I_NAME: " + entry.getKey());
			float percentage = entry.getValue() * 100.0f / orderItems.size();
			System.out.println("Percentage: " + percentage + "%");
		}
		
		return true;
	}

}
