package cs4224.project.cassandra.transactions;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import cs4224.project.cassandra.models.wm.District;

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
		String keyspace = session.getLoggedKeyspace();
		Statement statement;
		ResultSet results;
		
		System.out.println("W_ID: " + w_id);
		System.out.println("D_ID: " + d_id);
		System.out.println("Number of last orders to be examined: " + l);
		
		long nextOid = District.getNextOid(session, w_id, d_id);
		if (nextOid < 0) {
			System.out.println("The required district does not exsit!");
			return false;
		}
		
		// Find last L orders
		statement = QueryBuilder.select()
				.all()
				.from(keyspace, "order2")
				.where(eq("o_w_id", w_id))
				.and(eq("o_d_id", d_id))
				.and(gte("o_id", nextOid - l))
				;
		
		results = session.execute(statement);
		
		// Distinct popular items
		Set<String> popItems = new HashSet<>();
		
		// Check each order
		int orderSize = 0;
		for (Row order : results) {
			orderSize++;
			System.out.println("O_ID: " + order.getInt("o_id"));
			System.out.println("O_ENTRY_D: " + df.format(order.getTimestamp("o_entry_d")));
			
			System.out.println("C_FIRST: " + order.getString("c_first"));
			System.out.println("C_MIDDLE: " + order.getString("c_middle"));
			System.out.println("C_LAST: " + order.getString("c_last"));
			
			// Find most popular items
			int max = 0;
			Set<String> items = new HashSet<>();
			
			Set<UDTValue> orderlines = order.getSet("ols", UDTValue.class);
			for (UDTValue line : orderlines) {
				String ol_i_name = line.getString("ol_i_name");
				int quantity = line.getInt("ol_quantity");
				if (quantity > max) {
					max = quantity;
					items.clear();
					items.add(ol_i_name);
				} else if (quantity == max) {
					items.add(ol_i_name);
				}
			};
			
			for (String item : items) {
				System.out.println("I_NAME: " + item);
				System.out.println("OL_QUANTITY: " + max);
			}
			
			popItems.addAll(items);
		}
		
		// The percentage of examined orders that contains each popular item
		Map<String, Integer> counterMap = new HashMap<>();
		popItems.forEach(item -> counterMap.put(item, 0));
		
		for (Row order : results) {
			Set<UDTValue> orderlines = order.getSet("ols", UDTValue.class);
			for (UDTValue line : orderlines) {
				String ol_i_name = line.getString("ol_i_name");
				if (counterMap.containsKey(ol_i_name)) {
					counterMap.put(ol_i_name, counterMap.get(ol_i_name) + 1);
				}
			}
		}
		
		for (Map.Entry<String, Integer> entry : counterMap.entrySet()) {
			System.out.println("I_NAME: " + entry.getKey());
			System.out.println("Percentage: " + entry.getValue() / (float)orderSize);
		}
		
		return true;
	}

}
