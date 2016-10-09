package cs4224.project.cassandra.transactions;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;

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
		String keyspace = session.getLoggedKeyspace();
		Statement statement;
		ResultSet results;
		
		// Find next available order id
		statement = QueryBuilder.select()
							.column("d_next_oid")
							.from(keyspace, "nextoid")
							.where(eq("w_id", w_id))
							.and(eq("d_id", d_id))
							;
		
		results = session.execute(statement);
		Row district = results.one();
		if (district == null) {
			System.out.println("The required district does not exsit!");
			return false;
		}
		
		long nextOid = district.getLong("d_next_oid");
		
		// Find last L orders
		statement = QueryBuilder.select()
				.column("ols")
				.from(keyspace, "order2")
				.where(eq("o_w_id", w_id))
				.and(eq("o_d_id", d_id))
				.and(gte("o_id", nextOid - l))
				;
		
		results = session.execute(statement);
		
		// Collect all appeared items
		Set<Integer> items = new HashSet<>();
		for (Row order : results) {
			Set<UDTValue> orderlines = order.getSet("ols", UDTValue.class);
			for (UDTValue item : orderlines) {
				items.add(item.getInt("ol_i_id"));
			}
		}
		
		// TODO CHECK STOCK LEVEL
		statement = QueryBuilder.select()
				.column("i_id").column("s_quantity")
				.from(keyspace, "item")
				.where(eq("i_w_id", w_id))
				.and(in("i_id", new ArrayList<Integer>(items)))
				;
		
		results = session.execute(statement);
		for (Row row : results) {
			if (row.getDouble("s_quantity") >= t) {
				items.remove(row.getInt("i_id"));
			}
		}
		
		System.out.println("Total number of items where stock is lower than threshould: " + items.size());
		
		return true;
	}
}
