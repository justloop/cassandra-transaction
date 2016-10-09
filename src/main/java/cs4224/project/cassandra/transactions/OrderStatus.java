package cs4224.project.cassandra.transactions;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class OrderStatus {
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * Execute an order status transaction.
	 * @param session
	 * @param c_w_id
	 * @param c_d_id
	 * @param c_id
	 * @return
	 */
	public static boolean execute(Session session, int c_w_id, int c_d_id, int c_id) {
		String keyspace = session.getLoggedKeyspace();
		Statement statement;
		ResultSet results;
		
		// Select the required customer
		statement = QueryBuilder.select()
							.column("c_first").column("c_middle")
							.column("c_last").column("c_balance")
							.from(keyspace, "customer")
							.where(eq("w_id", c_w_id))
							.and(eq("d_id", c_d_id))
							.and(eq("c_id", c_id))
							;
		
		results = session.execute(statement);
		Row customer = results.one();
		if (customer == null) {
			System.out.println("The required customer does not exsit!");
			return false;
		}
		
		System.out.println("C_FIRST: " + customer.getString("c_first"));
		System.out.println("C_MIDDLE: " + customer.getString("c_middle"));
		System.out.println("C_LAST: " + customer.getString("c_last"));
		System.out.println("C_BALANCE: " + customer.getDouble("c_balance"));
		
		
		// Find customer's last order
		statement = QueryBuilder.select()
				.column("o_id").column("o_entry_id")
				.column("o_carrier_id").column("ol_delivery_d")
				.column("ols")
				.from(keyspace, "order2")
				.where(eq("o_w_id", c_w_id))
				.and(eq("o_d_id", c_d_id))
				.and(eq("o_c_id", c_id))
				.limit(1)
				;
		
		results = session.execute(statement);
		Row order = results.one();
		if (order == null) {
			System.out.println("The customer has not place any order yet!");
			return false;
		}
		
		System.out.println("O_ID: " + order.getInt("o_id"));
		System.out.println("O_ENTRY_D: " + df.format(order.getTimestamp("o_entry_id")));
		System.out.println("O_CARRIER_ID: " + order.getInt("o_carrier_id"));
		
		// List each item
		Set<UDTValue> orderlines = order.getSet("ols", UDTValue.class);
		for (UDTValue item : orderlines) {
			System.out.println("OL_I_ID: " + item.getInt("ol_i_id"));
			System.out.println("OL_SUPPLY_W_ID: " + item.getInt("ol_supply_w_id"));
			System.out.println("OL_QUANTITY: " + item.getDouble("ol_quantity"));
			System.out.println("OL_AMOUNT: " + item.getDouble("ol_amount"));
			System.out.println("OL_DELIVERY_D: " + df.format(order.getTimestamp("ol_delivery_d")));
		}
		
		return true;
	}

}
