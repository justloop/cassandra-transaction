package cs4224.project.cassandra.transactions;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;

public class OrderStatus {
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private static PreparedStatement selectCustomer;
	private static PreparedStatement selectOrder;
	
	/**
	 * Execute an order status transaction.
	 * @param session
	 * @param w_id
	 * @param d_id
	 * @param c_id
	 * @return
	 */
	public static boolean execute(Session session, int w_id, int d_id, int c_id) {
		ResultSet results;
		
		// Select the required customer
		if (selectCustomer == null) {
			selectCustomer = session.prepare(
				"SELECT c_first, c_middle, c_last, c_balance, o_id "
					+ "FROM customer WHERE w_id = ? AND d_id = ? AND c_id = ?"
			);
		}
		results = session.execute(selectCustomer.bind(w_id, d_id, c_id));
		Row customer = results.one();
		
		System.out.println("C_FIRST: " + customer.getString("c_first"));
		System.out.println("C_MIDDLE: " + customer.getString("c_middle"));
		System.out.println("C_LAST: " + customer.getString("c_last"));
		System.out.println("C_BALANCE: " + customer.getDouble("c_balance"));
		
		// Find customer's last order
		int o_id = customer.getInt("o_id");
		
		if (selectOrder == null) {
			selectOrder = session.prepare(
				"SELECT o_entry_d, o_carrier_id, ol_delivery_d, ols "
					+ "FROM order2 WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? "
					+ "LIMIT 1"
			);
		}
		results = session.execute(selectOrder.bind(w_id, d_id, o_id));
		Row order = results.one();
		
		System.out.println("O_ID: " + o_id);
		System.out.println("O_ENTRY_D: " + df.format(order.getTimestamp("o_entry_d")));
		System.out.println("O_CARRIER_ID: " + order.getInt("o_carrier_id"));
		
		Date ol_delivery_d = order.getTimestamp("ol_delivery_d");
		
		// List each item
		Set<UDTValue> orderlines = order.getSet("ols", UDTValue.class);
		for (UDTValue item : orderlines) {
			System.out.println("OL_I_ID: " + item.getInt("ol_i_id"));
			System.out.println("OL_SUPPLY_W_ID: " + item.getInt("ol_supply_w_id"));
			System.out.println("OL_QUANTITY: " + item.getInt("ol_quantity"));
			System.out.println("OL_AMOUNT: " + item.getDouble("ol_amount"));
			
			if (ol_delivery_d != null) {
				System.out.println("OL_DELIVERY_D: " + df.format(ol_delivery_d));
			} else {
				System.out.println("OL_DELIVERY_D: NA");
			}
		}
		
		return true;
	}

}
