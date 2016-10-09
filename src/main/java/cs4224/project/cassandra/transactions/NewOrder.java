package cs4224.project.cassandra.transactions;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

public class NewOrder {
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * Execute a new order request.
	 * @param session
	 * @param w_id
	 * @param d_id
	 * @param c_id
	 * @param numItems
	 * @param itemNum
	 * @param supplier
	 * @param quantity
	 * @return
	 */
	public static boolean execute(Session session, int w_id, int d_id, int c_id, 
			int numItems, int[] itemNum, int[] supplier, int[] quantity) {
		PreparedStatement statement;
		ResultSet results;
		
		// Reserver next order id
		int o_id = reserveNextOid(session, w_id, d_id);
		
		// Customer
		statement = session.prepare("SELECT * FROM customer WHERE w_id = ? AND d_id = ? AND c_id = ?");
		results = session.execute(statement.bind(w_id, d_id, c_id));
		Row consumer = results.one();
		
		String firstName = consumer.getString("c_first");
		String middleName = consumer.getString("c_middle");
		String lastName = consumer.getString("c_last");
		String c_credit = consumer.getString("c_crdit");
		double c_discount = consumer.getDouble("c_discount");
		double w_tax = consumer.getDouble("w_tax");
		double d_tax = consumer.getDouble("d_tax");
		
		System.out.println("W_ID: " + w_id);
		System.out.println("D_ID: " + d_id);
		System.out.println("C_ID: " + c_id);
		System.out.println("C_LAST: " + lastName);
		System.out.println("C_CREDIT: " + c_credit);
		System.out.println("C_DISCOUNT: " + c_discount);
		
		System.out.println("W_TAX: " + w_tax);
		System.out.println("D_TAX: " + d_tax);
		
		// All local ?
		int o_all_local = 0;
		// Order lines
		Set<UDTValue> ols = new HashSet<>();
		// Total amount
		double totalAmount = 0.0;
		
		// Process each item
		for (int i = 0; i < numItems; i++) {
			if (supplier[i] != w_id) o_all_local = 1;
			
			// Get item stock information
			statement = session.prepare(
				"SELECT i_name, i_price, s_quantity, s_ytd, s_order_cnt, s_remote_cnt " 
						+ "FROM item WHERE i_w_id = ? AND i_id = ?"
			);
			results = session.execute(statement.bind(w_id, itemNum[i]));
			Row item = results.one();
			
			String itemName = item.getString("i_name");
			int s_quantity = item.getInt("s_quantity");
			double price = item.getDouble("price");
			double s_ytd = item.getDouble("s_ytd");
			int s_order_cnt = item.getInt("s_order_cnt");
			int s_remote_cnt = item.getInt("s_remote_cnt");
			
			// Adjust stock quantity
			s_quantity -= quantity[i];
			s_quantity = s_quantity < 10 ? s_quantity + 100 : s_quantity;
			
			// Update stock
			statement = session.prepare(
				"UPDATE item SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ? "
					+ "WHERE i_w_id = ? AND i_id = ?"
			);
			session.execute(statement.bind(s_quantity, s_ytd + quantity[i], s_order_cnt + 1, 
					s_remote_cnt + supplier[i] != w_id ? 1 : 0));
			
			double itemAmount = quantity[i] * price;
			totalAmount += itemAmount;
			
			// Create Order line
			UserType olType = session.getCluster().getMetadata().getKeyspace("d8").getUserType("orderline");
			UDTValue ol = olType.newValue().setInt("ol_i_id", i)
					.setString("ol_i_name", itemName)
					.setDouble("ol_amount", itemAmount)
					.setInt("ol_supply_w_id", supplier[i])
					.setInt("ol_quantity", quantity[i])
					;
			ols.add(ol);
			
			System.out.println("ITEM_NUMBER: " + itemNum[i]);
			System.out.println("I_NAME: " + itemName);
			System.out.println("SUPPLIER_WARHOUSE: " + supplier[i]);
			System.out.println("QUANTITY: " + quantity[i]);
			System.out.println("OL_AMOUNT: " + itemAmount);
			System.out.println("S_QUANTITY: " + s_quantity);
		}
		
		// Calculate total amount
		totalAmount = totalAmount * (1 + d_tax + w_tax) * (1 - c_discount);
		
		// Create a new order
		statement = session.prepare("INSERT INTO order2 " 
				+ "(o_w_id, o_d_id, o_id, o_c_id, c_first, c_middle, c_last, o_carrier_id, " 
				+ "o_ol_cn, o_all_local, o_entry_d, ols) "
				+ "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)"
		);
		
		// Insert new order
		Timestamp o_entry_d = new Timestamp(System.currentTimeMillis());
		session.execute(statement.bind(
				w_id, d_id, o_id, c_id, firstName, middleName, lastName, -1,
				numItems, o_all_local, o_entry_d, ols
		));
		
		System.out.println("O_ID: " + o_id);
		System.out.println("O_ENTRY_D: " + df.format(new Date(o_entry_d.getTime())));
		System.out.println("NUM_ITEMS: " + numItems);
		System.out.println("TOTAL_AMOUNT: " + totalAmount);
		
		return true;
	}
	
	// Get next order id and increment counter
	private static int reserveNextOid(Session session, int w_id, int d_id) {
		// Retrieve next oder id
		PreparedStatement statement = session.prepare("SELECT d_next_oid FROM district WHERE w_id = ? AND d_id = ?");
		ResultSet results = session.execute(statement.bind(w_id, d_id));
		Row district = results.one();
		if (district == null) {
			System.out.println("The required district does not exsit!");
			return -1;
		}
		
		int o_id = (int) district.getInt("d_next_oid");
		System.out.println("O_ID: " + o_id);
		
		// Increment
		statement = session.prepare("UPDATE district SET d_next_oid = ? WHERE w_id = ? AND d_id = ?");
		session.execute(statement.bind(o_id + 1, w_id, d_id));
		
		return o_id;
	}

}
