package cs4224.project.cassandra.transactions;

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

import cs4224.project.cassandra.Driver;

public class NewOrder {
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private static PreparedStatement selectCustomer;
	private static PreparedStatement insertOrder;
	private static PreparedStatement updateCustomer;
	private static UserType olType;
	
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
		ResultSet results;
		
		// Reserver next order id
		int o_id = reserveNextOid(session, w_id, d_id);
		
		// Customer
		if (selectCustomer == null) {
			selectCustomer = session.prepare("SELECT * FROM customer WHERE w_id = ? AND d_id = ? AND c_id = ?");
		}
		
		results = session.execute(selectCustomer.bind(w_id, d_id, c_id));
		Row consumer = results.one();
		
		String firstName = consumer.getString("c_first");
		String middleName = consumer.getString("c_middle");
		String lastName = consumer.getString("c_last");
		String c_credit = consumer.getString("c_credit");
		double c_discount = consumer.getDouble("c_discount");
		double w_tax = consumer.getDouble("w_tax");
		double d_tax = consumer.getDouble("d_tax");
		
		//System.out.println("W_ID: " + w_id);
		//System.out.println("D_ID: " + d_id);
		//System.out.println("C_ID: " + c_id);
		//System.out.println("C_LAST: " + lastName);
		//System.out.println("C_CREDIT: " + c_credit);
		//System.out.println("C_DISCOUNT: " + c_discount);
		
		//System.out.println("W_TAX: " + w_tax);
		//System.out.println("D_TAX: " + d_tax);
		
		// All local ?
		int o_all_local = 0;
		// Order lines
		Set<UDTValue> ols = new HashSet<>();
		// Total amount
		double totalAmount = 0.0;
		
		// Process each item
		for (int i = 0; i < numItems; i++) {
			if (supplier[i] != w_id) o_all_local = 1;
			
			// Update stock
			Row item = updateItemStock(session, w_id, itemNum[i], supplier[i], quantity[i]);
			
			String itemName = item.getString("i_name");
			int s_quantity = item.getInt("s_quantity");
			double i_price = item.getDouble("i_price");
			
			double itemAmount = quantity[i] * i_price;
			totalAmount += itemAmount;
			
			// Create Order line
			if (olType == null) {
				olType = session.getCluster().getMetadata().getKeyspace(Driver.keyspace)
						.getUserType("orderline");
			}
			UDTValue ol = olType.newValue().setInt("ol_i_id", itemNum[i])
					.setString("ol_i_name", itemName)
					.setDouble("ol_amount", itemAmount)
					.setInt("ol_supply_w_id", supplier[i])
					.setInt("ol_quantity", quantity[i])
					;
			ols.add(ol);
			
			//System.out.println("ITEM_NUMBER: " + itemNum[i]);
			//System.out.println("I_NAME: " + itemName);
			//System.out.println("SUPPLIER_WARHOUSE: " + supplier[i]);
			//System.out.println("QUANTITY: " + quantity[i]);
			//System.out.println("OL_AMOUNT: " + itemAmount);
			//System.out.println("S_QUANTITY: " + s_quantity);
		}
		
		// Calculate total amount
		totalAmount = totalAmount * (1 + d_tax + w_tax) * (1 - c_discount);
		
		// Create a new order
		if (insertOrder == null) {
			insertOrder = session.prepare("INSERT INTO order2 " 
					+ "(o_w_id, o_d_id, o_id, o_c_id, c_first, c_middle, c_last, o_carrier_id, " 
					+ "o_ol_cnt, o_all_local, o_entry_d, ols) "
					+ "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)"
			);
		}
		
		// Insert new order
		Date o_entry_d = new Date(System.currentTimeMillis());
		session.execute(insertOrder.bind(
				w_id, d_id, o_id, c_id, firstName, middleName, lastName, -1,
				numItems, o_all_local, o_entry_d, ols
		));
		
		// Update consumer last order id
		if (updateCustomer == null) {
			updateCustomer = session.prepare(
				"UPDATE customer SET o_id = ? WHERE w_id = ? AND d_id = ? AND c_id = ?"
			);
		}
		session.execute(updateCustomer.bind(o_id, w_id, d_id, c_id));
		
		//System.out.println("O_ID: " + o_id);
		//System.out.println("O_ENTRY_D: " + df.format(o_entry_d));
		//System.out.println("NUM_ITEMS: " + numItems);
		//System.out.println("TOTAL_AMOUNT: " + totalAmount);
		
		return true;
	}
	
	private static PreparedStatement selectDistrict;
	private static PreparedStatement updateDistrict;
	
	// Get next order id and increment counter
	private static int reserveNextOid(Session session, int w_id, int d_id) {
		ResultSet results;
		
		while (true) {
			// Retrieve next oder id
			if (selectDistrict == null) {
				selectDistrict = session.prepare("SELECT d_next_oid FROM district WHERE w_id = ? AND d_id = ?");
			}
			results = session.execute(selectDistrict.bind(w_id, d_id));
			Row district = results.one();
			
			int o_id = district.getInt("d_next_oid");
			// Increment
			if (updateDistrict == null) {
				updateDistrict = session.prepare(
					"UPDATE district SET d_next_oid = ? WHERE w_id = ? AND d_id = ? IF d_next_oid = ?"
				);
			}
			results = session.execute(updateDistrict.bind(o_id + 1, w_id, d_id, o_id));
			
			if (results.wasApplied()) {
				return o_id;
			} else {
				//System.out.println("Fail to update district");
				TransactionUtils.randomSleep();
			}
		}
	}
	
	private static PreparedStatement selectItem;
	private static PreparedStatement updateItem;
	
	private static Row updateItemStock(Session session, int w_id, int i_id, int supplier,
			int quantity) {
		ResultSet results;
		
		while (true) {
			// Get item stock information
			if (selectItem == null) {
				selectItem = session.prepare(
					"SELECT i_name, i_price, s_quantity, s_ytd, s_order_cnt, s_remote_cnt " 
							+ "FROM item WHERE i_w_id = ? AND i_id = ?"
				);
			}
			results = session.execute(selectItem.bind(supplier, i_id));
			Row item = results.one();
			
			int s_quantity = item.getInt("s_quantity");
			double s_ytd = item.getDouble("s_ytd");
			int s_order_cnt = item.getInt("s_order_cnt");
			int s_remote_cnt = item.getInt("s_remote_cnt");
			
			// Adjust stock quantity
			s_quantity -= quantity;
			s_quantity = s_quantity < 10 ? s_quantity + 100 : s_quantity;
			
			// Update stock
			if (updateItem == null) {
				updateItem = session.prepare(
					"UPDATE item SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ? "
						+ "WHERE i_w_id = ? AND i_id = ? IF s_order_cnt = ?"
				);
			}
			results = session.execute(updateItem.bind(s_quantity, s_ytd + quantity, s_order_cnt + 1, 
					s_remote_cnt + supplier != w_id ? 1 : 0, supplier, i_id, s_order_cnt));
			
			if (results.wasApplied()) {
				return item;
			} else {
				//System.out.println("Fail to update stock");
				TransactionUtils.randomSleep();
			}
		}		
	}
}
