package cs4224.project.cassandra.transactions;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Payment {
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * Execute a payment transaction.
	 * @param session
	 * @param w_id
	 * @param d_id
	 * @param c_id
	 * @param payment
	 * @return
	 */
	public static boolean execute(Session session, int w_id, int d_id, int c_id, 
			double payment) {
		PreparedStatement statement;
		ResultSet results;
		
		// Update warehouse
		statement = session.prepare(
			"SELECT * FROM warehouse WHERE w_id = ?"
		);
		
		results = session.execute(statement.bind(w_id));
		Row warehouse = results.one();
		if (warehouse == null) {
			System.out.println("The requried warehose does not exist");
			return false;
		}
		
		statement = session.prepare(
			"UPDATE warehouse SET w_ytd = ? WHERE w_id = ?"
		);
		session.execute(statement.bind(warehouse.getDouble("w_ytd") + payment));
		
		// Update district
		statement = session.prepare(
			"SELECT * FROM district WHERE w_id = ? AND d_id = ?"
		);
			
		results = session.execute(statement.bind(w_id, d_id));
		Row district = results.one();
		if (district == null) {
			System.out.println("The requried district does not exist");
			return false;
		}
		
		statement = session.prepare(
			"UPDATE district SET d_ytd = ? WHERE w_id = ? AND d_id = ?"
		);
		session.execute(statement.bind(district.getDouble("d_ytd") + payment));
		
		// Update customer
		statement = session.prepare(
			"SELECT * FROM customer WHERE w_id = ? AND d_id = ? AND c_id = ?"
		);
			
		results = session.execute(statement.bind(w_id, d_id));
		Row customer = results.one();
		if (customer == null) {
			System.out.println("The requried customer does not exist");
			return false;
		}
		
		statement = session.prepare(
			"UPDATE customer SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? " + 
					"WHERE w_id = ? AND d_id = ? AND c_id = ?"
		);
		session.execute(statement.bind(
			customer.getDouble("c_balance") - payment,
			customer.getDouble("c_ytd_payment") + payment,
			customer.getInt("c_payment_cnt") + 1
		));
		
		System.out.println("C_W_ID:" + w_id);
		System.out.println("C_D_ID:" + d_id);
		System.out.println("C_ID:" + c_id);
		
		System.out.println("C_FIRST:" + customer.getString("c_first"));
		System.out.println("C_MIDDLE:" + customer.getString("c_middle"));
		System.out.println("C_LAST:" + customer.getString("c_last"));
		
		System.out.println("C_STREET_1:" + customer.getString("c_street_1"));
		System.out.println("C_STREET_2:" + customer.getString("c_street_2"));
		System.out.println("C_CITY:" + customer.getString("c_city"));
		System.out.println("C_STATE:" + customer.getString("c_state"));
		System.out.println("C_ZIP:" + customer.getString("c_zip"));
		
		System.out.println("C_PHONE:" + customer.getString("c_phone"));
		System.out.println("C_SINCE:" + df.format(new Date(customer.getTimestamp("c_since").getTime())));
		System.out.println("C_CREDIT:" + customer.getString("c_credit"));
		System.out.println("C_REDIT_LIM:" + customer.getDouble("c_credit_lim"));
		System.out.println("C_DISCOUNT:" + customer.getDouble("c_discount"));
		System.out.println("C_BALANCE:" + customer.getDouble("c_balance"));
		
		System.out.println("W_STREET_1:" + customer.getDouble("w_street_1"));
		System.out.println("W_STREET_2:" + customer.getDouble("w_street_2"));
		System.out.println("W_CITY:" + customer.getDouble("w_city"));
		System.out.println("W_STATE:" + customer.getDouble("w_state"));
		System.out.println("W_ZIP:" + customer.getDouble("w_zip"));
		
		System.out.println("D_STREET_1:" + customer.getDouble("d_street_1"));
		System.out.println("D_STREET_2:" + customer.getDouble("d_street_2"));
		System.out.println("D_CITY:" + customer.getDouble("d_city"));
		System.out.println("D_STATE:" + customer.getDouble("d_state"));
		System.out.println("D_ZIP:" + customer.getDouble("d_zip"));
		
		System.out.println("PAYMENT:" + payment);
		
		return true;
	}

}
