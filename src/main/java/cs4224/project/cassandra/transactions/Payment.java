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
		// Update warehouse
		updateWarehouse(session, w_id, payment);
		
		// Update district
		updateDistrict(session, w_id, d_id, payment);
		
		// Update customer
		Row customer = updateCustomer(session, w_id, d_id, c_id, payment);
		
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
		
		System.out.println("W_STREET_1:" + customer.getString("w_street_1"));
		System.out.println("W_STREET_2:" + customer.getString("w_street_2"));
		System.out.println("W_CITY:" + customer.getString("w_city"));
		System.out.println("W_STATE:" + customer.getString("w_state"));
		System.out.println("W_ZIP:" + customer.getString("w_zip"));
		
		System.out.println("D_STREET_1:" + customer.getString("d_street_1"));
		System.out.println("D_STREET_2:" + customer.getString("d_street_2"));
		System.out.println("D_CITY:" + customer.getString("d_city"));
		System.out.println("D_STATE:" + customer.getString("d_state"));
		System.out.println("D_ZIP:" + customer.getString("d_zip"));
		
		System.out.println("PAYMENT:" + payment);
		
		return true;
	}
	
	private static PreparedStatement selectWarehouse;
	private static PreparedStatement updateWarehouse;
	
	/**
	 * Try until update pass.
	 * @param session
	 * @param w_id
	 * @param payment
	 * @return
	 */
	private static Row updateWarehouse(Session session, int w_id, double payment) {
		ResultSet results;
		
		while (true) {
			if (selectWarehouse == null) {
				selectWarehouse = session.prepare(
					"SELECT * FROM warehouse WHERE w_id = ?"
				);
			}
			results = session.execute(selectWarehouse.bind(w_id));
			Row warehouse = results.one();
			
			if (updateWarehouse == null) {
				updateWarehouse = session.prepare(
					"UPDATE warehouse SET w_ytd = ? WHERE w_id = ? IF w_ytd > ? AND w_ytd < ?"
				);
			}
			results = session.execute(updateWarehouse.bind(
				warehouse.getDouble("w_ytd") + payment, w_id, 
				warehouse.getDouble("w_ytd") - 0.1,
				warehouse.getDouble("w_ytd") + 0.1
			));
			
			if (results.wasApplied()) {
				return warehouse;
			} else {
				System.out.println("Fail to update warehouse");
				TransactionUtils.randomSleep();
			}
		}
	}
	
	private static PreparedStatement selectDistrict;
	private static PreparedStatement updateDistrict;
	
	private static Row updateDistrict(Session session, int w_id, int d_id, double payment) {
		ResultSet results;
		
		while (true) {
			if (selectDistrict == null) {
				selectDistrict = session.prepare(
					"SELECT * FROM district WHERE w_id = ? AND d_id = ?"
				);
			}
			results = session.execute(selectDistrict.bind(w_id, d_id));
			Row district = results.one();
			
			if (updateDistrict == null) {
				updateDistrict = session.prepare(
					"UPDATE district SET d_ytd = ? WHERE w_id = ? AND d_id = ? IF d_ytd > ? AND d_ytd < ?"
				);
			}
			session.execute(updateDistrict.bind(
				district.getDouble("d_ytd") + payment, w_id, d_id, 
				district.getDouble("d_ytd") - 0.1,
				district.getDouble("d_ytd") + 0.1
			));
			
			if (results.wasApplied()) {
				return district;
			} else {
				System.out.println("Fail to update district");
				TransactionUtils.randomSleep();
			}
		}
	}
	
	private static PreparedStatement selectCustomer;
	private static PreparedStatement updateCustomer;
	
	private static Row updateCustomer(Session session, int w_id, int d_id, int c_id, 
			double payment) {
		ResultSet results;
		
		while (true) {
			if (selectCustomer == null) {
				selectCustomer = session.prepare(
					"SELECT * FROM customer WHERE w_id = ? AND d_id = ? AND c_id = ?"
				);
			}
			results = session.execute(selectCustomer.bind(w_id, d_id, c_id));
			Row customer = results.one();
			
			if (updateCustomer == null) {
				updateCustomer = session.prepare(
					"UPDATE customer SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? "
							+ "WHERE w_id = ? AND d_id = ? AND c_id = ? "
							+ "IF c_payment_cnt = ?"
				);
			}
			session.execute(updateCustomer.bind(
				customer.getDouble("c_balance") - payment,
				customer.getDouble("c_ytd_payment") + payment,
				customer.getInt("c_payment_cnt") + 1,
				w_id, d_id, c_id,
				customer.getInt("c_payment_cnt")
			));
			
			if (results.wasApplied()) {
				return customer;
			} else {
				System.out.println("Fail to update customer");
				TransactionUtils.randomSleep();
			}
		}
	}

}
