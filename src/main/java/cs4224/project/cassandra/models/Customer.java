package cs4224.project.cassandra.models;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Customer {
	private Session session;
	private String tablename = "customer";
	
	public Customer(Session session) {
		this.session = session;
	}
	public void Insert(int w_id, int d_id, int c_id, String c_first, String c_middle, String c_last) {
		String query = String.format("INSERT INTO " + tablename + 
				" (w_id, d_id, c_id, c_first, c_middle, c_last) VALUES (%d, %d, %d, '%s', '%s', '%s');"
				, w_id, d_id, c_id, c_first, c_middle, c_last);
		System.out.println(query);
		session.execute(query);
	}
	
	public int GetDeliveryCountById(int w_id, int d_id, int c_id){
		String query = String.format("SELECT * FROM CUSTOMER where w_id = %d and d_id = %d and c_id = %d;", w_id, d_id, c_id);
		System.out.println(query);
		int counter = -1;
		ResultSet results = session.execute(query);
		for (Row row : results) {
			counter = row.getInt("c_delivery_cnt");
		}
		System.out.println("Counter gotten: " + counter);
		return counter;
	}
	
	public Row GetById(int w_id, int d_id, int c_id){
		String query = String.format("SELECT * FROM CUSTOMER where w_id = %d and d_id = %d and c_id = %d;", w_id, d_id, c_id);
		System.out.println(query);
		ResultSet results = session.execute(query);
		for (Row row : results) {
			return row;
		}
		return null;
	}
	
	public void UpdateBalanceAndCount(int w_id, int d_id, int c_id, double balance){
		ResultSet results;
		Row temp = GetById(w_id, d_id, c_id);
		System.out.println("Before balance: " + temp.getDouble("c_balance"));
		if (temp != null){
			//first fetch the current c_delivery count
			int c_delivery_cnt = temp.getInt("c_delivery_cnt") + 1;
			double c_balance = temp.getDouble("c_balance");
			System.out.println("Balance to be deduced: " + balance);
			String query = String.format("UPDATE CUSTOMER set c_delivery_cnt = %d, "
					+ "c_balance = %f where "
				+ "w_id = %d and d_id = %d and c_id = %d;", c_delivery_cnt, c_balance - balance, w_id, d_id, c_id);
			System.err.println(query);
			try{
				results = session.execute(query);
				if (results.wasApplied()) {
					return;
				} else {
					try {
						Thread.sleep(100);
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
			
			//test
			temp = GetById(w_id, d_id, c_id);
			System.out.println("Updated balance: " + temp.getDouble("c_balance"));
		}
	}
}
