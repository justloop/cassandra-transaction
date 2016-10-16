package cs4224.project.cassandra.models;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import cs4224.project.cassandra.transactions.TransactionUtils;

public class Customer {
	public static String tablename = "customer";
	
	public static Row GetById(Session session, int w_id, int d_id, int c_id){
		String query = String.format("SELECT c_delivery_cnt, c_balance FROM CUSTOMER where w_id = %d and d_id = %d and c_id = %d;", w_id, d_id, c_id);
		System.out.println(query);
		ResultSet results = session.execute(query);
		for (Row row : results) {
			return row;
		}
		return null;
	}
	
	public static void UpdateBalanceAndCount(Session session, int w_id, int d_id, int c_id, double balance){
		ResultSet results;
		Row temp = GetById(session, w_id, d_id, c_id);
		System.out.println("Before balance: " + temp.getDouble("c_balance"));
		if (temp != null){
			//first fetch the current c_delivery count
			int old_c_delivery_cnt = temp.getInt("c_delivery_cnt");
			int c_delivery_cnt = temp.getInt("c_delivery_cnt") + 1;
			double c_balance = temp.getDouble("c_balance");
			System.out.println("Balance to be added: " + balance);
			String query = String.format("UPDATE CUSTOMER set c_delivery_cnt = %d, "
					+ "c_balance = %f where "
				+ "w_id = %d and d_id = %d and c_id = %d if c_delivery_cnt = %d;"
				, c_delivery_cnt, c_balance + balance, w_id, d_id, c_id, old_c_delivery_cnt);
			System.out.println(query);
			
			try{
				boolean flag = true;
				int counter = 0;
				while(flag && counter < 3){
					counter++;
					results = session.execute(query);
					if (results.wasApplied()) {
						flag = false;
					} else {
						try {
							TransactionUtils.randomSleep();
						} catch (Exception ex) {
							ex.printStackTrace();
						}
					}
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}
