package cs4224.project.cassandra.models;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class Order {
	private static String tablename = "order2";
	
	public static ResultSet SelectMin(Session session, int o_w_id, int o_d_id){
		//System.out.println("Trying to select min...");
		String query = String.format("SELECT * FROM %s where o_w_id = %d and o_d_id = %d and o_carrier_id = -1 limit 1;"
				, tablename, o_w_id, o_d_id);
		//System.out.println(query);
		ResultSet results = session.execute(query);
		return results;
	}
	
	//update the order X by setting O CARRIER ID to CARRIER ID
	//Update all the order-lines in X by setting OL DELIVERY D to the current date and time
	public static void UpdateCarrier(Session session, int o_w_id, int o_d_id, int o_id, int carrier_id){
		//System.out.println("in UpdateCarrier..");
		String query = String.format(
				"UPDATE order2 set o_carrier_id = %d, ol_delivery_d = dateof(now()) where o_w_id = %d and o_d_id = %d and o_id = %d;",
				carrier_id, o_w_id, o_d_id, o_id);
		//System.out.println(query);
		session.execute(query);
	}
}
