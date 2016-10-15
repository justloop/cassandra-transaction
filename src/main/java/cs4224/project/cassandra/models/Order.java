package cs4224.project.cassandra.models;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class Order {
	private Session session;
	private String tablename = "order2";
	public Order(Session session) {
		this.session = session;
	}

	public void Insert() {

	}
	
	public ResultSet SelectMin(int o_w_id, int o_d_id){
		System.out.println("Trying to select min...");
		String query = String.format("SELECT * FROM %s where o_w_id = %d and o_d_id = %d and o_carrier_id = -1 limit 1;", tablename, o_w_id, o_d_id);
		System.out.println(query);
		ResultSet results = session.execute(query);
		return results;
	}
	
	//pdate the order X by setting O CARRIER ID to CARRIER ID
	//Update all the order-lines in X by setting OL DELIVERY D to the current date and time
	public void UpdateCarrier(int o_w_id, int o_d_id, int o_id, int carrier_id){
		System.out.println("in UpdateCarrier..");
		String query = String.format(
				"UPDATE order2 set o_carrier_id = %d, ol_delivery_d = dateof(now()) where o_w_id = %d and o_d_id = %d and o_id = %d;",
				carrier_id, o_w_id, o_d_id, o_id);
		System.out.println(query);
		session.execute(query);
	}
}
