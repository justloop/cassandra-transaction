package cs4224.project.cassandra.transactions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import cs4224.project.cassandra.models.Order;

public class Delivery {

	private Cluster cluster;
	private Session session;
	
	public Delivery(){
		cluster = Cluster.builder().addContactPoint("localhost").build();
		System.out.println("Trying to connect...");
		session = cluster.connect("d8");
		System.out.println("Connected successfully...");
	}
	
	public static void main(String[] args) {
		Delivery a = new Delivery();
		try{
			a.Deliver(5, 0);
		}
		catch(Exception e){
		}
		finally{
			a.CloseCluster();
		}
		
	}
	
	public boolean Deliver(int w_id, int carrier_id){
		
		/*
		for DISTRICT NO 1 to 10
		(a) Let N denote the value of the smallest order number O ID for district (W ID,DISTRICT NO)
		with O CARRIER ID = null; i.e.,
		N =min{t.O ID∈Order|t.O W ID =W ID, t.D ID =DISTRICT NO,t.O CARRIER ID=null}
		Let X denote the order corresponding to order number N, and let C denote the customer who placed this order
		(b) Update the order X by setting O CARRIER ID to CARRIER ID
		(c) Update all the order-lines in X by setting OL DELIVERY D to the current date and time
		(d) Update customer C as follows:
		• Increment C BALANCE by B, where B denote the sum of OL AMOUNT for all the items placed in order X
		• Increment C DELIVERY CNT by 1
		*/
		Order order = new Order(session);
		//TODO change back to 0 to 10
		for(int i = 9; i <= 9; i++){
			ResultSet result = order.SelectMin(w_id, i);
			System.out.println("*******************");
			System.out.println(result);
			//shuld have only one result
			for (Row row : result) {
				//Update the order X by setting O CARRIER ID to CARRIER ID
				order.UpdateCarrier(row.getInt("o_w_id"), row.getInt("o_d_id"), row.getInt("o_id"), row.getInt("o_c_id"), carrier_id);
			}
		}
		
		return false;
	}
	
	public void CloseCluster(){
		cluster.close();
	}
}
