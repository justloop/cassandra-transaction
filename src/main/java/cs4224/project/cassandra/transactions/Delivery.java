package cs4224.project.cassandra.transactions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class Delivery {

	private Cluster cluster;
	private Session session;
	
	public Delivery(){
		cluster = Cluster.builder().addContactPoint("localhost").build();
		session = cluster.connect("d8");
	}
	
	public boolean Deliver(int w_id, int carrier_id){
		//for DISTRICT NO 1 to 10
		/*
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
		for(int i = 1; i <= 10; i++){
			//N =min{t.O ID∈Order|t.O W ID =W ID, t.D ID =DISTRICT NO,t.O CARRIER ID=null}
			
			//for this o_id
			
		}
		
		return false;
	}
}
