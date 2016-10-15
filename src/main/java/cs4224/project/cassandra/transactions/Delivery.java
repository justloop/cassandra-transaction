package cs4224.project.cassandra.transactions;

import java.util.Set;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

import cs4224.project.cassandra.models.Customer;
import cs4224.project.cassandra.models.Order;
import cs4224.project.cassandra.models.Orderline;
import cs4224.project.cassandra.models.OrderlineCodec;

public class Delivery {

	private Cluster cluster;
	private Session session;
	
	public Delivery(){
		CodecRegistry codecRegistry = new CodecRegistry();
		cluster = Cluster.builder().addContactPoint("localhost").withCodecRegistry(codecRegistry).build();
		
		UserType orderlineType = cluster.getMetadata().getKeyspace("d8").getUserType("Orderline");
		TypeCodec<UDTValue> orderlineTypeCodec = codecRegistry.codecFor(orderlineType);
		OrderlineCodec orderlineCodec = new OrderlineCodec(orderlineTypeCodec, Orderline.class);
		codecRegistry.register(orderlineCodec);

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
		Customer customer = new Customer(session);
		//TODO change back to 0 to 10
		for(int i = 9; i <= 9; i++){
			ResultSet result = order.SelectMin(w_id, i);
			//shuld have only one result
			for (Row row : result) {
				//Update the order X by setting O CARRIER ID to CARRIER ID
				order.UpdateCarrier(row.getInt("o_w_id"), row.getInt("o_d_id"), row.getInt("o_id"), row.getInt("o_c_id"), carrier_id);
				
				Set<Orderline> temp = row.getSet("ols", Orderline.class);
				double sum = 0.0;
							
				for(Orderline j: temp)
					sum += j.getAmount();
				System.err.println("************IN LOOP**************" + sum);	
				customer.UpdateBalanceAndCount(row.getInt("o_w_id"), row.getInt("o_d_id"), row.getInt("o_c_id"), sum);
				System.err.println("************Finished**************");
				
			}
		}
		
		return false;
	}
	
	public void CloseCluster(){
		cluster.close();
	}
}
