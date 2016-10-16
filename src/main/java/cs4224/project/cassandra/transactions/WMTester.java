package cs4224.project.cassandra.transactions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.policies.DefaultRetryPolicy;

import cs4224.project.cassandra.models.Orderline;
import cs4224.project.cassandra.models.OrderlineCodec;

/**
 * Testing commit.
 * @author chenweiming
 *
 */
public class WMTester {
	
	public static void main(String[] args) {
		Cluster cluster;
		Session session;
		CodecRegistry codecRegistry = new CodecRegistry();
		
		cluster = Cluster
				.builder()
				.addContactPoint("127.0.0.1")
				.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
				.withCodecRegistry(codecRegistry)
				.build();
		
		UserType orderlineType = cluster.getMetadata().getKeyspace("d8").getUserType("Orderline");
		TypeCodec<UDTValue> orderlineTypeCodec = codecRegistry.codecFor(orderlineType);
		OrderlineCodec orderlineCodec = new OrderlineCodec(orderlineTypeCodec, Orderline.class);
		codecRegistry.register(orderlineCodec);
		
		session = cluster.connect("d8");
		
		
		
		// Test new order
		/*
		int[] itemNum = {1, 2};
		int[] supplier = {1, 1};
		int[] quantity = {1, 2};
		NewOrder.execute(session, 1, 1, 1, 2, itemNum, supplier, quantity);
		*/
		
		// Test payment
		//Payment.execute(session, 1, 1, 1, 100);
		
		// Test delivery
		Delivery.execute(session, 1, 100);
		
		
		// Test order status
		//OrderStatus.execute(session, 1, 1, 1);
		
		// Test stock level
		//StockLevel.execute(session, 1, 1, 100, 5);
		
		// Test popular item
		//PopularItem.execute(session, 2, 2, 10);
		
		// Test top 10
		//TopBalance.execute(session);
		
		// Clean up
		cluster.close();
	}

}
