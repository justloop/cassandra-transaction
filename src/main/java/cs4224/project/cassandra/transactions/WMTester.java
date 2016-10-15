package cs4224.project.cassandra.transactions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;

/**
 * Testing commit.
 * @author chenweiming
 *
 */
public class WMTester {
	
	public static void main(String[] args) {
		Cluster cluster;
		Session session;
		
		cluster = Cluster
				.builder()
				.addContactPoint("127.0.0.1")
				.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
				.build();
		session = cluster.connect("d8");
		
		// Test new order
		int[] itemNum = {1, 2};
		int[] supplier = {1, 1};
		int[] quantity = {1, 2};
		NewOrder.execute(session, 1, 1, 1, 2, itemNum, supplier, quantity);
		
		
		// Test payment
		Payment.execute(session, 1, 1, 1, 100);
		
		/*
		// Test order status
		OrderStatus.execute(session, 1, 1, 1);
		*/
		
		// Test stock level
		//StockLevel.execute(session, 1, 1, 50, 5);
		
		// Test popular item
		//PopularItem.execute(session, 1, 1, 5);
		
		// Test top 10
		//TopBalance.execute(session);
		
		// Clean up
		cluster.close();
	}

}
