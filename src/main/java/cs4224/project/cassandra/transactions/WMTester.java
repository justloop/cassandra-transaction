package cs4224.project.cassandra.transactions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;

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
		
		// Test transaction
		int[] itemNum = {1, 2, 3};
		int[] supplier = {1, 2, 3};
		int[] quantity = {1, 2, 1};
		NewOrder.execute(session, 1, 1, 1, 3, itemNum, supplier, quantity);
		
		// Clean up
		cluster.close();
	}

}
