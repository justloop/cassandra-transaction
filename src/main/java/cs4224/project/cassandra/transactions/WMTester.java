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
		OrderStatus.execute(session, 1, 1, 1);
		
		System.out.println(session.getLoggedKeyspace());
		
		// Clean up
		cluster.close();
	}

}
