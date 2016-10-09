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
		PopularItem.execute(session, 1, 1, 4);
		
		// Clean up
		cluster.close();
	}

}
