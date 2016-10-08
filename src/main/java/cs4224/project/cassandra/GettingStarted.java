package cs4224.project.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import cs4224.project.cassandra.models.Warehouse;

public class GettingStarted {
	public static Logger logger = LoggerFactory.getLogger(GettingStarted.class);
	
	public static void main(String[] args) {
		Cluster cluster;
		Session session;
		System.out.println("code starts...");
		// Connect to the cluster and keyspace "demo"
		cluster = Cluster.builder().addContactPoint("localhost").build();
		session = cluster.connect("d8");
		Warehouse wh = new Warehouse(session);
		
		wh.Insert(2, 0.01);
		wh.PrintAll();
		wh.Update(2, 0.1);
		wh.PrintAll();
		wh.Delete(2);
		wh.PrintAll();
		cluster.close();
	}

}
