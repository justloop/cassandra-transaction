package cs4224.project.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import cs4224.project.cassandra.models.Customer;
import cs4224.project.cassandra.models.District;
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
		/*
		Warehouse wh = new Warehouse(session);
		wh.Insert(1, 0.01);
		wh.Insert(3, 0.01);
		wh.Insert(4, 0.01);
		wh.Insert(5, 0.01);
		wh.PrintAll();
		*/
		
		/*
		District d = new District(session);
		d.Insert(1, 1, 1, 0.02);
		d.Insert(1, 2, 1, 0.02);
		d.Insert(1, 3, 1, 0.02);
		d.Insert(1, 4, 1, 0.02);
		
		d.Insert(2, 5, 1, 0.02);
		d.Insert(2, 6, 1, 0.02);
		d.Insert(2, 7, 1, 0.02);
		d.Insert(2, 8, 1, 0.02);
		*/
		
		Customer c = new Customer(session);
		c.Insert(1, 1, 2, "a2", "b2", "c2");
		c.Insert(1, 1, 3, "a3", "b3", "c3");
		c.Insert(1, 1, 4, "a4", "b4", "c4");
		c.Insert(1, 2, 5, "a1", "b1", "c1");
		c.Insert(1, 2, 6, "a2", "b2", "c2");
		c.Insert(1, 2, 7, "a3", "b3", "c3");
		c.Insert(1, 2, 8, "a4", "b4", "c4");
		
		cluster.close();
	}

}
