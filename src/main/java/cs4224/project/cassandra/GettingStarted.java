package cs4224.project.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

import cs4224.project.cassandra.models.Customer;
import cs4224.project.cassandra.models.District;
import cs4224.project.cassandra.models.Orderline;
import cs4224.project.cassandra.models.OrderlineCodec;
import cs4224.project.cassandra.models.Warehouse;
import cs4224.project.cassandra.transactions.Delivery;

public class GettingStarted {
	public static Logger logger = LoggerFactory.getLogger(GettingStarted.class);
	
	public static void main(String[] args) {
		
		CodecRegistry codecRegistry = new CodecRegistry();
		Cluster cluster = Cluster.builder().addContactPoint("localhost").withCodecRegistry(codecRegistry).build();
		UserType orderlineType = cluster.getMetadata().getKeyspace("d8").getUserType("Orderline");
		TypeCodec<UDTValue> orderlineTypeCodec = codecRegistry.codecFor(orderlineType);
		OrderlineCodec orderlineCodec = new OrderlineCodec(orderlineTypeCodec, Orderline.class);
		codecRegistry.register(orderlineCodec);

		System.out.println("Trying to connect...");
		Session session = cluster.connect("d8");
		System.out.println("Connected successfully...");
		Delivery.execute(session, 5, 9);
		
		cluster.close();
	}

}
