package cs4224.project.cassandra.models.wm;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class District {
	
	public static long getNextOid(Session session, int w_id, int d_id) {
		String keyspace = session.getLoggedKeyspace();
		Statement statement;
		ResultSet results;
		
		// Find next available order id
		statement = QueryBuilder.select()
							.column("d_next_oid")
							.from(keyspace, "district")
							.where(eq("w_id", w_id))
							.and(eq("d_id", d_id))
							;
		
		results = session.execute(statement);
		Row district = results.one();
		if (district == null) {
			System.out.println("The required district does not exsit!");
			return -1;
		}
		
		return district.getLong("d_next_oid");
	}

}
