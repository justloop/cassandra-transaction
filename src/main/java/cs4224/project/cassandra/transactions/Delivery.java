package cs4224.project.cassandra.transactions;

import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import cs4224.project.cassandra.models.Customer;
import cs4224.project.cassandra.models.Order;
import cs4224.project.cassandra.models.Orderline;

public class Delivery {
    public static boolean execute(Session session, int w_id, int carrier_id) {
        Order order = new Order(session);
        Customer customer = new Customer(session);
        //TODO change back to 0 to 10
        for (int i = 1; i <= 10; i++) {
            ResultSet result = order.SelectMin(w_id, i);
            //shuld have only one result
            for (Row row : result) {
                //Update the order X by setting O CARRIER ID to CARRIER ID
                order.UpdateCarrier(row.getInt("o_w_id"), row.getInt("o_d_id"), row.getInt("o_id"), carrier_id);
                Set<Orderline> temp = row.getSet("ols", Orderline.class);
                double sum = 0.0;

                for (Orderline j : temp)
                    sum += j.getAmount();
                System.err.println("************IN LOOP**************" + sum);
                customer.UpdateBalanceAndCount(row.getInt("o_w_id"), row.getInt("o_d_id"), row.getInt("o_c_id"), sum);
            }
        }

        return false;
    }

}
