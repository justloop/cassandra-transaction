package cs4224.project.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Throwables;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by gejun on 16/10/16.
 */
public class Utils {
    public static ResultSet execute(Session session, Statement statement, long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException {
        ResultSetFuture future = session.executeAsync(statement);
        try {
            return future.get(timeout, unit);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        } catch (TimeoutException e) {
            future.cancel(true);
            throw e;
        }
    }
}
