package cs4224.project.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * Created by gejun on 16/10/16.
 */
public class CustomRetryPolicy implements RetryPolicy {

    private final int readAttempts;
    private final int writeAttempts;
    private final int unavailableAttempts;

    public CustomRetryPolicy(int readAttempts, int writeAttempts, int unavailableAttempts) {
        this.readAttempts = readAttempts;
        this.writeAttempts = writeAttempts;
        this.unavailableAttempts = unavailableAttempts;
    }

    @Override
    public RetryDecision onReadTimeout(Statement stmnt, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataReceived, int rTime) {
        if (dataReceived) {
            return RetryDecision.ignore();
        } else if (rTime < readAttempts) {
            return RetryDecision.retry(cl);
        } else {
            return RetryDecision.rethrow();
        }

    }

    @Override
    public RetryDecision onWriteTimeout(Statement stmnt, ConsistencyLevel cl, WriteType wt, int requiredResponses, int receivedResponses, int wTime) {
        if (wTime < writeAttempts) {
            return RetryDecision.retry(cl);
        }
        return RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onUnavailable(Statement stmnt, ConsistencyLevel cl, int requiredResponses, int receivedResponses, int uTime) {
        if (uTime < unavailableAttempts) {
            return RetryDecision.retry(ConsistencyLevel.ONE);
        }
        return RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel consistencyLevel, DriverException e, int i) {
        return null;
    }

    @Override
    public void init(Cluster cluster) {

    }

    @Override
    public void close() {

    }

}