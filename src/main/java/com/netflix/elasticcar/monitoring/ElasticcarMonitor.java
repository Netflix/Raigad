package com.netflix.elasticcar.monitoring;

import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by sloke on 7/16/14.
 */
public class ElasticcarMonitor
{
    // all the counters
    public final AtomicInteger snapshotSuccess = new AtomicInteger(0);
    public final AtomicInteger snapshotFailure = new AtomicInteger(0);

    @Monitor(name="snapshotSuccess", type= DataSourceType.COUNTER)
    public int getSnapshotSuccess() {
        return snapshotSuccess.get();
    }

    @Monitor(name="snapshotFailure", type=DataSourceType.COUNTER)
    public int getSnapshotFailure() {
        return snapshotFailure.get();
    }

    public String getStatus() {
        StringBuilder sb = new StringBuilder("ElasticcarMonitor ");
        sb.append("\tsnapshotSuccess: " + snapshotSuccess.get());
        sb.append("\tsnapshotFailure: " + snapshotFailure.get());
        sb.append("\n");
        return sb.toString();
    }
}
