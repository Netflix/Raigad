package com.netflix.elasticcar.utils;

import com.netflix.elasticcar.utils.Sleeper;

public class FakeSleeper implements Sleeper
{
    @Override
    public void sleep(long waitTimeMs) throws InterruptedException
    {
        // no-op
    }

    public void sleepQuietly(long waitTimeMs)
    {
        //no-op
    }
}
