package com.netflix.raigad.utils;

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
