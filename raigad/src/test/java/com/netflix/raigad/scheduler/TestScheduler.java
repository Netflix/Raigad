package com.netflix.raigad.scheduler;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.configuration.UnitTestModule;
import org.junit.Ignore;
import org.junit.Test;

import javax.management.MBeanServerFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TestScheduler {
    private static CountDownLatch latch;

    @Test
    public void testSchedule() throws Exception {
        latch = new CountDownLatch(1);
        Injector inject = Guice.createInjector(new UnitTestModule());
        RaigadScheduler scheduler = inject.getInstance(RaigadScheduler.class);
        scheduler.start();
        scheduler.addTask("test", TestTask.class, new SimpleTimer("testtask", 10));
        // verify the task has run or fail in 1s
        latch.await(1000, TimeUnit.MILLISECONDS);
        scheduler.shutdown();
    }
/*
    @Test
    public void testSingleInstanceSchedule() throws Exception {
        latch = new CountDownLatch(3);
        Injector inject = Guice.createInjector(new UnitTestModule());
        RaigadScheduler scheduler = inject.getInstance(RaigadScheduler.class);
        scheduler.start();
        scheduler.addTask("test2", SingleTestTask.class, SingleTestTask.getTimer());
        // verify 3 tasks run or fail in 1s
        latch.await(4000, TimeUnit.MILLISECONDS);
        scheduler.shutdown();
        assertEquals(3, SingleTestTask.count);
    }*/

    @Ignore
    public static class TestTask extends Task {
        @Inject
        public TestTask(IConfiguration config) {
            // todo: mock the MBeanServer instead, but this will prevent exceptions due to duplicate registrations
            super(config, MBeanServerFactory.newMBeanServer());
        }

        @Override
        public void execute() {
            latch.countDown();
        }

        @Override
        public String getName() {
            return "test";
        }

    }

    @Ignore
    @Singleton
    public static class SingleTestTask extends Task {
        @Inject
        public SingleTestTask(IConfiguration config) {
            super(config, MBeanServerFactory.newMBeanServer());
        }

        public static int count = 0;

        @Override
        public void execute() {
            ++count;
            latch.countDown();
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public String getName() {
            return "test2";
        }

        public static TaskTimer getTimer() {
            return new SimpleTimer("test2", 11L);
        }
    }
}
