package com.netflix.raigad.configuration;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.netflix.raigad.backup.AbstractRepository;
import com.netflix.raigad.backup.S3Repository;
import org.junit.Ignore;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

@Ignore
public class UnitTestModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(IConfiguration.class).toInstance(new FakeConfiguration(FakeConfiguration.FAKE_REGION, "fake-app", "az1", "fakeInstance1"));
        bind(SchedulerFactory.class).to(StdSchedulerFactory.class).in(Scopes.SINGLETON);
        bind(AbstractRepository.class).annotatedWith(Names.named("s3")).to(S3Repository.class);
    }
}
