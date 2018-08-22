/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.raigad.resources;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.raigad.aws.IAMCredential;
import com.netflix.raigad.aws.ICredential;
import com.netflix.raigad.backup.AbstractRepository;
import com.netflix.raigad.backup.AbstractRepositorySettingsParams;
import com.netflix.raigad.backup.S3Repository;
import com.netflix.raigad.backup.S3RepositorySettingsParams;
import com.netflix.raigad.configuration.CompositeConfigSource;
import com.netflix.raigad.configuration.CustomConfigSource;
import com.netflix.raigad.configuration.IConfigSource;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.identity.CassandraInstanceFactory;
import com.netflix.raigad.identity.EurekaHostsSupplier;
import com.netflix.raigad.identity.HostSupplier;
import com.netflix.raigad.identity.IRaigadInstanceFactory;
import com.netflix.raigad.scheduler.GuiceJobFactory;
import com.netflix.raigad.startup.RaigadServer;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InjectedWebListener extends GuiceServletContextListener {
    protected static final Logger logger = LoggerFactory.getLogger(InjectedWebListener.class);

    @Override
    protected Injector getInjector() {
        List<Module> moduleList = new ArrayList<>();
        moduleList.add(new JaxServletModule());
        moduleList.add(new RaigadGuiceModule());
        Injector injector;

        try {
            injector = LifecycleInjector.builder().withModules(moduleList).build().createInjector();
            startJobs(injector);

            LifecycleManager manager = injector.getInstance(LifecycleManager.class);
            manager.start();
        }
        catch (Exception e) {
            logger.error(e.getMessage(),e);
            throw new RuntimeException(e.getMessage(), e);
        }

        return injector;
    }

    private void startJobs(Injector injector) throws Exception {
        injector.getInstance(IConfiguration.class).initialize();

        logger.info("** Now starting to initialize Raigad server from OSS");
        injector.getInstance(RaigadServer.class).initialize();
    }

    private static class JaxServletModule extends ServletModule {
        @Override
        protected void configureServlets() {
            Map<String, String> params = new HashMap<String, String>();
            params.put(PackagesResourceConfig.PROPERTY_PACKAGES, "unbound");
            params.put("com.sun.jersey.config.property.packages", "com.netflix.raigad.resources");
            params.put(ServletContainer.PROPERTY_FILTER_CONTEXT_PATH, "/REST");
            serve("/REST/*").with(GuiceContainer.class, params);
        }
    }

    private static class RaigadGuiceModule extends AbstractModule {
        @Override
        protected void configure() {
    		logger.info("** Binding OSS Config classes.");

            // Fix bug in Jersey-Guice integration exposed by child injectors
            binder().bind(GuiceContainer.class).asEagerSingleton();
            binder().bind(GuiceJobFactory.class).asEagerSingleton();
            binder().bind(IRaigadInstanceFactory.class).to(CassandraInstanceFactory.class);

            // TODO: Use config.getCredentialProvider() instead of IAMCredential
            binder().bind(ICredential.class).to(IAMCredential.class);
            binder().bind(AbstractRepository.class).annotatedWith(Names.named("s3")).to(S3Repository.class);
            binder().bind(AbstractRepositorySettingsParams.class).annotatedWith(Names.named("s3")).to(S3RepositorySettingsParams.class);
            bind(SchedulerFactory.class).to(StdSchedulerFactory.class).asEagerSingleton();
            bind(HostSupplier.class).to(EurekaHostsSupplier.class).in(Scopes.SINGLETON);
            binder().bind(IConfigSource.class).annotatedWith(Names.named("custom")).to(CompositeConfigSource.class);
        }
    }
}
