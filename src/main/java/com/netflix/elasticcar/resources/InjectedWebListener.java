/**
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.elasticcar.resources;

public class InjectedWebListener //extends GuiceServletContextListener
{
//    protected static final Logger logger = LoggerFactory.getLogger(InjectedWebListener.class);
//
//    @Override
//    protected Injector getInjector()
//    {
//        List<Module> moduleList = Lists.newArrayList();
//        moduleList.add(new JaxServletModule());
//        moduleList.add(new ElasticCarGuiceModule());
//        Injector injector;
//        try
//        {
//        		injector  = Guice.createInjector(moduleList);
//        		startJobs(injector);
//        }
//        catch (Exception e)
//        {
//            logger.error(e.getMessage(),e);
//            throw new RuntimeException(e.getMessage(), e);
//        }
//        return injector;
//    }
//
//    private void startJobs(Injector injector) throws Exception
//    {
//    		ElasticCarServer escarServer = injector.getInstance(ElasticCarServer.class);
//
//    		logger.info("**Now starting to initialize escarserver from OSS");
//    		escarServer.initialize();
//
//    }
//
//    public static class JaxServletModule extends ServletModule
//    {
//        @Override
//        protected void configureServlets()
//        {
//            Map<String, String> params = new HashMap<String, String>();
//            params.put(PackagesResourceConfig.PROPERTY_PACKAGES
//                    , "unbound");
//            params.put("com.sun.jersey.config.property.packages", "com.netflix.escar.resources");
//            params.put(ServletContainer.PROPERTY_FILTER_CONTEXT_PATH, "/REST");
////            serve("/REST/*").with(GuiceContainer.class, params);
//        }
//    }
//
//
//    public static class ElasticCarGuiceModule extends AbstractModule
//    {
//        @Override
//        protected void configure()
//        {
//    		logger.info("**Binding OSS Config classes.");
//            bind(IConfiguration.class).to(ElasticCarConfiguration.class);
//            bind(SchedulerFactory.class).to(StdSchedulerFactory.class).asEagerSingleton();
////            bind(IElasticCarInstanceFactory.class).to(CassandraInstanceFactory.class);
////            bind(ICredential.class).to(IAMCredential.class);
//        }
//    }
}
