package com.netflix.elasticcar.resources;

import com.google.common.collect.ImmutableList;
import com.netflix.elasticcar.startup.ElasticCarServer;
import com.netflix.elasticcar.identity.ElasticCarInstance;
import com.netflix.elasticcar.identity.InstanceManager;
import mockit.Expectations;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.net.UnknownHostException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class TestElasticsearchConfig
{

    private
    @Mocked
    ElasticCarServer elasticCarServer;

    private ElasticsearchConfig resource;

    @Before
    public void setUp() {
        resource = new ElasticsearchConfig(elasticCarServer);
    }

    @Test
    public void getNodes() throws Exception {

        ElasticCarInstance es1 = new ElasticCarInstance();es1.setApp("fake-app1");
        ElasticCarInstance es2 = new ElasticCarInstance();es2.setApp("fake-app2");
        ElasticCarInstance es3 = new ElasticCarInstance();es3.setApp("fake-app3");
        final List<ElasticCarInstance> nodes = asList(es1, es2, es3);
        new NonStrictExpectations() {
            InstanceManager instanceManager;

            {
                elasticCarServer.getInstanceManager();
                result = instanceManager;
                times = 1;
                instanceManager.getAllInstances();
                result = nodes;
                times = 1;
            }
        };

        Response response = resource.getNodes();
        assertEquals(200, response.getStatus());
    }

    @Test
    public void getNodes_notFound() throws Exception {
        final List<String> nodes = ImmutableList.of();
        new NonStrictExpectations() {
            InstanceManager instanceManager;

            {
                elasticCarServer.getInstanceManager();
                result = instanceManager;
                times = 1;
                instanceManager.getAllInstances();
                result = nodes;
                times = 1;
            }
        };

        Response response = resource.getNodes();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void getNodes_handlesUnknownHostException() throws Exception {
        new Expectations() {
            InstanceManager instanceManager;

            {
                elasticCarServer.getInstanceManager();
                result = instanceManager;
                instanceManager.getAllInstances();
                result = new UnknownHostException();
            }
        };

        Response response = resource.getNodes();
        assertEquals(500, response.getStatus());
    }
}