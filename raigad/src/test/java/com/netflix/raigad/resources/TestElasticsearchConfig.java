package com.netflix.raigad.resources;

import com.google.common.collect.ImmutableList;
import com.netflix.raigad.identity.InstanceManager;
import com.netflix.raigad.identity.RaigadInstance;
import com.netflix.raigad.startup.RaigadServer;
import com.netflix.raigad.utils.TribeUtils;
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
    RaigadServer raigadServer;

    private ElasticsearchConfig resource;
    private TribeUtils tribeUtils;

    @Before
    public void setUp() {
        resource = new ElasticsearchConfig(raigadServer,tribeUtils);
    }

    @Test
    public void getNodes() throws Exception {

        RaigadInstance es1 = new RaigadInstance();es1.setApp("fake-app1");
        RaigadInstance es2 = new RaigadInstance();es2.setApp("fake-app2");
        RaigadInstance es3 = new RaigadInstance();es3.setApp("fake-app3");
        final List<RaigadInstance> nodes = asList(es1, es2, es3);
        new NonStrictExpectations() {
            InstanceManager instanceManager;

            {
                raigadServer.getInstanceManager();
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
                raigadServer.getInstanceManager();
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
                raigadServer.getInstanceManager();
                result = instanceManager;
                instanceManager.getAllInstances();
                result = new UnknownHostException();
            }
        };

        Response response = resource.getNodes();
        assertEquals(500, response.getStatus());
    }
}