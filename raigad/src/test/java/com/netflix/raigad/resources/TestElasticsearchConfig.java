package com.netflix.raigad.resources;

import com.netflix.raigad.configuration.CustomConfigSource;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.identity.InstanceManager;
import com.netflix.raigad.identity.RaigadInstance;
import com.netflix.raigad.startup.RaigadServer;
import com.netflix.raigad.utils.TribeUtils;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class TestElasticsearchConfig {
    private TribeUtils tribeUtils;
    private IConfiguration config;

    @Before
    public void setUp() {

        tribeUtils = mock(TribeUtils.class);
        config = mock(IConfiguration.class);
    }

    @Test
    public void getNodes() {
        RaigadInstance raigadInstance1 = new RaigadInstance();
        raigadInstance1.setApp("fake-app1");

        RaigadInstance raigadInstance2 = new RaigadInstance();
        raigadInstance2.setApp("fake-app2");

        RaigadInstance raigadInstance3 = new RaigadInstance();
        raigadInstance3.setApp("fake-app3");

        final List<RaigadInstance> nodes = asList(raigadInstance1, raigadInstance2, raigadInstance3);

        InstanceManager instanceManager = mock(InstanceManager.class);
        when(instanceManager.getAllInstances()).thenReturn(nodes);

        RaigadServer raigadServer = mock(RaigadServer.class);
        when(raigadServer.getInstanceManager()).thenReturn(instanceManager);

        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig(raigadServer, tribeUtils, new CustomConfigSource(), config);

        Response response = elasticsearchConfig.getNodes();
        assertEquals(200, response.getStatus());

        verify(raigadServer, times(1)).getInstanceManager();
        verify(instanceManager, times(1)).getAllInstances();
    }

    @Test
    public void getNodes_notFound() {
        InstanceManager instanceManager = mock(InstanceManager.class);
        when(instanceManager.getAllInstances()).thenReturn(Collections.emptyList());

        RaigadServer raigadServer = mock(RaigadServer.class);
        when(raigadServer.getInstanceManager()).thenReturn(instanceManager);

        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig(raigadServer, tribeUtils, new CustomConfigSource(), config);

        Response response = elasticsearchConfig.getNodes();
        assertEquals(200, response.getStatus());

        verify(raigadServer, times(1)).getInstanceManager();
        verify(instanceManager, times(1)).getAllInstances();
    }

    @Test
    public void getNodes_Error() {
        InstanceManager instanceManager = mock(InstanceManager.class);
        when(instanceManager.getAllInstances()).thenReturn(null);

        RaigadServer raigadServer = mock(RaigadServer.class);
        when(raigadServer.getInstanceManager()).thenReturn(instanceManager);

        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig(raigadServer, tribeUtils, new CustomConfigSource(), config);

        Response response = elasticsearchConfig.getNodes();
        assertEquals(500, response.getStatus());

        verify(raigadServer, times(1)).getInstanceManager();
        verify(instanceManager, times(1)).getAllInstances();
    }

    @Test
    public void getNodes_handlesUnknownHostException() {
        InstanceManager instanceManager = mock(InstanceManager.class);
        when(instanceManager.getAllInstances()).thenThrow(new RuntimeException());

        RaigadServer raigadServer = mock(RaigadServer.class);
        when(raigadServer.getInstanceManager()).thenReturn(instanceManager);

        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig(raigadServer, tribeUtils, new CustomConfigSource(), config);

        Response response = elasticsearchConfig.getNodes();
        assertEquals(500, response.getStatus());

        verify(raigadServer, times(1)).getInstanceManager();
        verify(instanceManager, times(1)).getAllInstances();
    }
}
