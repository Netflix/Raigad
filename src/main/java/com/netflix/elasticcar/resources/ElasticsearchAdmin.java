package com.netflix.elasticcar.resources;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.netflix.elasticcar.IConfiguration;
import com.netflix.elasticcar.IElasticsearchProcess;

@Path("/v1/esadmin")
@Produces(MediaType.APPLICATION_JSON)
public class ElasticsearchAdmin 
{
    private static final String REST_SUCCESS = "[\"ok\"]";
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchAdmin.class);
    private IConfiguration config;
    private final IElasticsearchProcess esProcess;

    @Inject
    public ElasticsearchAdmin(IConfiguration config, IElasticsearchProcess esProcess)
    {
        this.config = config;
        this.esProcess = esProcess;
    }

    @GET
    @Path("/start")
    public Response esStart() throws IOException, InterruptedException, JSONException
    {
    		logger.info("Starting Elastic Search now through REST call ...");
        esProcess.start(true);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/stop")
    public Response esStop() throws IOException, InterruptedException, JSONException
    {
		logger.info("Stopping Elastic Search now through REST call ...");
        esProcess.stop();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }


}
