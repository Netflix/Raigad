package com.netflix.elasticcar.resources;

import com.google.inject.Inject;
import com.netflix.elasticcar.IElasticsearchProcess;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.utils.SystemUtils;
import org.codehaus.jettison.json.JSONException;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

@Path("/v1/esadmin")
@Produces(MediaType.APPLICATION_JSON)
public class ElasticsearchAdmin 
{
    private static final String REST_SUCCESS = "[\"ok\"]";
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchAdmin.class);
    private final IConfiguration config;
    private final IElasticsearchProcess esProcess;
    private static final String SHARD_REALLOCATION_PROPERTY = "cluster.routing.allocation.enable";

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

    @GET
    @Path("/shard_allocation_enable/{type}")
    public Response esShardAllocationEnable(@PathParam("type") String type) throws IOException, InterruptedException, JSONException
    {
        logger.info("Enabling Shard Allocation through REST call ...");
        if(!type.equalsIgnoreCase("transient") && !type.equalsIgnoreCase("persistent"))
           throw new IOException("Parameter must be equal to transient or persistent");
        //URL
        String url = "http://127.0.0.1:"+config.getHttpPort()+"/_cluster/settings";
        JSONObject settings = new JSONObject();
        JSONObject property = new JSONObject();
        property.put(SHARD_REALLOCATION_PROPERTY,"all");
        settings.put(type,property);
        String RESPONSE = SystemUtils.runHttpPutCommand(url,settings);
        return Response.ok(RESPONSE, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/shard_allocation_disable/{type}")
    public Response esShardAllocationDisable(@PathParam("type") String type) throws IOException, InterruptedException, JSONException
    {
        logger.info("Disabling Shard Allocation through REST call ...");
        if(!type.equalsIgnoreCase("transient") && !type.equalsIgnoreCase("persistent"))
            throw new IOException("Parameter must be equal to transient or persistent");
        //URL
        String url = "http://127.0.0.1:"+config.getHttpPort()+"/_cluster/settings";
        JSONObject settings = new JSONObject();
        JSONObject property = new JSONObject();
        property.put(SHARD_REALLOCATION_PROPERTY,"none");
        settings.put(type,property);
        String RESPONSE = SystemUtils.runHttpPutCommand(url,settings);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

}
