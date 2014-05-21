package com.netflix.elasticcar.resources;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.netflix.elasticcar.ElasticCarServer;
import com.netflix.elasticcar.identity.ElasticCarInstance;
import com.netflix.elasticcar.utils.EsUtils;

/**
 * This servlet will provide the configuration API service as and when Elasticsearch
 * requests for it.
 */
@Path("/v1/esconfig")
@Produces(MediaType.APPLICATION_JSON)
public class ElasticsearchConfig 
{
	private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConfig.class);
	private final ElasticCarServer esCarServer;

	@Inject
	public ElasticsearchConfig(ElasticCarServer esCarServer) {
		this.esCarServer = esCarServer;
	}

	@GET
	@Path("/get_nodes")
	public Response getNodes() 
	{
		try 
		{
			logger.info("Fetching nodes via get_nodes ...");
			final List<ElasticCarInstance> instances = esCarServer
					.getInstanceManager().getAllInstances();
			if (instances != null && !instances.isEmpty()) {
				JSONObject esCarJson = EsUtils
						.transformEsCarInstanceToJson(instances);
				return Response.ok(esCarJson, MediaType.APPLICATION_JSON)
						.build();
			}
		} catch (Exception e) {
			logger.error("Error while executing get_nodes", e);
			return Response.serverError().build();
		}
		return Response.status(500).build();
	}

}
