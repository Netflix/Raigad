/**
 * Copyright 2014 Netflix, Inc.
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

import com.google.inject.Inject;
import com.netflix.elasticcar.identity.ElasticCarInstance;
import com.netflix.elasticcar.startup.ElasticCarServer;
import com.netflix.elasticcar.utils.EsUtils;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * This servlet will provide the configuration API service as and when Elasticsearch
 * requests for it.
 */
@Path("/v1/esconfig")
@Produces(MediaType.TEXT_PLAIN)
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
				return Response.ok(esCarJson.toString())
						.build();
			}
		} catch (Exception e) {
			logger.error("Error while executing get_nodes", e);
			return Response.serverError().build();
		}
		return Response.status(500).build();
	}

}
