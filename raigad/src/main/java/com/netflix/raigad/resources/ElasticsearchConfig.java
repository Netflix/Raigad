/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.raigad.resources;

import com.google.inject.Inject;
import com.netflix.raigad.identity.RaigadInstance;
import com.netflix.raigad.startup.RaigadServer;
import com.netflix.raigad.utils.EsUtils;
import com.netflix.raigad.utils.TribeUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * This servlet will provide the configuration API service as and when Elasticsearch requests for it.
 */
@Path("/v1/esconfig")
@Produces(MediaType.TEXT_PLAIN)
public class ElasticsearchConfig {
	private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConfig.class);

	private final RaigadServer raigadServer;
	private final TribeUtils tribeUtils;

	@Inject
	public ElasticsearchConfig(RaigadServer raigadServer, TribeUtils tribeUtils) {
		this.raigadServer = raigadServer;
		this.tribeUtils = tribeUtils;
	}

	@GET
	@Path("/get_nodes")
	public Response getNodes() {
		try {
			logger.info("getNodes - fetching nodes");
			final List<RaigadInstance> instances = raigadServer.getInstanceManager().getAllInstances();

			if (!CollectionUtils.isEmpty(instances)) {
				logger.info("getNodes - got {} instances", instances.size());
				JSONObject raigadJson = EsUtils.transformRaigadInstanceToJson(instances);
				return Response.ok(raigadJson.toString()).build();
			}
		}
		catch (Exception e) {
			logger.error("Error getting nodes (getNodes)", e);
			return Response.serverError().build();
		}

		return Response.status(500).build();
	}

	@GET
	@Path("/get_tribe_nodes/{id}")
	public Response getTribeNodes(@PathParam("id") String id) {
		try {
			logger.info("getTribeNodes - fetching nodes for tribe ID = {}", id);

			// Find source cluster name from the tribe ID by reading YAML file
			String tribeSourceClusterName = tribeUtils.getTribeClusterNameFromId(id);

			if (StringUtils.isEmpty(tribeSourceClusterName)) {
				throw new RuntimeException("Tribe source cluster name is null or empty," +
						"check if the field exists in elasticsearch.yml");
			}

			final List<RaigadInstance> instances =
					raigadServer.getInstanceManager().getAllInstancesPerCluster(tribeSourceClusterName);

			if (!CollectionUtils.isEmpty(instances)) {
				logger.info("getTribeNodes - got {} instances", instances.size());
				JSONObject raigadJson = EsUtils.transformRaigadInstanceToJson(instances);
				return Response.ok(raigadJson.toString()).build();
			}
		}
		catch (Exception e) {
			logger.error("Error getting nodes (getTribeNodes)", e);
			return Response.serverError().build();
		}

		return Response.status(500).build();
	}
}