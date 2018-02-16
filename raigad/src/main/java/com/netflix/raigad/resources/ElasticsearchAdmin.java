/**
 * Copyright 2017 Netflix, Inc.
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
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.defaultimpl.IElasticsearchProcess;
import com.netflix.raigad.indexmanagement.ElasticsearchIndexManager;
import com.netflix.raigad.utils.SystemUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
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
public class ElasticsearchAdmin {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchAdmin.class);

    private static final String REST_SUCCESS = "[\"ok\"]";
    private static final String SHARD_REALLOCATION_PROPERTY = "cluster.routing.allocation.enable";

    private final IConfiguration config;
    private final IElasticsearchProcess esProcess;
    private final ElasticsearchIndexManager esIndexManager;

    @Inject
    public ElasticsearchAdmin(IConfiguration config, IElasticsearchProcess esProcess, ElasticsearchIndexManager esIndexManager) {
        this.config = config;
        this.esProcess = esProcess;
        this.esIndexManager = esIndexManager;
    }

    @GET
    @Path("/start")
    public Response esStart() throws IOException {
        logger.info("Starting Elasticsearch now through a REST call...");

        esProcess.start();

        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/stop")
    public Response esStop() throws IOException {
        logger.info("Stopping Elasticsearch now through a REST call...");

        esProcess.stop();

        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/run_indexmanager")
    public Response manageIndex() throws Exception {
        logger.info("Running index manager through a REST call...");

        esIndexManager.runIndexManagement();

        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }


    @GET
    @Path("/existingRepositories")
    public Response esExistingRepositories() throws Exception {
        logger.info("Retrieving existing repositories through a REST call...");

        String URL = "http://127.0.0.1:" + config.getHttpPort() + "/_snapshot/";
        String RESPONSE = SystemUtils.runHttpGetCommand(URL);
        JSONObject jsonObject = (JSONObject) new JSONParser().parse(RESPONSE);

        return Response.ok(jsonObject, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/shard_allocation_enable/{type}")
    public Response esShardAllocationEnable(@PathParam("type") String type) throws IOException {
        logger.info("Enabling shard allocation through a REST call...");

        if (!type.equalsIgnoreCase("transient") && !type.equalsIgnoreCase("persistent")) {
            throw new IOException("Parameter must be equal to transient or persistent");
        }

        String url = "http://127.0.0.1:" + config.getHttpPort() + "/_cluster/settings";
        JSONObject settings = new JSONObject();
        JSONObject property = new JSONObject();
        property.put(SHARD_REALLOCATION_PROPERTY, "all");
        settings.put(type, property);

        String response = SystemUtils.runHttpPutCommand(url, settings.toJSONString());

        return Response.ok(response, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/shard_allocation_disable/{type}")
    public Response esShardAllocationDisable(@PathParam("type") String type) throws IOException {
        logger.info("Disabling shard allocation through a REST call...");

        if (!type.equalsIgnoreCase("transient") && !type.equalsIgnoreCase("persistent")) {
            throw new IOException("Parameter must be equal to transient or persistent");
        }

        String url = "http://127.0.0.1:" + config.getHttpPort() + "/_cluster/settings";
        JSONObject settings = new JSONObject();
        JSONObject property = new JSONObject();
        property.put(SHARD_REALLOCATION_PROPERTY, "none");
        settings.put(type, property);

        SystemUtils.runHttpPutCommand(url, settings.toJSONString());

        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

}
