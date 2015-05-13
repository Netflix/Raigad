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
package com.netflix.raigad.resources;

import com.netflix.raigad.utils.ElasticsearchProcessMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by alfasi on 4/23/15.
 */
@Path("/v1/healthcheck")
@Produces(MediaType.APPLICATION_JSON)
public class NodeHealthCheck {

    private static final Logger logger = LoggerFactory.getLogger(NodeHealthCheck.class);
    private static final String REST_SUCCESS = "[\"ok\"]";

    @GET
    @Path("/isesprocessrunning")
    public Response checkHealth()
    {
        logger.info("Got REST call to check Node-health...");
        if (!ElasticsearchProcessMonitor.isElasticsearchRunning()) {
            return Response.serverError().status(500).build();
        }
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

}
