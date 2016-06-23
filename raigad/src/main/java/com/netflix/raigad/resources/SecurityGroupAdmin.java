/**
 * Copyright 2016 Netflix, Inc.
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

import com.google.inject.Inject;
import com.netflix.raigad.identity.IMembership;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

/**
 * This http endpoint allows direct updates (adding/removing) (CIDR) IP addresses and port
 * ranges to the security group for this app.
 */

@Path("/v1/secgroup")
@Produces(MediaType.TEXT_PLAIN)
public class SecurityGroupAdmin
{
    private static final Logger log = LoggerFactory.getLogger(SecurityGroupAdmin.class);
    private static final Integer DEFAULT_MASK = 32;
    private final IMembership membership;

    @Inject
    public SecurityGroupAdmin(IMembership membership)
    {
        this.membership = membership;
    }

    @POST
    public Response addACL(
            @QueryParam("ip") String ipAddress,
            @QueryParam("mask") Integer mask,
            @QueryParam("fromPort") int fromPort,
            @QueryParam("toPort") int toPort)
    {
        if (!InetAddressValidator.getInstance().isValid(ipAddress)) {
            log.error("Invalid IP address", ipAddress);
            return Response.status(Response.Status.BAD_REQUEST).build();
        }

        if (mask == null || mask < 8) {
            log.info("IP mask is too wide or not provided, using /32");
            mask = DEFAULT_MASK;
        }

        try {
            membership.addACL(Collections.singletonList(String.format("%s/%d", ipAddress, mask)), fromPort, toPort);
        }
        catch (Exception e) {
            log.error("Error adding ACL to a security group", e);
            return Response.serverError().build();
        }

        return Response.ok().build();
    }

    @DELETE
    public Response removeACL(
            @QueryParam("ip") String ipAddress,
            @QueryParam("mask") Integer mask,
            @QueryParam("fromPort") int fromPort,
            @QueryParam("toPort") int toPort)
    {
        if (!InetAddressValidator.getInstance().isValid(ipAddress)) {
            log.error("Invalid IP address", ipAddress);
            return Response.status(Response.Status.BAD_REQUEST).build();
        }

        if (mask == null) {
            log.info("IP mask not provided, using /32");
            mask = DEFAULT_MASK;
        }

        try {
            membership.removeACL(Collections.singletonList(String.format("%s/%d", ipAddress, mask)), fromPort, toPort);
        }
        catch (Exception e) {
            log.error("Error removing ACL from a security group", e);
            return Response.serverError().build();
        }

        return Response.ok().build();
    }
}
