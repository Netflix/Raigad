package com.netflix.elasticcar.resources;

import java.util.Collections;

import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.netflix.elasticcar.identity.IMembership;

/**
 * This http endpoint allows direct updates (adding/removing) (CIDR) IP addresses and port
 * ranges to the security group for this app.
 */
@Path("/v1/secgroup")
@Produces(MediaType.TEXT_PLAIN)
public class SecurityGroupAdmin
{
    private static final Logger log = LoggerFactory.getLogger(SecurityGroupAdmin.class);
    private static final String CIDR_TAG = "/32";
    private final IMembership membership;

    @Inject
    public SecurityGroupAdmin(IMembership membership)
    {
        this.membership = membership;
    }

    @POST
    public Response addACL(@QueryParam("ip") String ipAddr, @QueryParam("fromPort") int fromPort, @QueryParam("toPort") int toPort)
    {
        if(!ipAddr.endsWith(CIDR_TAG))
            ipAddr += CIDR_TAG;
        try
        {
            membership.addACL(Collections.singletonList(ipAddr), fromPort, toPort);
        }
        catch(Exception e)
        {
            log.error("Error while trying to add an ACL to a security group", e);
            return Response.serverError().build();
        }
        return Response.ok().build();
    }

    @DELETE
    public Response removeACL(@QueryParam("ip") String ipAddr, @QueryParam("fromPort") int fromPort, @QueryParam("toPort") int toPort)
    {
        if(!ipAddr.endsWith(CIDR_TAG))
            ipAddr += CIDR_TAG;
        try
        {
            membership.removeACL(Collections.singletonList(ipAddr), fromPort, toPort);
        }
        catch(Exception e)
        {
            log.error("Error while trying to remove an ACL to a security group", e);
            return Response.serverError().build();
        }
        return Response.ok().build();
    }
}

