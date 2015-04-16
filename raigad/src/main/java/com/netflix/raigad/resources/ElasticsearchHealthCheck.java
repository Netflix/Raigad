package com.netflix.raigad.resources;

/**
 * Created by alfasi on 4/9/15.
 */
import com.netflix.raigad.utils.ElasticsearchProcessMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/healthcheck")
@Produces(MediaType.APPLICATION_JSON)
public class ElasticsearchHealthCheck {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(ElasticsearchHealthCheck.class);
    private static final String REST_SUCCESS = "[\"ok\"]";

    @GET
    @Path("/")
    public Response service() {
        boolean started = ElasticsearchProcessMonitor.isElasticsearchStarted();
        log.info("Checking if ES is UP returned: " + started);
        if (!started){
            return Response.serverError().build();
        }
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

}

