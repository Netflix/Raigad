package com.netflix.elasticcar.dataobjects;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by sloke on 7/7/14.
 */
/*
   [
        {
            "id":"8sZZWYmmQaeNUKMq1S1uow",
            "host":"es-slokemsd-useast1d-master-i-9e1b62b4",
            "ip":"10.218.89.139",
            "node":"us-east-1d.i-9e1b62b4"
        }
   ]
 */
public class MasterNodeInformationDO
{
    private final String id;
    private final String host;
    private final String ip;
    private final String node;

    @JsonCreator
    public MasterNodeInformationDO(@JsonProperty("id") final String id,
                                   @JsonProperty("host") final String host,
                                   @JsonProperty("ip") final String ip,
                                   @JsonProperty("node") final String node)
    {
        this.id = id;
        this.host = host;
        this.ip = ip;
        this.node = node;
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public String getIp() {
        return ip;
    }

    public String getNode() {
        return node;
    }
}
