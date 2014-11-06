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
package com.netflix.elasticcar.dataobjects;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

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
