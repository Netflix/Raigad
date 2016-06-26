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

package com.netflix.raigad.identity;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.configuration.IConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Factory to use Cassandra for managing instance data
 */
@Singleton
public class CassandraInstanceFactory implements IRaigadInstanceFactory {
    private static final Logger logger = LoggerFactory.getLogger(CassandraInstanceFactory.class);

    @Inject
    IConfiguration config;

    @Inject
    InstanceDataDAOCassandra dao;

    @Override
    public RaigadInstance create(String app, String id, String instanceID,
                                 String hostname, String ip, String zone, String dc, String asgName,
                                 Map<String, Object> volumes) {

        try {
            logger.info("App = (" + app + ") " +
                    "id = (" + id + ") " +
                    "instanceID = (" + instanceID + ") " +
                    "hostname = (" + hostname + ") " +
                    "IP = (" + ip + ") " +
                    "zone = (" + zone + ") " +
                    "dc = (" + dc + ")");

            RaigadInstance raigadInstance = new RaigadInstance();
            raigadInstance.setAvailabilityZone(zone);
            raigadInstance.setHostIP(ip);
            raigadInstance.setHostName(hostname);
            raigadInstance.setId(id);
            raigadInstance.setInstanceId(instanceID);
            raigadInstance.setDC(dc);
            raigadInstance.setApp(app);
            raigadInstance.setAsg(asgName);

            dao.createInstanceEntry(raigadInstance);
            return raigadInstance;
        }
        catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<RaigadInstance> getAllIds(String appName) {
        List<RaigadInstance> raigadInstances = new ArrayList<>(dao.getAllInstances(appName));

        if (config.isDebugEnabled()) {
            for (RaigadInstance instance : dao.getAllInstances(appName)) {
                logger.debug("Instance details: " + instance.getInstanceId());
            }
        }

        return raigadInstances;
    }

    @Override
    public RaigadInstance getInstance(String appName, String dc, String id) {
        return dao.getInstance(appName, dc, id);
    }

    @Override
    public void sort(List<RaigadInstance> list) {
        Collections.sort(list, new Comparator<RaigadInstance>() {

            @Override
            public int compare(RaigadInstance esInstance1,
                               RaigadInstance esInstance2) {
                int azCompare = esInstance1.getAvailabilityZone().compareTo(
                        esInstance2.getAvailabilityZone());
                if (azCompare == 0) {
                    return esInstance1.getId().compareTo(esInstance2.getId());
                } else {
                    return azCompare;
                }
            }
        });
    }

    @Override
    public void delete(RaigadInstance instance) {
        try {
            dao.deleteInstanceEntry(instance);
        }
        catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Unable to deregister Raigad instance", e);
        }
    }

    @Override
    public void update(RaigadInstance arg0) {
        // TODO Auto-generated method stub
    }

    @Override
    public void attachVolumes(RaigadInstance arg0, String arg1, String arg2) {
        // TODO Auto-generated method stub
    }
}