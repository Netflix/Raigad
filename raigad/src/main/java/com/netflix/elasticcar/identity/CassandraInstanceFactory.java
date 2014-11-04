package com.netflix.elasticcar.identity;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.configuration.IConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Factory to use cassandra for managing instance data
 */
@Singleton
public class CassandraInstanceFactory implements IElasticCarInstanceFactory {
    private static final Logger logger = LoggerFactory
            .getLogger(CassandraInstanceFactory.class);

    @Inject
    IConfiguration config;

    @Inject
    InstanceDataDAOCassandra dao;

    @Override
    public ElasticCarInstance create(String app, String id, String instanceID,
                                     String hostname, String ip, String zone, String dc, String asgName,
                                     Map<String, Object> volumes) {

        try {
            logger.info("App = (" + app + ") " +
                            "id = (" + id + ") " +
                            "instanceID = ("	+ instanceID + ") " +
                            "hostname = (" + hostname + ") " +
                            "ip = ("	+ ip + ") " +
                            "zone = (" + zone + ") " +
                            "dc = (" + dc + ")"
            );
            ElasticCarInstance escarInstance = new ElasticCarInstance();
            escarInstance.setAvailabilityZone(zone);
            escarInstance.setHostIP(ip);
            escarInstance.setHostName(hostname);
            escarInstance.setId(id);
            escarInstance.setInstanceId(instanceID);
            escarInstance.setDC(dc);
            escarInstance.setApp(app);
            escarInstance.setAsg(asgName);

            dao.createInstanceEntry(escarInstance);
            return escarInstance;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<ElasticCarInstance> getAllIds(String appName) {
        List<ElasticCarInstance> return_ = new ArrayList<ElasticCarInstance>();
        for (ElasticCarInstance instance : dao.getAllInstances(appName)) {
            if (config.isDebugEnabled()) {
                logger.debug("Instance Details = " + instance.getInstanceId());
            }
            return_.add(instance);
        }

        return return_;
    }

    @Override
    public ElasticCarInstance getInstance(String appName, String dc, String id) {
        return dao.getInstance(appName, dc, id);
    }

    @Override
    public void sort(List<ElasticCarInstance> list) {
        Collections.sort(list, new Comparator<ElasticCarInstance>() {

            @Override
            public int compare(ElasticCarInstance esInstance1,
                               ElasticCarInstance esInstance2) {
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
    public void delete(ElasticCarInstance instance) {
        try {
            dao.deleteInstanceEntry(instance);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Unable to deregister ElasticCarInstance",e);
        }
    }

    @Override
    public void update(ElasticCarInstance arg0) {
        // TODO Auto-generated method stub
    }

    @Override
    public void attachVolumes(ElasticCarInstance arg0, String arg1, String arg2) {
        // TODO Auto-generated method stub

    }

}
