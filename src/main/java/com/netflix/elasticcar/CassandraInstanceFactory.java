package com.netflix.elasticcar;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.elasticcar.identity.ElasticCarInstance;
import com.netflix.elasticcar.identity.IElasticCarInstanceFactory;

/**
 * Factory to use cassandra for managing instance data 
 */
@Singleton
public class CassandraInstanceFactory implements IElasticCarInstanceFactory
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraInstanceFactory.class);

    @Inject
    IConfiguration config;

    @Inject
    InstanceDataDAOCassandra dao;

    public List<ElasticCarInstance> getAllIds(String appName)
    {
        List<ElasticCarInstance> return_ = new ArrayList<ElasticCarInstance>();
        for (ElasticCarInstance instance : dao.getAllInstances(appName)) {
            return_.add(instance);
        }

        sort(return_);
        return return_;
    }

    public void sort(List<ElasticCarInstance> return_)
    {
        Comparator<? super ElasticCarInstance> comparator = new Comparator<ElasticCarInstance>()
        {

            @Override
            public int compare(ElasticCarInstance o1, ElasticCarInstance o2)
            {
                Integer c1 = o1.getId();
                Integer c2 = o2.getId();
                return c1.compareTo(c2);
            }
        };
        Collections.sort(return_, comparator);
    }

    public ElasticCarInstance create(String app, int id, String instanceID, String hostname, String ip, String zone, Map<String, Object> volumes, String payload)
    {
        try {
            Map<String, Object> v = (volumes == null) ? new HashMap<String, Object>() : volumes;
            ElasticCarInstance ins = new ElasticCarInstance();
            ins.setApp(app);
            ins.setRac(zone);
            ins.setHost(hostname);
            ins.setHostIP(ip);
            ins.setId(id);
            ins.setInstanceId(instanceID);
            ins.setDC(config.getDC());
            ins.setToken(payload);
            ins.setVolumes(v);

            // remove old data node which are dead.
            if (app.endsWith("-dead")) {
                   ElasticCarInstance oldData = dao.getInstance(app,ins.getDC(), id);
                   // clean up a very old data...
                   if (null != oldData)
                        dao.deleteInstanceEntry(oldData);
            }
            dao.createInstanceEntry(ins);
            return ins;
        }
        catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void delete(ElasticCarInstance inst)
    {
        try {
            dao.deleteInstanceEntry(inst);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void update(ElasticCarInstance inst)
    {
        try {
            dao.createInstanceEntry(inst);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void attachVolumes(ElasticCarInstance instance, String mountPath, String device)
    {
        throw new UnsupportedOperationException("Volumes not supported");
    }

    @Override
    public ElasticCarInstance getInstance(String appName, String dc, int id)
    {
        return dao.getInstance(appName, dc, id);
    }
}
