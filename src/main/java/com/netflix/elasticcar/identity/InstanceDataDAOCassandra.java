package com.netflix.elasticcar.identity;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.*;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.TimeUUIDUtils;
import com.netflix.elasticcar.configuration.IConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Use bootstrap cluster to find tokens and nodes in the ring
 *
 * @author Sagar Loke
 *
 */
@Singleton
public class InstanceDataDAOCassandra
{
    private static final Logger logger = LoggerFactory.getLogger(InstanceDataDAOCassandra.class);
    private static final String CN_CLUSTER = "cluster";
    private static final String CN_AZ = "availabilityZone";
    private static final String CN_INSTANCEID = "instanceId";
    private static final String CN_HOSTNAME = "hostname";
    private static final String CN_IP = "ip";
    private static final String CN_LOCATION = "location";
    private static final String CN_ASGNAME = "asgname";
    private static final String CN_UPDATETIME = "updatetime";
    public static final String CF_NAME_INSTANCES = "instances";
    public static final String CF_NAME_LOCKS = "locks";

    private final Keyspace bootKeyspace;
    private final IConfiguration config;
    private final String KS_NAME;

    public static final ColumnFamily<String, String> CF_INSTANCES =
            new ColumnFamily<String, String>(CF_NAME_INSTANCES, StringSerializer.get(), StringSerializer.get());
    public static final ColumnFamily<String, String> CF_LOCKS =
            new ColumnFamily<String, String>(CF_NAME_LOCKS, StringSerializer.get(), StringSerializer.get());

    @Inject
    public InstanceDataDAOCassandra(IConfiguration config) throws ConnectionException
    {
//        astyanaxManager.registerKeyspace(configuration.getBootClusterName(), KS_NAME);
//        bootKeyspace = astyanaxManager.getRegisteredKeyspace(configuration.getBootClusterName(), KS_NAME);
        KS_NAME = config.getCassandraKeyspaceName();
        bootKeyspace = initWithThriftDriver(config).getClient();
        this.config = config;
    }

    public void createInstanceEntry(ElasticCarInstance instance) throws Exception
    {
        logger.info("***Creating New Instance Entry");
        String key = getRowKey(instance);
        // If the key exists throw exception
        if (getInstance(instance.getApp(), instance.getDC(),
                instance.getId()) != null) {
            logger.info(String.format("Key already exists: %s", key));
            return;
        }
        // Grab the lock
        getLock(instance);
        MutationBatch m = bootKeyspace.prepareMutationBatch();
        ColumnListMutation<String> clm = m.withRow(CF_INSTANCES, key);
        clm.putColumn(CN_CLUSTER, instance.getApp(), null);
        clm.putColumn(CN_AZ, instance.getAvailabilityZone(), null);
        clm.putColumn(CN_INSTANCEID, instance.getInstanceId(), null);
        clm.putColumn(CN_HOSTNAME, instance.getHostName(), null);
        clm.putColumn(CN_IP, instance.getHostIP(), null);
        clm.putColumn(CN_LOCATION, instance.getDC(), null);
        clm.putColumn(CN_ASGNAME, instance.getAsg(), null);
        clm.putColumn(CN_UPDATETIME, TimeUUIDUtils.getUniqueTimeUUIDinMicros(),
                null);
        m.execute();
    }

    public ElasticCarInstance getInstance(String cluster, String region, String instanceId)
    {
        List<ElasticCarInstance> list = getAllInstances(cluster);
        for (ElasticCarInstance ins : list) {
            if (ins.getInstanceId().equals(instanceId) && ins.getDC().equals(region))
                return ins;
        }
        return null;
    }

    public List<ElasticCarInstance> getAllInstances(String cluster)
    {
        List<ElasticCarInstance> list = new ArrayList<ElasticCarInstance>();
        try {

            String selectClause = "";
            if(config.isMultiDC())
            {
                selectClause  = String.format("SELECT * FROM %s WHERE %s = '%s' ", CF_NAME_INSTANCES, CN_CLUSTER, cluster);
            }else {
                selectClause  = String.format("SELECT * FROM %s WHERE %s = '%s' AND %s = '%s' ", CF_NAME_INSTANCES, CN_CLUSTER, cluster, CN_LOCATION, config.getDC());
            }

            if (config.isDebugEnabled()) {
                logger.debug(selectClause);
            }

            final ColumnFamily<String, String> CF_INSTANCES_NEW = ColumnFamily.newColumnFamily(KS_NAME,
                    StringSerializer.get(), StringSerializer.get());

            OperationResult<CqlResult<String, String>> result = bootKeyspace.prepareQuery(CF_INSTANCES_NEW)
                    .withCql(selectClause).execute();

            for (Row<String, String> row : result.getResult().getRows()) {
                list.add(transform(row.getColumns()));
            }
        }
        catch (Exception e) {
            logger.warn("Caught an Unknown Exception during reading msgs ... -> " + e.getMessage());
            throw new RuntimeException(e);
        }

        if (config.isDebugEnabled()) {
            for (ElasticCarInstance ei : list) {
                logger.debug("-----------");
                logger.debug(ei.toString());
            }
        }
        return list;
    }

    public void deleteInstanceEntry(ElasticCarInstance instance) throws Exception
    {
        logger.info("***Deleting Dead Instance Entry");
        // Acquire the lock first
        getLock(instance);

        // Delete the row
        String key = findKey(instance.getApp(), instance.getInstanceId(), instance.getDC());
        if (key == null)
            return;  //don't fail it

        MutationBatch m = bootKeyspace.prepareMutationBatch();
        m.withRow(CF_INSTANCES, key).delete();
        m.execute();

        key = getLockingKey(instance);
        // Delete key
        m = bootKeyspace.prepareMutationBatch();
        m.withRow(CF_LOCKS, key).delete();
        m.execute();

        // Have to delete choosing key as well to avoid issues with delete
        // followed by immediate writes
        key = getChoosingKey(instance);
        m = bootKeyspace.prepareMutationBatch();
        m.withRow(CF_LOCKS, key).delete();
        m.execute();
    }

    protected void sort(List<ElasticCarInstance> list) {
        Collections.sort(list, new Comparator<ElasticCarInstance>() {

            @Override
            public int compare(ElasticCarInstance esInstance1, ElasticCarInstance esInstance2) {
                int azCompare = esInstance1.getAvailabilityZone().compareTo(esInstance2.getAvailabilityZone());
                if (azCompare == 0) {
                    return esInstance1.getId().compareTo(esInstance2.getId());
                } else {
                    return azCompare;
                }
            }
        });
    }

    /*
     * To get a lock on the row - Create a choosing row and make sure there are
     * no contenders. If there are bail out. Also delete the column when bailing
     * out. - Once there are no contenders, grab the lock if it is not already
     * taken.
     */
    private void getLock(ElasticCarInstance instance) throws Exception
    {
        String choosingkey = getChoosingKey(instance);
        MutationBatch m = bootKeyspace.prepareMutationBatch();
        ColumnListMutation<String> clm = m.withRow(CF_LOCKS, choosingkey);

        // Expire in 6 sec
        clm.putColumn(instance.getInstanceId(), instance.getInstanceId(), new Integer(6));
        m.execute();
        int count = bootKeyspace.prepareQuery(CF_LOCKS).getKey(choosingkey).getCount().execute().getResult();
        if (count > 1) {
            // Need to delete my entry
            m.withRow(CF_LOCKS, choosingkey).deleteColumn(instance.getInstanceId());
            m.execute();
            throw new Exception(String.format("More than 1 contender for lock %s %d", choosingkey, count));
        }

        String lockKey = getLockingKey(instance);
        OperationResult<ColumnList<String>> result = bootKeyspace.prepareQuery(CF_LOCKS).getKey(lockKey).execute();
        if (result.getResult().size() > 0 && !result.getResult().getColumnByIndex(0).getName().equals(instance.getInstanceId()))
            throw new Exception(String.format("Lock already taken %s", lockKey));

        clm = m.withRow(CF_LOCKS, lockKey);
        clm.putColumn(instance.getInstanceId(), instance.getInstanceId(), new Integer(600));
        m.execute();
        Thread.sleep(100);
        result = bootKeyspace.prepareQuery(CF_LOCKS).getKey(lockKey).execute();
        if (result.getResult().size() == 1 && result.getResult().getColumnByIndex(0).getName().equals(instance.getInstanceId())) {
            logger.info("Got lock " + lockKey);
            return;
        }
        else
            throw new Exception(String.format("Cannot insert lock %s", lockKey));

    }

    public String findKey(String cluster, String instanceId, String dc)
    {
        try {
            final String selectClause = String.format(
                    "SELECT * FROM %s WHERE %s = '%s' and %s = '%s' and %s = '%s'  ", CF_NAME_INSTANCES,
                    CN_CLUSTER, cluster, CN_INSTANCEID, instanceId, CN_LOCATION, dc);
            logger.info(selectClause);


            final ColumnFamily<String, String> CF_INSTANCES_NEW = ColumnFamily.newColumnFamily(KS_NAME,
                    StringSerializer.get(), StringSerializer.get());

            OperationResult<CqlResult<String, String>> result = bootKeyspace.prepareQuery(CF_INSTANCES_NEW)
                    .withCql(selectClause).execute();

            if (result == null || result.getResult().getRows().size() == 0)
                return null;

            Row<String, String> row = result.getResult().getRows().getRowByIndex(0);
            return row.getKey();

        }
        catch (Exception e) {
            logger.warn("Caught an Unknown Exception during find a row matching cluster[" + cluster +
                    "], id[" + instanceId + "], and region[" + dc + "]  ... -> "
                    + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private ElasticCarInstance transform(ColumnList<String> columns)
    {
        ElasticCarInstance ins = new ElasticCarInstance();
        Map<String, String> cmap = new HashMap<String, String>();
        for (Column<String> column : columns) {
            cmap.put(column.getName(), column.getStringValue());
            if (column.getName().equals(CN_CLUSTER))
                ins.setUpdatetime(column.getTimestamp());
        }
        ins.setId(cmap.get(CN_LOCATION) +"."+ cmap.get(CN_INSTANCEID));
        ins.setApp(cmap.get(CN_CLUSTER));
        ins.setAvailabilityZone(cmap.get(CN_AZ));
        ins.setHostName(cmap.get(CN_HOSTNAME));
        ins.setHostIP(cmap.get(CN_IP));
        ins.setInstanceId(cmap.get(CN_INSTANCEID));
        ins.setAsg(cmap.get(CN_ASGNAME));
        ins.setDC(cmap.get(CN_LOCATION));
        return ins;
    }

    private String getChoosingKey(ElasticCarInstance instance)
    {
        return instance.getApp() + "_" + instance.getDC() + "_" + instance.getInstanceId() + "-choosing";
    }

    private String getLockingKey(ElasticCarInstance instance)
    {
        return instance.getApp() + "_" + instance.getDC() + "_" + instance.getInstanceId() + "-lock";
    }

    private String getRowKey(ElasticCarInstance instance)
    {
        return instance.getApp() + "_" + instance.getDC() + "_" + instance.getInstanceId();
    }

    private AstyanaxContext<Keyspace> initWithThriftDriver(
            IConfiguration config) {

        return new AstyanaxContext.Builder()
                .forCluster(config.getBootClusterName())
                .forKeyspace(KS_NAME)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(
                                        NodeDiscoveryType.RING_DESCRIBE)
                                .setConnectionPoolType(
                                        ConnectionPoolType.ROUND_ROBIN)
                                .setDiscoveryDelayInSeconds(60000))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(
                                "MyConnectionPool")
                                .setMaxConnsPerHost(5)
                                .setPort(config.getCassandraThriftPortForAstyanax()))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

    }

}

