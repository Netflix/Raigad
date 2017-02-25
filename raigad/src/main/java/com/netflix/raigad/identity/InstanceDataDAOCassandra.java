/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.raigad.identity;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.Host;
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
import com.netflix.raigad.configuration.IConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Use bootstrap cluster to find nodes
 */
@Singleton
public class InstanceDataDAOCassandra {
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
    private final EurekaHostsSupplier eurekaHostsSupplier;
    private final String BOOT_CLUSTER;
    private final String KS_NAME;
    private final int thriftPortForAstyanax;
    private final AstyanaxContext<Keyspace> ctx;

    public static final ColumnFamily<String, String> CF_INSTANCES =
            new ColumnFamily<String, String>(CF_NAME_INSTANCES, StringSerializer.get(), StringSerializer.get());
    public static final ColumnFamily<String, String> CF_LOCKS =
            new ColumnFamily<String, String>(CF_NAME_LOCKS, StringSerializer.get(), StringSerializer.get());

    @Inject
    public InstanceDataDAOCassandra(IConfiguration config, EurekaHostsSupplier eurekaHostsSupplier)
            throws ConnectionException {
        this.config = config;

        BOOT_CLUSTER = config.getBootClusterName();

        if (BOOT_CLUSTER == null || BOOT_CLUSTER.isEmpty()) {
            throw new RuntimeException("Boot cluster can not be blank. Please use getBootClusterName() property");
        }

        KS_NAME = config.getCassandraKeyspaceName();

        if (KS_NAME == null || KS_NAME.isEmpty()) {
            throw new RuntimeException("Cassandra keyspace can not be blank. Please use getCassandraKeyspaceName() property");
        }

        thriftPortForAstyanax = config.getCassandraThriftPortForAstyanax();

        if (thriftPortForAstyanax <= 0) {
            throw new RuntimeException("Thrift port for Astyanax can not be blank. Please use getCassandraThriftPortForAstyanax() property");
        }

        this.eurekaHostsSupplier = eurekaHostsSupplier;

        if (config.isEurekaHostSupplierEnabled()) {
            ctx = initWithThriftDriverWithEurekaHostsSupplier();
        }
        else {
            ctx = initWithThriftDriverWithExternalHostsSupplier();
        }

        ctx.start();
        bootKeyspace = ctx.getClient();
    }

    public void createInstanceEntry(RaigadInstance instance) throws Exception {
        logger.info("Creating new instance entry");

        String key = getRowKey(instance);
        // If the key exists throw exception
        if (getInstance(instance.getApp(), instance.getDC(), instance.getId()) != null) {
            logger.info(String.format("Key already exists: %s", key));
            return;
        }

        // Grab the lock
        getLock(instance);

        MutationBatch mutationBatch = bootKeyspace.prepareMutationBatch();
        ColumnListMutation<String> columnListMutation = mutationBatch.withRow(CF_INSTANCES, key);
        columnListMutation.putColumn(CN_CLUSTER, instance.getApp(), null);
        columnListMutation.putColumn(CN_AZ, instance.getAvailabilityZone(), null);
        columnListMutation.putColumn(CN_INSTANCEID, instance.getInstanceId(), null);
        columnListMutation.putColumn(CN_HOSTNAME, instance.getHostName(), null);
        columnListMutation.putColumn(CN_IP, instance.getHostIP(), null);
        columnListMutation.putColumn(CN_LOCATION, instance.getDC(), null);
        columnListMutation.putColumn(CN_ASGNAME, instance.getAsg(), null);
        columnListMutation.putColumn(CN_UPDATETIME, TimeUUIDUtils.getUniqueTimeUUIDinMicros(), null);

        mutationBatch.execute();
    }

    public RaigadInstance getInstance(String cluster, String region, String instanceId) {
        List<RaigadInstance> instances = getAllInstances(cluster);

        for (RaigadInstance instance : instances) {
            if (instance.getInstanceId().equals(instanceId) && instance.getDC().equals(region)) {
                return instance;
            }
        }

        return null;
    }

    public List<RaigadInstance> getAllInstances(String cluster) {
        List<RaigadInstance> list = new ArrayList<RaigadInstance>();

        try {
            String selectClause;

            if (config.isMultiDC() || config.amISourceClusterForTribeNodeInMultiDC()) {
                selectClause = String.format("SELECT * FROM %s WHERE %s = '%s' ",
                        CF_NAME_INSTANCES, CN_CLUSTER, cluster);
            }
            else {
                selectClause = String.format("SELECT * FROM %s WHERE %s = '%s' AND %s = '%s' ",
                        CF_NAME_INSTANCES, CN_CLUSTER, cluster, CN_LOCATION, config.getDC());
            }

            if (config.isDebugEnabled()) {
                logger.debug("Getting nodes for {}: {}", cluster, selectClause);
            }

            final ColumnFamily<String, String> CF_INSTANCES_NEW = ColumnFamily.newColumnFamily(
                    KS_NAME, StringSerializer.get(), StringSerializer.get());

            OperationResult<CqlResult<String, String>> result =
                    bootKeyspace.prepareQuery(CF_INSTANCES_NEW).withCql(selectClause).execute();

            for (Row<String, String> row : result.getResult().getRows()) {
                list.add(transform(row.getColumns()));
            }
        }
        catch (Exception e) {
            logger.warn("Caught unknown exception while reading: {}", e.getMessage());
            throw new RuntimeException(e);
        }

        if (config.isDebugEnabled()) {
            for (RaigadInstance instance : list) {
                logger.debug("Read instance: {}", instance.toString());
            }
        }

        return list;
    }

    public void deleteInstanceEntry(RaigadInstance instance) throws Exception {
        logger.info("Deleting dead instance entry");

        // Acquire the lock first
        getLock(instance);

        // Delete the row
        String key = findKey(instance.getApp(), instance.getInstanceId(), instance.getDC());

        if (key == null) {
            return;  // don't fail it
        }

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

    protected void sort(List<RaigadInstance> list) {
        Collections.sort(list, new Comparator<RaigadInstance>() {

            @Override
            public int compare(RaigadInstance esInstance1, RaigadInstance esInstance2) {
                int azCompare = esInstance1.getAvailabilityZone().compareTo(esInstance2.getAvailabilityZone());
                if (azCompare == 0) {
                    return esInstance1.getId().compareTo(esInstance2.getId());
                }
                else {
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
    private void getLock(RaigadInstance instance) throws Exception {
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
        if (result.getResult().size() > 0 && !result.getResult().getColumnByIndex(0).getName().equals(instance.getInstanceId())) {
            throw new Exception(String.format("Lock already taken %s", lockKey));
        }

        clm = m.withRow(CF_LOCKS, lockKey);
        clm.putColumn(instance.getInstanceId(), instance.getInstanceId(), new Integer(600));
        m.execute();
        Thread.sleep(100);
        result = bootKeyspace.prepareQuery(CF_LOCKS).getKey(lockKey).execute();
        if (result.getResult().size() == 1 && result.getResult().getColumnByIndex(0).getName().equals(instance.getInstanceId())) {
            logger.info("Got lock " + lockKey);
            return;
        }
        else {
            throw new Exception(String.format("Cannot insert lock %s", lockKey));
        }
    }

    public String findKey(String cluster, String instanceId, String dc) {
        try {
            final String selectClause = String.format(
                    "SELECT * FROM %s WHERE %s = '%s' and %s = '%s' and %s = '%s'  ", CF_NAME_INSTANCES,
                    CN_CLUSTER, cluster, CN_INSTANCEID, instanceId, CN_LOCATION, dc);

            logger.info(selectClause);

            final ColumnFamily<String, String> CF_INSTANCES_NEW = ColumnFamily.newColumnFamily(KS_NAME,
                    StringSerializer.get(), StringSerializer.get());

            OperationResult<CqlResult<String, String>> result = bootKeyspace.prepareQuery(CF_INSTANCES_NEW)
                    .withCql(selectClause).execute();

            if (result == null || result.getResult().getRows().size() == 0) {
                return null;
            }

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

    private RaigadInstance transform(ColumnList<String> columns) {
        RaigadInstance instance = new RaigadInstance();
        Map<String, String> columnMap = new HashMap<>();

        for (Column<String> column : columns) {
            columnMap.put(column.getName(), column.getStringValue());
            if (column.getName().equals(CN_CLUSTER)) {
                instance.setUpdatetime(column.getTimestamp());
            }
        }

        instance.setId(columnMap.get(CN_LOCATION) + "." + columnMap.get(CN_INSTANCEID));
        instance.setApp(columnMap.get(CN_CLUSTER));
        instance.setAvailabilityZone(columnMap.get(CN_AZ));
        instance.setHostName(columnMap.get(CN_HOSTNAME));
        instance.setHostIP(columnMap.get(CN_IP));
        instance.setInstanceId(columnMap.get(CN_INSTANCEID));
        instance.setAsg(columnMap.get(CN_ASGNAME));
        instance.setDC(columnMap.get(CN_LOCATION));

        return instance;
    }

    private String getChoosingKey(RaigadInstance instance) {
        return instance.getApp() + "_" + instance.getDC() + "_" + instance.getInstanceId() + "-choosing";
    }

    private String getLockingKey(RaigadInstance instance) {
        return instance.getApp() + "_" + instance.getDC() + "_" + instance.getInstanceId() + "-lock";
    }

    private String getRowKey(RaigadInstance instance) {
        return instance.getApp() + "_" + instance.getDC() + "_" + instance.getInstanceId();
    }

    private AstyanaxContext<Keyspace> initWithThriftDriverWithEurekaHostsSupplier() {
        logger.info("Boot cluster (BOOT_CLUSTER) is {}, keyspace name (KS_NAME) is {}", BOOT_CLUSTER, KS_NAME);

        return new AstyanaxContext.Builder()
                .forCluster(BOOT_CLUSTER)
                .forKeyspace(KS_NAME)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(
                                        NodeDiscoveryType.DISCOVERY_SERVICE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(
                                "MyConnectionPool")
                                .setMaxConnsPerHost(3)
                                .setPort(thriftPortForAstyanax))
                .withHostSupplier(eurekaHostsSupplier.getSupplier(BOOT_CLUSTER))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

    }

    private AstyanaxContext<Keyspace> initWithThriftDriverWithExternalHostsSupplier() {
        logger.info("Boot cluster (BOOT_CLUSTER) is {}, keyspace name (KS_NAME) is {}", BOOT_CLUSTER, KS_NAME);

        return new AstyanaxContext.Builder()
                .forCluster(BOOT_CLUSTER)
                .forKeyspace(KS_NAME)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(
                                        NodeDiscoveryType.DISCOVERY_SERVICE)
                                .setConnectionPoolType(
                                        ConnectionPoolType.ROUND_ROBIN))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(
                                "MyConnectionPool")
                                .setMaxConnsPerHost(3)
                                .setPort(thriftPortForAstyanax))
                .withHostSupplier(getSupplier())
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

    }

    private Supplier<List<Host>> getSupplier() {
        return new Supplier<List<Host>>() {
            @Override
            public List<Host> get() {
                List<Host> hosts = new ArrayList<>();
                List<String> cassandraHostnames = new ArrayList<>(Arrays.asList(StringUtils.split(config.getCommaSeparatedCassandraHostNames(), ",")));

                if (cassandraHostnames.size() == 0) {
                    throw new RuntimeException("Cassandra host names can not be blank, at least one host is needed." +
                            "Please use getCommaSeparatedCassandraHostNames() property.");
                }

                for (String cassHost : cassandraHostnames) {
                    logger.info("Adding Cassandra host {}", cassHost);
                    hosts.add(new Host(cassHost, thriftPortForAstyanax));
                }

                return hosts;
            }
        };
    }
}
