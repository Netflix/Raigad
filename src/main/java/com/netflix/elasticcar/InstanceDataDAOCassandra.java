package com.netflix.elasticcar;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.TimeUUIDUtils;
import com.netflix.cassandra.NFAstyanaxManager;
import com.netflix.elasticcar.identity.ElasticCarInstance;
import com.netflix.instance.identity.StorageDevice;
import com.netflix.library.NFLibraryManager;


@Singleton
public class InstanceDataDAOCassandra
{
	private static final Logger logger = LoggerFactory.getLogger(InstanceDataDAOCassandra.class);
	private static final String CF_NAME = "cassbootstrap";
	private static final String CN_ID = "Id";
	private static final String CN_APPID = "appId";
	private static final String CN_AZ = "availabilityZone";
	private static final String CN_INSTANCEID = "instanceId";
	private static final String CN_HOSTNAME = "hostname";
	private static final String CN_EIP = "elasticIP";
	private static final String CN_TOKEN = "token";
	private static final String CN_LOCATION = "location";
	private static final String CN_VOLUME_PREFIX = "ssVolumes";
	private static final String CN_UPDATETIME = "updatetime";
	private static final String CF_NAME_TOKENS = "tokens";
	private static final String CF_NAME_LOCKS = "locks";

	private final Keyspace bootKeyspace;

	/*
	 * Schema: create column family tokens with comparator=UTF8Type and
	 * column_metadata=[ {column_name: appId, validation_class:
	 * UTF8Type,index_type: KEYS}, {column_name: instanceId, validation_class:
	 * UTF8Type}, {column_name: token, validation_class: UTF8Type},
	 * {column_name: availabilityZone, validation_class: UTF8Type},
	 * {column_name: hostname, validation_class: UTF8Type},{column_name: Id,
	 * validation_class: UTF8Type}, {column_name: elasticIP, validation_class:
	 * UTF8Type}, {column_name: updatetime, validation_class: TimeUUIDType},
	 * {column_name: location, validation_class: UTF8Type}];
	 */
	public static final ColumnFamily<String, String> CF_TOKENS = new ColumnFamily<String, String>(CF_NAME_TOKENS, StringSerializer.get(), StringSerializer.get());
	// Schema: create column family locks with comparator=UTF8Type;
	public static final ColumnFamily<String, String> CF_LOCKS = new ColumnFamily<String, String>(CF_NAME_LOCKS, StringSerializer.get(), StringSerializer.get());

	@Inject
	public InstanceDataDAOCassandra(IConfiguration config)
	{
		this(config.getBootClusterName());
	}

	public InstanceDataDAOCassandra(final String bootCluster)
	{
		final Properties props = new Properties();
		final String base = bootCluster  + "." + CF_NAME + ".astyanax.";
		props.setProperty(base + "readConsistency", "CL_LOCAL_QUORUM");
		props.setProperty(base + "writeConsistency", "CL_LOCAL_QUORUM");
		props.setProperty(base + "connectionPool", "BAG");
		props.setProperty(base + "maxConns", "2");
		//		props.setProperty(base + "cqlVersion", "3.0.0");

		try {
			NFLibraryManager.initLibrary(NFAstyanaxManager.class, props, true, false);
			NFAstyanaxManager.getInstance().registerKeyspace(bootCluster, CF_NAME);
			bootKeyspace = NFAstyanaxManager.getInstance().getRegisteredKeyspace(bootCluster, CF_NAME);
		}
		catch (Exception e) {
			logger.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	public void createInstanceEntry(ElasticCarInstance instance) throws Exception
	{
		logger.info("***Creating New Instance Entry");
		String key = getRowKey(instance);
		// If the key exists throw exception
		if (getInstance(instance.getApp(), instance.getDC(), instance.getId()) != null)
			throw new Exception(String.format("Key already exists: %s", key));
		// Grab the lock
		getLock(instance);
		MutationBatch m = bootKeyspace.prepareMutationBatch();
		ColumnListMutation<String> clm = m.withRow(CF_TOKENS, key);
		clm.putColumn(CN_ID, Integer.toString(instance.getId()), null);
		clm.putColumn(CN_APPID, instance.getApp(), null);
		clm.putColumn(CN_AZ, instance.getRac(), null);
		clm.putColumn(CN_INSTANCEID, instance.getInstanceId(), null);
		clm.putColumn(CN_HOSTNAME, instance.getHostName(), null);
		clm.putColumn(CN_EIP, instance.getHostIP(), null);
		clm.putColumn(CN_TOKEN, instance.getToken(), null);
		clm.putColumn(CN_LOCATION, instance.getDC(), null);
		clm.putColumn(CN_UPDATETIME, TimeUUIDUtils.getUniqueTimeUUIDinMicros(), null);
		Map<String, Object> volumes = instance.getVolumes();
		if (volumes != null) {
			for (String path : volumes.keySet()) {
				clm.putColumn(CN_VOLUME_PREFIX + "_" + path, volumes.get(path).toString(), null);
			}
		}
		m.execute();
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

	public void deleteInstanceEntry(ElasticCarInstance instance) throws Exception
	{
		// Acquire the lock first
		getLock(instance);

		// Delete the row
		String key = findKey(instance.getApp(), String.valueOf(instance.getId()), instance.getDC());
		if (key == null)
			return;  //don't fail it

		MutationBatch m = bootKeyspace.prepareMutationBatch();
		m.withRow(CF_TOKENS, key).delete();
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

	public ElasticCarInstance getInstance(String app, String dc, int id)
	{
		Set<ElasticCarInstance> set = getAllInstances(app);
		for (ElasticCarInstance ins : set) {
			if (ins.getId() == id && ins.getDC().equals(dc))
				return ins;
		}
		return null;
	}

	public Set<ElasticCarInstance> getAllInstances(String app)
	{
		Set<ElasticCarInstance> set = new HashSet<ElasticCarInstance>();
		try {

			final String selectClause = String.format("SELECT * FROM %s WHERE %s = '%s' ", CF_NAME_TOKENS, CN_APPID, app);
			logger.debug(selectClause);

			final ColumnFamily<String, String> CF_TOKENS_NEW = ColumnFamily.newColumnFamily(CF_NAME,
					StringSerializer.get(), StringSerializer.get());

			OperationResult<CqlResult<String, String>> result = bootKeyspace.prepareQuery(CF_TOKENS_NEW)
					.withCql(selectClause).execute();

			for (Row<String, String> row : result.getResult().getRows())
				set.add(transform(row.getColumns()));
		}
		catch (Exception e) {
			logger.warn("Caught an Unknown Exception during reading msgs from tokens ... -> " + ExceptionUtils.getFullStackTrace(e));
			throw new RuntimeException(e);
		}
		return set;
	}


	public String findKey(String app, String id, String location)
	{
		try {
			final String selectClause = String.format(
					"SELECT * FROM %s WHERE %s = '%s' and %s = '%s' and %s = '%s'  ", "tokens", CN_APPID, app, CN_ID, id, CN_LOCATION, location);
			logger.info(selectClause);


			final ColumnFamily<String, String> CF_TOKENS_NEW = ColumnFamily.newColumnFamily("cassbootstrap",
					StringSerializer.get(), StringSerializer.get());

			OperationResult<CqlResult<String, String>> result = bootKeyspace.prepareQuery(CF_TOKENS_NEW)
					.withCql(selectClause).execute();

			if (result == null || result.getResult().getRows().size() == 0)
				return null;

			Row<String, String> row = result.getResult().getRows().getRowByIndex(0);
			return row.getKey();

		}
		catch (Exception e) {
			logger.warn("Caught an Unknown Exception during find a row from tokens matching app[" + app + "], id[" + id + "], and location[" + location + "]  ... -> "
					+ ExceptionUtils.getFullStackTrace(e));
			throw new RuntimeException(e);
		}

	}


	private ElasticCarInstance transform(ColumnList<String> columns)
	{
		ElasticCarInstance ins = new ElasticCarInstance();
		Map<String, StorageDevice> volumes = new HashMap<String, StorageDevice>();
		Map<String, String> cmap = new HashMap<String, String>();
		for (Column<String> column : columns) {
			//        		logger.info("***Column Name = "+column.getName()+ " Value = "+column.getStringValue());
			cmap.put(column.getName(), column.getStringValue());
			if (column.getName().startsWith(CN_VOLUME_PREFIX)) {
				String[] volName = column.getName().split("_");
				StorageDevice sd = new StorageDevice();
				sd.deserialize(column.getStringValue());
				volumes.put(volName[1], sd);
			}
			if (column.getName().equals(CN_APPID))
				ins.setUpdatetime(column.getTimestamp());
		}

		ins.setApp(cmap.get(CN_APPID));
		ins.setRac(cmap.get(CN_AZ));
		ins.setHost(cmap.get(CN_HOSTNAME));
		ins.setHostIP(cmap.get(CN_EIP));
		ins.setId(Integer.parseInt(cmap.get(CN_ID)));
		ins.setInstanceId(cmap.get(CN_INSTANCEID));
		ins.setDC(cmap.get(CN_LOCATION));
		ins.setToken(cmap.get(CN_TOKEN));
		Map<String, Object> sobjs = new HashMap<String, Object>();
		for( String name : volumes.keySet())
			sobjs.put(name, volumes.get(name));

		ins.setVolumes(sobjs);
		return ins;
	}

	private String getChoosingKey(ElasticCarInstance instance)
	{
		return instance.getApp() + "_" + instance.getDC() + "_" + instance.getId() + "-choosing";
	}

	private String getLockingKey(ElasticCarInstance instance)
	{
		return instance.getApp() + "_" + instance.getDC() + "_" + instance.getId() + "-lock";
	}

	private String getRowKey(ElasticCarInstance instance)
	{
		return instance.getApp() + "_" + instance.getDC() + "_" + instance.getId();
	}

}
