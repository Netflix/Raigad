package com.netflix.elasticcar.backup;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.elasticcar.config.UnitTestModule;
import com.netflix.elasticcar.configuration.IConfiguration;
import com.netflix.elasticcar.utils.ESTransportClient;
import mockit.Mock;
import mockit.Mocked;
import mockit.Mockit;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 * Reference:https://github.com/elasticsearch/elasticsearch-cloud-aws/blob/es-1.1/src/test/java/org/elasticsearch/repositories/s3/S3SnapshotRestoreTest.java
 *
 * Following tests do not test S3 cloud functionality but uses fs (file system) locally to run Snapshot and Backup
 * TODO Need to fix for S3 functionality
 */
/*
    {
        "20140331": {
            "type": "s3",
            "settings": {
                "region": "us-east-1",
                "base_path": "es_test/20140331",
                "bucket": "es-backup-test"
            }
        },
        "20140410": {
            "type": "s3",
            "settings": {
                "region": "us-east-1",
                "base_path": "es_test/20140410",
                "bucket": "es-backup-test"
            }
        }
    }
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes=2)
public class TestBackupRestore extends ElasticsearchIntegrationTest
{
    private static final char PATH_SEP = File.separatorChar;
    public static String repositoryName="";
    public static String repositoryLocation="";
    public static String LOCAL_DIR="data";

    private static Injector injector;
    public static Client client0;

    @Mocked
    private static ESTransportClient esTransportClient;

    private static IConfiguration conf;
    private static S3RepositorySettingsParams s3RepositorySettingsParams;
    private static S3Repository s3Repository;
    @Mocked
    private static SnapshotBackupManager snapshotBackupManager;
    @Mocked
    private static RestoreBackupManager restoreBackupManager;


    @Before
    public final void setup() throws IOException {

        injector = Guice.createInjector(new UnitTestModule());
        conf = injector.getInstance(IConfiguration.class);
        s3RepositorySettingsParams = injector.getInstance(S3RepositorySettingsParams.class);

        Mockit.setUpMock(ESTransportClient.class, MockESTransportClient.class);
        esTransportClient = injector.getInstance(ESTransportClient.class);

        Mockit.setUpMock(S3Repository.class, MockS3Repository.class);
        s3Repository = injector.getInstance(S3Repository.class);

        Mockit.setUpMock(SnapshotBackupManager.class, MockSnapshotBackupManager.class);
        if(snapshotBackupManager == null)
            snapshotBackupManager = injector.getInstance(SnapshotBackupManager.class);

        Mockit.setUpMock(RestoreBackupManager.class, MockRestoreBackupManager.class);
        if(restoreBackupManager == null)
            restoreBackupManager = injector.getInstance(RestoreBackupManager.class);

        wipeRepositories();
        cleanupDir(LOCAL_DIR, null);
    }

    @After
    public final void wipeAfter() throws IOException {
        wipeRepositories();
        injector = null;
        conf = null;
        s3RepositorySettingsParams = null;
        s3Repository = null;
        esTransportClient = null;
        client0= null;
        cleanupDir(LOCAL_DIR, null);
    }

    @Test
    public void testSimpleWorkflow() throws Exception
    {
        client0 = client();
        repositoryName = s3Repository.getRemoteRepositoryName();

        //Create S3 Repository
        assertFalse(s3Repository.createOrGetSnapshotRepository() == null);

        createIndex("test-idx-1", "test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-3", "doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();
        assertThat(client0.prepareCount("test-idx-1").get().getCount(), equalTo(100L));
        assertThat(client0.prepareCount("test-idx-3").get().getCount(), equalTo(100L));

        //Run backup
        snapshotBackupManager.runSnapshotBackup();

        assertThat(client0.admin().cluster().prepareGetSnapshots(repositoryName)
                .setSnapshots(snapshotBackupManager.getSnapshotName("_all", false)).get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> delete some data");
        for (int i = 0; i < 50; i++) {
            client0.prepareDelete("test-idx-1", "doc", Integer.toString(i)).get();
        }
        for (int i = 0; i < 100; i += 2) {
            client0.prepareDelete("test-idx-3", "doc", Integer.toString(i)).get();
        }
        refresh();
        assertThat(client0.prepareCount("test-idx-1").get().getCount(), equalTo(50L));
        assertThat(client0.prepareCount("test-idx-3").get().getCount(), equalTo(50L));

        logger.info("--> close indices");
        client0.admin().indices().prepareClose("test-idx-1", "test-idx-3").get();

        logger.info("--> restore all indices from the snapshot");
        restoreBackupManager.runRestore(repositoryName,"fs",snapshotBackupManager.getSnapshotName("_all", false),null);

        ensureGreen();
        assertThat(client0.prepareCount("test-idx-1").get().getCount(), equalTo(100L));
        assertThat(client0.prepareCount("test-idx-3").get().getCount(), equalTo(100L));

    }

    @Ignore
    public static class MockESTransportClient
    {
        @Mock
        public static ESTransportClient instance(IConfiguration config)
        {
            return esTransportClient;
        }

        @Mock
        public Client getTransportClient(){

            return client0;
        }
    }

    @Ignore
    public static class MockS3Repository
    {
        @Mock
        public PutRepositoryResponse getPutRepositoryResponse(Client esTransportClient,String s3RepoName)
        {
            PutRepositoryResponse putRepositoryResponse = client0.admin().cluster().preparePutRepository(repositoryName)
                    .setType(AbstractRepository.RepositoryType.fs.name()).setSettings(ImmutableSettings.settingsBuilder()
                                    .put("location", LOCAL_DIR + PATH_SEP + s3RepositorySettingsParams.getBase_path())
                    ).get();

            //Setting local repository location
            repositoryLocation = LOCAL_DIR + PATH_SEP+ s3RepositorySettingsParams.getBase_path();
            return putRepositoryResponse;
        }

    }

    @Ignore
    public static class MockSnapshotBackupManager
    {
        @Mock
        public CreateSnapshotResponse getCreateSnapshotResponse(Client esTransportClient,String repositoryName, String snapshotName)
        {
            return client0.admin().cluster().prepareCreateSnapshot(repositoryName, snapshotName)
                    .setWaitForCompletion(conf.waitForCompletionOfBackup())
                    .setIndices(conf.getCommaSeparatedIndicesToBackup())
                    .setIncludeGlobalState(conf.includeGlobalStateDuringBackup())
                    .setPartial(conf.partiallyBackupIndices()).get();
        }
    }

    @Ignore
    public static class MockRestoreBackupManager
    {
        @Mock
        public RestoreSnapshotResponse getRestoreSnapshotResponse(Client esTransportClient, String commaSeparatedIndices,String restoreRepositoryName,String snapshotN)
        {
            snapshotN = snapshotBackupManager.getSnapshotName("_all", false);
            return client0.admin().cluster().prepareRestoreSnapshot(repositoryName, snapshotN)
                    .setIndices("test-idx-*")
                    .setWaitForCompletion(true)
                    .execute()
                    .actionGet();
        }
    }

    public static void cleanupDir(String dirPath, List<String> childdirs) throws IOException
    {
        if (childdirs == null || childdirs.size() == 0)
            FileUtils.cleanDirectory(new File(dirPath));
        else
        {
            for (String cdir : childdirs)
                FileUtils.cleanDirectory(new File(dirPath + "/" + cdir));
        }
    }

    /**
     * Deletes repositories, supports wildcard notation.
     */
    public static void wipeRepositories(String... repositories) {
        // if nothing is provided, delete all
        if (repositories.length == 0) {
            repositories = new String[]{"*"};
        }
        for (String repository : repositories) {
            try {
                client().admin().cluster().prepareDeleteRepository(repository).execute().actionGet();
            } catch (RepositoryMissingException ex) {
                // ignore
            }
        }
    }
}
