package com.netflix.raigad.backup;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.configuration.UnitTestModule;
import com.netflix.raigad.utils.ESTransportClient;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Reference:https://github.com/elasticsearch/elasticsearch-cloud-aws/blob/es-1.1/src/test/java/org/elasticsearch/repositories/s3/S3SnapshotRestoreTest.java
 * <p>
 * Following tests do not test S3 cloud functionality but uses fs (file system) locally to run Snapshot and Backup
 * TODO: Need to fix for S3 functionality
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
@Ignore
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2)
public class TestBackupRestore extends ESIntegTestCase {
    private static final char PATH_SEP = File.separatorChar;

    public static String repositoryName = "";
    public static String repositoryLocation = "";
    public static String LOCAL_DIR = "data";

    private static Injector injector;
    public static Client client0;

    @Mocked
    private static ESTransportClient esTransportClient;

    private static IConfiguration configuration;
    private static S3RepositorySettingsParams s3RepositorySettingsParams;
    private static S3Repository s3Repository;

    @Mocked
    private static SnapshotBackupManager snapshotBackupManager;

    @Mocked
    private static RestoreBackupManager restoreBackupManager;


    @Before
    public final void setup() throws IOException {
        System.out.println("Running setup now...");

        injector = Guice.createInjector(new UnitTestModule());

        configuration = injector.getInstance(IConfiguration.class);
        s3RepositorySettingsParams = injector.getInstance(S3RepositorySettingsParams.class);
        esTransportClient = injector.getInstance(ESTransportClient.class);
        s3Repository = injector.getInstance(S3Repository.class);

        if (snapshotBackupManager == null) {
            snapshotBackupManager = injector.getInstance(SnapshotBackupManager.class);
        }

        if (restoreBackupManager == null) {
            restoreBackupManager = injector.getInstance(RestoreBackupManager.class);
        }

        wipeRepositories();

        cleanupDir(LOCAL_DIR, null);
    }

    @After
    public final void wipeAfter() throws IOException {
        System.out.println("Running wipeAfter ...");
        wipeRepositories();
        injector = null;
        configuration = null;
        s3RepositorySettingsParams = null;
        s3Repository = null;
        esTransportClient = null;
        client0 = null;
        cleanupDir(LOCAL_DIR, null);
    }

    @Test
    public void testSimpleWorkflow() throws Exception {
        client0 = client();
        repositoryName = s3Repository.getRemoteRepositoryName();

        //Create S3 Repository
        Assert.assertFalse(s3Repository.createOrGetSnapshotRepository() == null);

        createIndex("test-idx-1", "test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-3", "doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();

        Assert.assertEquals(client0.prepareSearch("test-idx-1").setSize(0).get().getHits().getTotalHits(), 100L);
        Assert.assertEquals(client0.prepareSearch("test-idx-3").setSize(0).get().getHits().getTotalHits(), 100L);

        //Run backup
        snapshotBackupManager.runSnapshotBackup();

        Assert.assertEquals(
                client0.admin().cluster().prepareGetSnapshots(repositoryName).setSnapshots(
                        snapshotBackupManager.getSnapshotName("_all", false))
                        .get().getSnapshots().get(0).state(), SnapshotState.SUCCESS);

        logger.info("--> delete some data");

        for (int i = 0; i < 50; i++) {
            client0.prepareDelete("test-idx-1", "doc", Integer.toString(i)).get();
        }

        for (int i = 0; i < 100; i += 2) {
            client0.prepareDelete("test-idx-3", "doc", Integer.toString(i)).get();
        }

        refresh();

        Assert.assertEquals(client0.prepareSearch("test-idx-1").setSize(0).get().getHits().getTotalHits(), 50L);
        Assert.assertEquals(client0.prepareSearch("test-idx-3").setSize(0).get().getHits().getTotalHits(), 50L);

        logger.info("--> close indices");
        client0.admin().indices().prepareClose("test-idx-1", "test-idx-3").get();

        logger.info("--> restore all indices from the snapshot");
        restoreBackupManager.runRestore(repositoryName, "fs", snapshotBackupManager.getSnapshotName("_all", false), null, null, null);

        ensureGreen();

        Assert.assertEquals(client0.prepareSearch("test-idx-1").setSize(0).get().getHits().getTotalHits(), 100L);
        Assert.assertEquals(client0.prepareSearch("test-idx-3").setSize(0).get().getHits().getTotalHits(), 100L);
    }

    @Ignore
    public static class MockESTransportClient extends MockUp<ESTransportClient>{
        @Mock
        public static ESTransportClient instance(IConfiguration config) {
            return esTransportClient;
        }

        @Mock
        public Client getTransportClient() {
            return client0;
        }
    }

    @Ignore
    public static class MockS3Repository extends MockUp<S3Repository> {
        @Mock
        public PutRepositoryResponse getPutRepositoryResponse(Client esTransportClient, String s3RepoName) {
            String localRepositoryLocation = LOCAL_DIR + PATH_SEP + s3RepositorySettingsParams.getBase_path();

            PutRepositoryResponse putRepositoryResponse =
                    client0.admin().cluster()
                            .preparePutRepository(repositoryName)
                            .setType(AbstractRepository.RepositoryType.fs.name())
                            .setSettings(Settings.settingsBuilder().put("location", localRepositoryLocation))
                            .get();

            //Setting local repository location
            repositoryLocation = localRepositoryLocation;

            return putRepositoryResponse;
        }

    }

    @Ignore
    public static class MockSnapshotBackupManager extends MockUp<SnapshotBackupManager> {
        @Mock
        public CreateSnapshotResponse getCreateSnapshotResponse(Client esTransportClient, String repositoryName, String snapshotName) {
            return client0.admin().cluster().prepareCreateSnapshot(repositoryName, snapshotName)
                    .setWaitForCompletion(configuration.waitForCompletionOfBackup())
                    .setIndices(configuration.getCommaSeparatedIndicesToBackup())
                    .setIncludeGlobalState(configuration.includeGlobalStateDuringBackup())
                    .setPartial(configuration.partiallyBackupIndices()).get();
        }
    }

    @Ignore
    public static class MockRestoreBackupManager extends MockUp<RestoreBackupManager> {
        @Mock
        public RestoreSnapshotResponse getRestoreSnapshotResponse(
                Client esTransportClient, String commaSeparatedIndices, String restoreRepositoryName, String snapshotN) {
            snapshotN = snapshotBackupManager.getSnapshotName("_all", false);
            return client0.admin().cluster().prepareRestoreSnapshot(repositoryName, snapshotN)
                    .setIndices("test-idx-*")
                    .setWaitForCompletion(true)
                    .execute()
                    .actionGet();
        }
    }

    public static void cleanupDir(String dirPath, List<String> childDirs) throws IOException {
        if (childDirs == null || childDirs.size() == 0) {
            FileUtils.cleanDirectory(new File(dirPath));
        } else {
            for (String childDir : childDirs) {
                FileUtils.cleanDirectory(new File(dirPath + "/" + childDir));
            }
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