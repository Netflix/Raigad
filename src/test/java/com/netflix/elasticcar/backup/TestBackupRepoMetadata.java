package com.netflix.elasticcar.backup;

import com.netflix.elasticcar.objectmapper.DefaultRepositoryMapper;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * Created by sloke on 7/2/14.
 */
/*
    {
        "20140331": {
            "type": "s3",
            "settings": {
                "region": "us-east-1",
                "base_path": "es_chronos/20140331",
                "bucket": "elasticsearch-backup-prod"
            }
        },
        "20140410": {
            "type": "s3",
            "settings": {
                "region": "us-east-1",
                "base_path": "es_chronos/20140410",
                "bucket": "elasticsearch-backup-prod"
            }
        }
    }
 */
public class TestBackupRepoMetadata
{
    ObjectMapper mapper = new DefaultRepositoryMapper();

    @Test
    public void testBackupRepoSnapshotSettingsWrapperObject() throws IOException
    {
        String str = "{   \"20140331\":{   \"type2\": \"s3\",     \"type\": \"s3\",        \"settings\": {     \"region\": \"us-east-1\", \"base_path\": \"es_chronos/20140331\", \"bucket\": \"elasticsearch-backup-prod\"}     }," +
                     "    \"20140410\":{        \"type\": \"s3\",        \"settings\": {     \"region\": \"us-east-1\", \"base_path\": \"es_chronos/20140410\", \"bucket\": \"elasticsearch-backup-prod\"}     }}";
        String settings = "{   \"type\": \"s3\",       \"settings\": {     \"region\": \"us-east-1\", \"base_path\": \"es_chronos/20140331\", \"bucket\": \"elasticsearch-backup-prod\"}     }";
        String str2 = "{}";
        try
        {
            RepositoryWrapperDO repositoryWrapperDO = mapper.readValue(settings,RepositoryWrapperDO.class);
            System.out.println("Deserialization done");
            System.out.println("Serializing");
            System.out.println(mapper.writeValueAsString(repositoryWrapperDO));

            Map<String,RepositoryWrapperDO> myObjs = mapper.readValue(str2, new TypeReference<Map<String,RepositoryWrapperDO>>(){});

            for(String key:myObjs.keySet()) {
                System.out.println("***Key = " + key);
                if(myObjs.get(key).getType().equalsIgnoreCase(AbstractRepository.RepositoryType.s3.toString()))
                    System.out.println(" Type = " + myObjs.get(key).getType());
            }
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRepositoryMappingSerialization() throws Exception
    {
        RepositorySettingsDO repositorySettingsDO = new RepositorySettingsDO("us-east-1","es_chronos/20140331","elasticsearch-backup-prod");
        RepositoryWrapperDO repositoryWrapperDO = new RepositoryWrapperDO("s3",repositorySettingsDO);
        ObjectMapper mapper = new DefaultRepositoryMapper();
        System.out.println(mapper.writeValueAsString(repositoryWrapperDO));
    }

    /*
 * ec2-50-19-28-170.compute-1.amazonaws.com:7104/_snapshot/20140320/snapshot_1?wait_for_completion=true
 *
 * {
 *  "indices": "index_1,index_2",   //  "indices": "_all",
 *  "ignore_unavailable": "true",
 *  "include_global_state": false
 * }
 *
 */
    @Test
    public void testSnapshotMappingSerialization() throws Exception
    {
        SnapshotSettingsDO snapshotSettingsDO = new SnapshotSettingsDO("index_1,index_2","true",false);
        ObjectMapper mapper = new DefaultRepositoryMapper();
        System.out.println(mapper.writeValueAsString(snapshotSettingsDO));
    }
}
