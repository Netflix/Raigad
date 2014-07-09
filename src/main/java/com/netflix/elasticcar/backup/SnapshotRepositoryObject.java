package com.netflix.elasticcar.backup;

import java.util.List;

/**
 * Created by sloke on 7/7/14.
 *
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
public class SnapshotRepositoryObject
{
   private List<RepositoryWrapperDO> repositoryWrapperDOList;


}
