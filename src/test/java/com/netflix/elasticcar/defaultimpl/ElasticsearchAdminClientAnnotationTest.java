package com.netflix.elasticcar.defaultimpl;

import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchAdminClient;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchNode;
import com.github.tlrx.elasticsearch.test.support.junit.runners.ElasticsearchRunner;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.node.Node;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

/**
 * Created by sloke on 6/22/14.
 */
@RunWith(ElasticsearchRunner.class)
@ElasticsearchNode
public class ElasticsearchAdminClientAnnotationTest {

    @ElasticsearchAdminClient
    AdminClient adminClient0;

    @ElasticsearchNode(name = "node1")
    Node node1;

    @ElasticsearchAdminClient(nodeName = "node1")
    AdminClient adminClient1;

    @Test
    public void testElasticsearchAdminClients() {

        NodesInfoResponse nodeInfos = adminClient0.cluster().prepareNodesInfo().execute().actionGet();

        System.out.println("Cluster Name = " + nodeInfos.getClusterName());

        ClusterStatsResponse clusterStats = adminClient0.cluster().prepareClusterStats().execute().actionGet();

        System.out.println("Cluster Health Status = "+clusterStats.getStatus().toString());
        System.out.println("Node Count = " + nodeInfos.getNodes().length);

        for (NodeInfo nd: nodeInfos.getNodes())
        {
            System.out.println("*Node = "+nd.getNode().getName());
        }

        assertNotNull(adminClient0);

        assertNotNull(node1);
        assertNotNull(adminClient1);

        assertNotSame(adminClient0, adminClient1);

//        adminClient1.cluster().clusterStats()
    }
}