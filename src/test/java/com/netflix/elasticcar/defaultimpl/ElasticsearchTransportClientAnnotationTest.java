package com.netflix.elasticcar.defaultimpl;

import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchNode;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchSetting;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchTransportClient;
import com.github.tlrx.elasticsearch.test.support.junit.runners.ElasticsearchRunner;
import com.netflix.elasticcar.utils.SystemUtils;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.node.Node;
import org.json.simple.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;

/**
 * Created by sloke on 6/22/14.
 */

@RunWith(ElasticsearchRunner.class)
public class ElasticsearchTransportClientAnnotationTest {

    private static final String url = "http://127.0.0.1:9400/_cluster/settings";
    private static final String SHARD_REALLOCATION_PROPERTY = "cluster.routing.allocation.enable";

        @ElasticsearchNode(name = "node0", local = false, clusterName = "external",
                settings = {
                        @ElasticsearchSetting(name = "transport.tcp.port", value = "9500"),
                        @ElasticsearchSetting(name = "http.port", value = "9400"),
                        @ElasticsearchSetting(name = "cluster.routing.allocation.enable", value = "all")
                })
        Node node0;

        @ElasticsearchNode(name = "node1", local = false, clusterName = "external",
                settings = {
                        @ElasticsearchSetting(name = "cluster.routing.allocation.enable", value = "all")
                })
        Node node1;

    @ElasticsearchNode(name = "node2", local = false, clusterName = "external")
    Node node2;

    @ElasticsearchTransportClient(clusterName = "external",
                hostnames = {"127.0.0.1"},
                ports = {9500})
        TransportClient client0;

        @Test
//        @ElasticsearchIndex(indexName = "sites", nodeName = "node0")
        public void testTransportClient() {

            assertNotNull(node0);
            assertNotNull(client0);

//            // Checks if the index has been created
//            IndicesExistsResponse existResponse = client0.admin().indices()
//                    .prepareExists("sites")
//                    .execute().actionGet();
//            assertTrue("Index must exist", existResponse.isExists());

            NodesInfoResponse nodeInfos = client0.admin().cluster().prepareNodesInfo().execute().actionGet();

            System.out.println("Cluster Name = " + nodeInfos.getClusterName());

            ClusterStatsResponse clusterStats = client0.admin().cluster().prepareClusterStats().execute().actionGet();

            System.out.println("Cluster Health Status = "+clusterStats.getStatus().toString());
            System.out.println("Node Count = " + nodeInfos.getNodes().length);

            for (NodeInfo nd: nodeInfos.getNodes())
            {
                System.out.println("*Node = "+nd.getNode().getName());
            }

            try {
                String RESPONSE = SystemUtils.getDataFromUrl(url);
                System.out.println("*** RESPONSE = <"+RESPONSE+">");
                String RESPONSE2 = SystemUtils.runHttpGetCommand(url);
                System.out.println("*** RESPONSE = <"+RESPONSE2+">");
                JSONObject settings = new JSONObject();
                JSONObject property = new JSONObject();
                property.put(SHARD_REALLOCATION_PROPERTY,"all");
                settings.put("transient",property);
                String RESPONSE3 = SystemUtils.runHttpPutCommand(url,settings.toJSONString());
                System.out.println("*** RESPONSE = <"+RESPONSE3+">");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
}

