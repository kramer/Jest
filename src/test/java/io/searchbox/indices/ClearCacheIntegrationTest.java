package io.searchbox.indices;

import com.github.tlrx.elasticsearch.test.annotations.*;
import com.github.tlrx.elasticsearch.test.support.junit.runners.ElasticsearchRunner;
import io.searchbox.client.JestResult;
import io.searchbox.common.AbstractIntegrationTest;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.AdminClient;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author cihat keser
 */
@RunWith(ElasticsearchRunner.class)
@ElasticsearchNode
public class ClearCacheIntegrationTest extends AbstractIntegrationTest {
    private static final String INDEX_NAME = "test_index";
    private static final String INDEX_NAME_2 = "test_index_2";
    private static final String TYPE = "test_type";
    @ElasticsearchAdminClient
    AdminClient adminClient;
    @ElasticsearchClient
    org.elasticsearch.client.Client directClient;

    // TODO find a way to check cache sizes
    @Ignore
    @Test
    @ElasticsearchIndexes(indexes = {
            @ElasticsearchIndex(indexName = INDEX_NAME),
            @ElasticsearchIndex(indexName = INDEX_NAME_2)
    })
    public void testClear() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        String testId = "my_id";

        ActionFuture<IndexResponse> indexResponseActionFuture = directClient.index(
                new IndexRequest(INDEX_NAME, TYPE, testId).source("{\"this\":\"that\"}"));
        IndexResponse indexResponse = indexResponseActionFuture.actionGet(10, TimeUnit.SECONDS);
        assertNotNull(indexResponse);

        directClient.get(new GetRequest(INDEX_NAME, TYPE, testId)).actionGet(10, TimeUnit.SECONDS);

        ClearCache clearCache = new ClearCache.Builder().addIndex(INDEX_NAME).build();
        JestResult result = client.execute(clearCache);
        assertNotNull(result);
        assertTrue(result.isSucceeded());

        NodesStatsResponse nodesStatsResponse =
                adminClient.cluster().nodesStats(new NodesStatsRequest().all()).actionGet(10, TimeUnit.SECONDS);
        assertNotNull(nodesStatsResponse);
    }
}
