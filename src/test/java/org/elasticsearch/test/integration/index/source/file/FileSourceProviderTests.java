package org.elasticsearch.test.integration.index.source.file;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Classes;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHits;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.client.Requests.deleteIndexRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryString;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class FileSourceProviderTests {

    private Node node = null;

    private Client client = null;

    protected final ESLogger logger = Loggers.getLogger(getClass());

    private Node buildNode(String id, Settings settings) {
        String settingsSource = getClass().getName().replace('.', '/') + ".yml";
        Settings finalSettings = settingsBuilder()
                .loadFromClasspath(settingsSource)
                .put(settings)
                .put("name", id)
                .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress().getHostName())
                .put("gateway.type", "none")
                .build();

        Node node = nodeBuilder()
                .settings(finalSettings)
                .local(true)
                .build();
        return node;
    }

    private Node startNode(String id, Settings settings) {
        return buildNode(id, settings).start();
    }


    @BeforeMethod
    public void startNodes() throws IOException {
        node = startNode("node1", EMPTY_SETTINGS);
        client = node.client();
    }

    @AfterMethod
    public void stopNodes() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
        if (node != null) {
            node.close();
            node = null;
        }
    }

    private String getTestDataDir() {
        ClassLoader classLoader = Classes.getDefaultClassLoader();
        URL data = classLoader.getResource("testdata/1.json");
        String testDir = data.getPath();
        File rootDir = new File(testDir).getParentFile();
        return rootDir.getPath();
    }

    protected void deleteIndex(String index) {
        try {
            client.admin().indices().delete(deleteIndexRequest(index)).actionGet();
        } catch (ElasticSearchException ex) {
            // Ignore
        }
    }

    protected void createIndex(String index, Settings settings) throws IOException {
        logger.info("Creating index test");
        client.admin().indices().create(createIndexRequest(index)
                .settings(settings)
                .mapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type")
                        .startObject("_source")
                        .field("provider", "test")
                        .endObject()
                        .startObject("properties")
                        .startObject("body").field("type", "string").field("store", "no").endObject()
                        .endObject()
                        .endObject().endObject().string())).actionGet();
    }

    @Test
    public void testIndexingAndRetrieval() throws Exception {
        deleteIndex("test");
        createIndex("test", settingsBuilder()
                .put("index.number_of_replicas", 0)
                .put("index.source.provider.test.type", "file")
                .put("index.source.provider.test.root_path", getTestDataDir())
                .put("index.source.provider.test.path_field", "file_path")
                .build()
        );


        for (int i = 1; i < 4; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i)).setSource(
                    XContentFactory.jsonBuilder().startObject()
                            .field("_id", i)
                            .field("file_path", i + ".json")
                            .field("body", "Test " + i)
                            .endObject().string()
            ).execute().actionGet();
        }


        ClusterHealthResponse clusterHealth = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealth.timedOut(), equalTo(false));
        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery()).execute().actionGet();
        SearchHits hits = searchResponse.hits();
        assertThat(hits.totalHits(), equalTo(3l));
        for (int i = 0; i < 3; i++) {
            assertThat(hits.getAt(i).sourceAsString(), containsString("\"body\":\"Test " + hits.getAt(i).id() + "\""));
        }

        searchResponse = client.prepareSearch().setQuery(queryString("body:\"Test 2\"")).execute().actionGet();
        hits = searchResponse.hits();
        assertThat(hits.totalHits(), equalTo(1l));
        assertThat(hits.getAt(0).sourceAsString(), containsString("\"body\":\"Test 2\""));
    }

    @Test
    public void testDefaultSettings() throws Exception {
        deleteIndex("test");

        logger.info("Creating index test");
        client.admin().indices().create(createIndexRequest("test")
                .settings(settingsBuilder()
                        .put("index.number_of_replicas", 0)
                        .build())
                .mapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type")
                        .startObject("_source")
                        .field("provider", "file")
                        .field("root_path", getTestDataDir())
                        .endObject()
                        .startObject("properties")
                        .startObject("body").field("type", "string").field("store", "no").endObject()
                        .endObject()
                        .endObject().endObject().string())).actionGet();


        for (int i = 1; i < 4; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i)).setSource(
                    XContentFactory.jsonBuilder().startObject()
                            .field("_id", i)
                            .field("path", i + ".json")
                            .field("body", "Test " + i)
                            .endObject().string()
            ).execute().actionGet();
        }

        ClusterHealthResponse clusterHealth = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealth.timedOut(), equalTo(false));
        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery()).execute().actionGet();
        SearchHits hits = searchResponse.hits();
        assertThat(hits.totalHits(), equalTo(3l));
        for (int i = 0; i < 3; i++) {
            assertThat(hits.getAt(i).sourceAsString(), containsString("\"body\":\"Test " + hits.getAt(i).id() + "\""));
        }
    }
}
