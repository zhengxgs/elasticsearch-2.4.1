package org.zhengxgs;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortParseElement;
import org.junit.Test;

/**
 * Created by zhengxgs on 2016/10/28.
 */
public class ElasticsearchClient {

	public static final String CLUSTER_NAME = "elasticsearch-2.4";
	public static final String HOST = "172.16.2.248";
	public static final Integer PORT = 9300;

	public static final String INDEX = "testindex";
	public static final String TYPE = "testindextype";

	private static Client client;

	private static Client getClient() {
		if (client != null) {
			return client;
		}
		ElasticsearchClientFactory factory = new ElasticsearchClientFactory();
		try {
			return client = factory.create(CLUSTER_NAME, HOST, PORT);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}

	// @Before
	@Test
	public void test_index_mapping() throws IOException {

		XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject(TYPE).field("include_in_all", "false")
				.startObject("_timestamp").field("enabled", true).endObject().startObject("properties").startObject("id").field("type", "integer")
				.field("store", "yes").field("null_value", 0).endObject().startObject("name").field("type", "string").field("store", "yes")
				.field("index", "not_analyzed").endObject().startObject("message").field("type", "string").field("store", "yes")
				.field("index", "not_analyzed").endObject().endObject().endObject().endObject();

		getClient().admin().indices().prepareCreate(INDEX).addMapping(TYPE, mapping).execute().actionGet();
	}

    @Test
    public void test_index_mapping2() throws IOException {

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject()
            .field("number_of_shards", "1").field("number_of_replicas", "0");
        xContentBuilder.startObject("analysis");
        xContentBuilder.startObject("analyzer").startObject("my_ngram_analyzer").field("tokenizer","my_ngram_tokenizer").endObject().endObject();
        xContentBuilder.startObject("tokenizer").startObject("my_ngram_tokenizer")
                .field("type", "nGram")
                .field("min_gram", "1")
                .field("max_gram", "1")
                .array("token_chars", "letter", "digit", "punctuation")
            .endObject()
        .endObject();
        xContentBuilder.endObject();
        xContentBuilder.endObject();

        System.out.println(xContentBuilder.string());
        getClient().admin().indices().prepareCreate("myindex1").setSettings(xContentBuilder.string()).get();

//        "analysis" : {
//            "analyzer" : {
//                "my_ngram_analyzer" : {
//                    "tokenizer" : "my_ngram_tokenizer"
//                }
//            },
//            "tokenizer" : {
//                "my_ngram_tokenizer" : {
//                        "type" : "nGram",
//                        "min_gram" : "1",
//                        "max_gram" : "1",
//                        "token_chars": [ "letter", "digit", "punctuation"]
//                }
//            }
//        }
    }

	// @After
	public void test_clear() {
		// DeleteResponse deleteResponse = getClient().prepareDelete(INDEX, TYPE, "1").execute().actionGet();
		DeleteIndexResponse deleteIndexResponse = getClient().admin().indices().prepareDelete(INDEX).execute().actionGet();
		System.out.println(deleteIndexResponse);
	}

	@Test
	public void test_index_for() throws IOException {

		long begin = System.currentTimeMillis();
		List<XContentBuilder> xContentBuilders = new ArrayList<>();
		for (int i = 0; i < 100000; i++) {
			XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("name", "zxg" + i)
					.field("message", "Trying out Elasticsearch").field("id", i + 1).endObject();
			xContentBuilders.add(builder);
		}

		BulkRequestBuilder bulkRequestBuilder = getClient().prepareBulk();
		for (XContentBuilder xContentBuilder : xContentBuilders) {
			bulkRequestBuilder.add(new IndexRequest(INDEX, TYPE).source(xContentBuilder));
		}

		BulkResponse responses = bulkRequestBuilder.execute().actionGet();
		if (responses.hasFailures()) {
			System.out.println("create index fail");
		}
		// IndexResponse response = getClient().prepareIndex(INDEX, TYPE).setSource(builder).execute().actionGet();

		long end = System.currentTimeMillis();
		System.out.printf("%10s=%10d/n", "for", System.currentTimeMillis() - begin);

		// index.codec = DEFLATE

		// time: 10471 10418 9760 10813

		// size: 5.56Mi (5.56Mi)
		// docs: 100,000 (100,000)

		// size: 9.40Mi (9.40Mi)
		// docs: 200,000 (200,000)

		// size: 14.9Mi (14.9Mi)
		// docs: 300,000 (300,000)

		// size: 18.7Mi (18.7Mi)
		// docs: 400,000 (400,000)

		// --------------------------------------

		// index.codec = LZ4

		// time : 9598 10369 10454

		// size: 6.06Mi (6.06Mi)
		// docs: 100,000 (100,000)

		// size: 12.2Mi (12.2Mi)
		// docs: 200,000 (200,000)

		// size: 16.2Mi (16.2Mi)
		// docs: 300,000 (300,000)
	}

	@Test
	public void test_index() throws IOException {
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("name", "zxg").field("message", "Trying out Elasticsearch")
				.field("id", 1).endObject();
		System.out.println(builder.string());

		IndexResponse response = getClient().prepareIndex(INDEX, TYPE).setSource(builder).execute().actionGet();
		// Index name
		String _index = response.getIndex();
		// Type name
		String _type = response.getType();
		// Document ID (generated or not)
		String _id = response.getId();
		// Version (if it's the first time you index this document, you will get: 1)
		long _version = response.getVersion();
		// isCreated() is true if the document is a new one, false if it has been updated
		boolean created = response.isCreated();
	}

	@Test
	public void test_search_queryAll() {
		SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch(INDEX).setTypes(TYPE);
		searchRequestBuilder.setFrom(0).setSize(10);
		searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());

		SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
		SearchHits hits = searchResponse.getHits();
		for (SearchHit hit : hits) {
			Map<String, Object> source = hit.getSource();
			System.out.println(source);
		}
	}

	@Test
	public void test_delete() {
		DeleteResponse deleteResponse = getClient().prepareDelete(INDEX, TYPE, "AVgK1n1KgojAb4MO1YUj").execute().actionGet();
		System.out.println(deleteResponse.isFound());
	}

	// delete by query 插件
	//	@Test
	//	public void test_deleteByQuery() {
	//		DeleteByQueryRequestBuilder sd = new DeleteByQueryRequestBuilder(getClient(), DeleteByQueryAction.INSTANCE);
	//		sd.setQuery(QueryBuilders.termQuery("name", "zxg18"));
	//		sd.setIndices(INDEX).setTypes(TYPE);
	//		DeleteByQueryResponse deleteByQueryResponse = sd.execute().actionGet();
	//		if (deleteByQueryResponse.getTotalFound() == deleteByQueryResponse.getTotalFound()) {
	//			System.out.println("删除成功");
	//		}
	//	}

	@Test
	public void test_update() throws Exception {
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index(INDEX);
		updateRequest.type(TYPE);
		updateRequest.id("AVgK1n1KgojAb4MO1YUr");
		updateRequest.doc(XContentFactory.jsonBuilder().startObject().field("name", "AVgK1n1KgojAb4MO1YUr").endObject());
		UpdateResponse updateResponse = getClient().update(updateRequest).actionGet();

		// XContentBuilder docBuilder = XContentFactory.jsonBuilder().startObject();
		// docBuilder.field("name", "aaa");
		// docBuilder.endObject();
		// System.out.println(docBuilder.string());
	}

	@Test
	public void test_queryAll() {

		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		boolQueryBuilder.must(QueryBuilders.wildcardQuery("name", "*zxg*"));

		BoolQueryBuilder subBoolQuery = QueryBuilders.boolQuery();
		subBoolQuery.should(QueryBuilders.termQuery("name", "zxg" + new Random().nextInt()));
		subBoolQuery.should(QueryBuilders.termQuery("name", "zxg" + new Random().nextInt()));
		// boolQueryBuilder.mustNot(subBoolQuery);

		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
		boolQuery.filter(subBoolQuery);
		boolQueryBuilder.mustNot(boolQuery);
		// boolQueryBuilder.filter(subBoolQuery);

		SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch(INDEX).setTypes(TYPE);
		searchRequestBuilder.setFrom(0).setSize(10000);
        // searchRequestBuilder.setFetchSource();
        searchRequestBuilder.setQuery(boolQueryBuilder);
		// searchRequestBuilder.setPostFilter(boolQueryBuilder);

		SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        System.out.println(searchResponse);
        SearchHits hits = searchResponse.getHits();
		System.out.println("查询总数：" + hits.getHits().length);
		for (SearchHit hit : hits) {
			Map<String, Object> source = hit.getSource();
			System.out.println(source);
		}
	}

	@Test
	public void test_queryAll_filter() {
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		boolQueryBuilder.must(QueryBuilders.termQuery("name", "zxg5"));
		boolQueryBuilder.filter(QueryBuilders.rangeQuery("id").from(0).to(200));

		SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch(INDEX).setTypes(TYPE);
		searchRequestBuilder.setFrom(0).setSize(100);
		searchRequestBuilder.setQuery(boolQueryBuilder);
		// searchRequestBuilder.setPostFilter(boolQueryBuilder);  // 查询后，才进行过滤。效率慢

		System.out.println(searchRequestBuilder);
		SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
		System.out.println(searchResponse);
		SearchHits hits = searchResponse.getHits();
		System.out.println("查询总数：" + hits.getHits().length);
		for (SearchHit hit : hits) {
			Map<String, Object> source = hit.getSource();
			System.out.println(source);
		}
	}

	/**
	 * 死循环查询
	 */
	@Test
	public void test_for_query() {
		while (true) {
			test_queryAll();
		}
	}

	@Test
	public void test_scroll() {

		MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
		SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch(INDEX).addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
				.setScroll(new TimeValue(60000)) // ms
				.setQuery(matchAllQueryBuilder).setSize(5); // 每个分片将返回100个hits

		SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
		//Scroll until no hits are returned
		while (true) {

			for (SearchHit hit : searchResponse.getHits().getHits()) {
				//Handle the hit...
				Map<String, Object> source = hit.getSource();
				System.out.println(source);
			}
			searchResponse = getClient().prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
			//Break condition: No hits are returned
			if (searchResponse.getHits().getHits().length == 0) {
				break;
			}
		}
	}

    @Test
	public void test_multi_match() {

        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("给伊拉克的是", "content");
        multiMatchQueryBuilder.slop(0);
        multiMatchQueryBuilder.maxExpansions(1);
        multiMatchQueryBuilder.analyzer("my_ngram_analyzer");
        multiMatchQueryBuilder.type(MultiMatchQueryBuilder.Type.PHRASE);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(multiMatchQueryBuilder);

        SearchRequestBuilder searchRequestBuilder = getClient().prepareSearch("fenci").setTypes("fulltext");
        searchRequestBuilder.setFrom(0).setSize(100);
        searchRequestBuilder.setQuery(boolQueryBuilder);

        System.out.println(searchRequestBuilder);
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        System.out.println(searchResponse);
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询总数：" + hits.getHits().length);
        for (SearchHit hit : hits) {
            Map<String, Object> source = hit.getSource();
            System.out.println(source);
        }
    }

}
