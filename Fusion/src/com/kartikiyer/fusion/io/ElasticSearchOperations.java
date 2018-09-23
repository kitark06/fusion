package com.kartikiyer.fusion.io;


import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;


public class ElasticSearchOperations implements AutoCloseable
{
	HttpHost[]		hosts;
	RestHighLevelClient	client;

	public ElasticSearchOperations(HttpHost[] hosts)
	{
		this.hosts = hosts;
		this.client = new RestHighLevelClient(RestClient.builder(hosts));
	}


	public SearchResponse performSearch(String indexName, String mappingType, QueryBuilder query) throws IOException
	{
		SearchRequest searchRequest = new SearchRequest(indexName).types(mappingType);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query);
		searchRequest.source(searchSourceBuilder);
		return client.search(searchRequest);
	}

	public BulkResponse performBulkInsert(BulkRequest requests) throws IOException
	{
		requests.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
		return client.bulk(requests);
	}


	@Override
	public void close() throws IOException
	{
		if (client != null)
			client.close();
	}
}
