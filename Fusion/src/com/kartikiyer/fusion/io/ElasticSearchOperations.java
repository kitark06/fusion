package com.kartikiyer.fusion.io;


import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;


public class ElasticSearchOperations
{
	HttpHost[] hosts;

	public ElasticSearchOperations(HttpHost[] hosts)
	{
		this.hosts = hosts;
	}


	public SearchResponse performSearch(String indexName, String mappingType, QueryBuilder query) throws IOException
	{
		try (
			RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(hosts));)
		{
			SearchRequest searchRequest = new SearchRequest(indexName).types(mappingType);
			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query);
			searchRequest.source(searchSourceBuilder);
			return client.search(searchRequest);
		}

	}
}
