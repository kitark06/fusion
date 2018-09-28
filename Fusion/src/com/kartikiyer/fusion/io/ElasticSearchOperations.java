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


/**
 * The Class ElasticSearchOperations is a Database Access Object.
 * Being a wrapper class for the ElasticSearch Rest High level client, this class contains methods designed with the aim of reducing boilerplate code.
 *
 * @author Kartik Iyer
 */
public class ElasticSearchOperations implements AutoCloseable
{
	HttpHost[]		hosts;
	RestHighLevelClient	client;

	/**
	 * Instantiates a new elastic search operations.
	 *
	 * @param hosts
	 *             An array of HttpHost which contain the ip address and port of all the nodes of the ES cluster.
	 */
	public ElasticSearchOperations(HttpHost[] hosts)
	{
		this.hosts = hosts;
		this.client = new RestHighLevelClient(RestClient.builder(hosts));
	}


	/**
	 * Concise method for performing a search operation on ES. Returns the search response object.
	 *
	 * @param indexName
	 *             the name of the index to be searched
	 * @param mappingType
	 *             the mapping type which is recommended to be set to "_doc"
	 * @param query
	 *             Accepts a QueryBuilder parameter which has info like the search type (term, boolean etc)
	 *             along with the attribute & the value to be searched for that attribute.
	 * @return The search response
	 * @throws IOException
	 *              Signals that an I/O exception has occurred.
	 */
	public SearchResponse performSearch(String indexName, String mappingType, QueryBuilder query) throws IOException
	{
		SearchRequest searchRequest = new SearchRequest(indexName).types(mappingType);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query);
		searchRequest.source(searchSourceBuilder);
		return client.search(searchRequest);
	}

	/**
	 * Performs bulk insert against a particular ES cluster. Works Synchronously
	 * <p>
	 * NOTE
	 * <br>
	 * This method will WAIT_UNTIL the bulk insert batch is indexed & refreshed
	 * i.e. it will NOT return till the data is available for search.
	 *
	 * @param requests
	 *             A BulkRequest object containing all the requests grouped together
	 * @return A BulkResponse object which can be iterated to check for individual responses, id of the inserts or for failures if BulkResponse.hasFailures
	 * @throws IOException
	 *              Signals that an I/O exception has occurred.
	 *              <br>
	 * 			Mostly will be due to an {@link org.elasticsearch.ElasticsearchException ElasticsearchException}
	 */
	public BulkResponse performBulkInsert(BulkRequest requests) throws IOException
	{
		requests.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
		return client.bulk(requests);
	}


	/* (non-Javadoc)
	 * @see java.lang.AutoCloseable#close()
	 */
	@Override
	public void close() throws IOException
	{
		if (client != null)
			client.close();
	}
}
