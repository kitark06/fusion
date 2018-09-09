package com.kartikiyer.fusion.map;


import java.io.IOException;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import com.kartikiyer.fusion.io.ElasticSearchOperations;


public class ElasticSearchBulkIndexMapper extends RichAllWindowFunction<String, BulkItemResponse, GlobalWindow>
{
	private String	index;
	private String	type;
	private String	elasticSearchClusterIp;
	private int	elasticSearchClusterPort;

	public ElasticSearchBulkIndexMapper(String index)
	{
		super();
		this.index = index;
	}

	@Override
	public void open(Configuration config) throws Exception
	{
		super.open(config);
		ParameterTool parameters = (ParameterTool) getRuntimeContext()	.getExecutionConfig()
															.getGlobalJobParameters();
		this.type = parameters.get("mappingType");
		this.elasticSearchClusterIp = parameters.get("elasticSearchClusterIp");
		this.elasticSearchClusterPort = parameters.getInt("elasticSearchClusterPort");
	}

	@Override
	public void apply(GlobalWindow window, Iterable<String> values, Collector<BulkItemResponse> out)
	{
		HttpHost[] hosts = new HttpHost[] { new HttpHost(elasticSearchClusterIp, elasticSearchClusterPort) };
		ElasticSearchOperations esOps = new ElasticSearchOperations(hosts);
		BulkRequest requests = new BulkRequest();
		values.forEach(json ->
		{
			IndexRequest request = new IndexRequest(index, type);
			request.source(json,XContentType.JSON);
//			request.
			requests.add(request);
		});

		System.out.println("$$$$$$$$$$$ " + requests.numberOfActions());

		BulkResponse bulkResponse;
		try
		{
			bulkResponse = esOps.performBulkInsert(requests);
			if (bulkResponse.hasFailures())
				bulkResponse.forEach(response ->
				{
					if (response.isFailed())
						out.collect(response);
				});
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

