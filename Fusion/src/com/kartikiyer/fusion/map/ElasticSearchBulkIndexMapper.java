package com.kartikiyer.fusion.map;


import java.io.IOException;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.kartikiyer.fusion.StreamingCore;
import com.kartikiyer.fusion.io.ElasticSearchOperations;


public class ElasticSearchBulkIndexMapper extends RichAllWindowFunction<String, BulkItemResponse, GlobalWindow>
{
	private static final Logger		log	= LoggerFactory.getLogger(ElasticSearchBulkIndexMapper.class);

	private String	index;
	private String	type;
	private String	elasticSearchClusterIp;
	private int	elasticSearchClusterPort;
	Gson			gson;

	public ElasticSearchBulkIndexMapper(String index)
	{
		super();
		this.index = index;
	}

	@Override
	public void open(Configuration config) throws Exception
	{
		super.open(config);
		ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
		this.type = parameters.get("mappingType");
		this.elasticSearchClusterIp = parameters.get("elasticSearchClusterIp");
		this.elasticSearchClusterPort = parameters.getInt("elasticSearchClusterPort");
		gson = new Gson();
	}

	@Override
	public void apply(GlobalWindow window, Iterable<String> values, Collector<BulkItemResponse> out)
	{
		HttpHost[] hosts = new HttpHost[] { new HttpHost(elasticSearchClusterIp, elasticSearchClusterPort) };
		ElasticSearchOperations esOps = new ElasticSearchOperations(hosts);
		BulkRequest requests = new BulkRequest();
		values.forEach(json ->
		{
			String docID = gson.fromJson(json, JsonObject.class).get("pcn").getAsString();

			IndexRequest request = new IndexRequest(index, type, docID).source(json, XContentType.JSON);
//			request.
			requests.add(request);
		});

		BulkResponse bulkResponse;
		try
		{
			bulkResponse = esOps.performBulkInsert(requests);

			bulkResponse.forEach(response ->
			{
				if (response.isFailed())
				{
					log.error(response.getFailureMessage());
				}

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

