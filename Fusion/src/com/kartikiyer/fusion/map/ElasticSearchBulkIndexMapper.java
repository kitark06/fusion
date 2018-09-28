package com.kartikiyer.fusion.map;


import java.io.IOException;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
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


/**
 * The ElasticSearchBulkIndexMapper gets a DataStream of elements which needs to be indexed onto ES index. 
 * It applies a window function which creates an IndexRequest, adds it to a BulkRequest and then does a bulk insert which doesnt return till the data is indexed and searchable.
 * 
 * <p>
 * It passes the output of the window function to {@link AckEsInsertMapper} , 
 * which writes the docID from the response to a topic which indicates that the data is ready to be searched.    
 *
 * @author Kartik Iyer
 */
public class ElasticSearchBulkIndexMapper extends RichAllWindowFunction<String, BulkItemResponse, TimeWindow>
{
	private static final long	serialVersionUID	= 1L;

	private static final Logger	log				= LoggerFactory.getLogger(ElasticSearchBulkIndexMapper.class);

	private String				index;
	private String				type;
	private String				elasticSearchClusterIp;
	private int				elasticSearchClusterPort;
	Gson						gson;

	/**
	 * Instantiates a new elastic search bulk index mapper which will the index the documents on this supplied index name.
	 * <br>
	 * Will create the index if not present under default ES cluster settings.
	 * 
	 * <p>NOTE : 
	 * <br>Please read the JavaDoc of {@link ElasticSearchBulkIndexMapper this} class to find more info.
	 *
	 * @param index the index name on which documents will be inserted
	 */
	public ElasticSearchBulkIndexMapper(String index)
	{
		super();
		this.index = index;
	}

	/* (non-Javadoc)
	 * @see org.apache.flink.api.common.functions.AbstractRichFunction#open(org.apache.flink.configuration.Configuration)
	 */
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

	/* (non-Javadoc)
	 * @see org.apache.flink.streaming.api.functions.windowing.AllWindowFunction#apply(org.apache.flink.streaming.api.windowing.windows.Window, java.lang.Iterable, org.apache.flink.util.Collector)
	 */
	@Override
	public void apply(TimeWindow window, Iterable<String> values, Collector<BulkItemResponse> out) throws Exception
	{

		HttpHost[] hosts = new HttpHost[] { new HttpHost(elasticSearchClusterIp, elasticSearchClusterPort) };
		try (ElasticSearchOperations esOps = new ElasticSearchOperations(hosts))
		{
			BulkRequest requests = new BulkRequest();
			values.forEach(json ->
			{
				String docID = gson.fromJson(json, JsonObject.class).get("pcn").getAsString();

				IndexRequest request = new IndexRequest(index, type, docID).source(json, XContentType.JSON);
				requests.add(request);
			});

			BulkResponse bulkResponse = esOps.performBulkInsert(requests);

			bulkResponse.forEach(response ->
			{
				if (response.isFailed())
				{
					// TODO Auto-generated catch block
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

