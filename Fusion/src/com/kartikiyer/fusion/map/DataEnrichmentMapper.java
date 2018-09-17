package com.kartikiyer.fusion.map;


import static com.kartikiyer.fusion.util.ProjectFusionConstants.*;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.kartikiyer.fusion.io.ElasticSearchOperations;
import com.kartikiyer.fusion.model.EnrichedData;
import com.kartikiyer.fusion.model.PatientInfo;
import com.kartikiyer.fusion.model.Treatment;
import com.kartikiyer.fusion.util.CommonUtilityMethods;

import org.slf4j.LoggerFactory;
import org.slf4j.spi.LocationAwareLogger;
import org.slf4j.Logger;


public class DataEnrichmentMapper extends RichMapFunction<Optional<String>, String> implements Serializable
{
	Logger						LOG	= LoggerFactory.getLogger(DataEnrichmentMapper.class);

	private String					mappingType;
	private String					patientInfoIndexName;
	private ElasticSearchOperations	esOps;
	Map<String, String>				dataEnrichmentIndexClassPairs;

	@Override
	public void open(Configuration config) throws Exception
	{
		super.open(config);

		ParameterTool parameters = (ParameterTool) getRuntimeContext()	.getExecutionConfig()
															.getGlobalJobParameters();

		this.mappingType = parameters.get("mappingType", ES_DEFAULT_INDEX_TYPE);
		this.patientInfoIndexName = parameters.get("patientInfoIndexName", PATIENT_INFO);

		String elasticSearchClusterIp = parameters.get("elasticSearchClusterIp", ELASTIC_SEARCH_CLUSTER_IP);
		int elasticSearchClusterPort = parameters.getInt("elasticSearchClusterPort", ELASTIC_SEARCH_CLUSTER_PORT);

		esOps = new ElasticSearchOperations(new HttpHost[] { new HttpHost(elasticSearchClusterIp, elasticSearchClusterPort) });

		dataEnrichmentIndexClassPairs = new HashMap<>();

		String topicList = parameters.get("dataEnrichmentStream");

		Arrays	.stream(topicList.split(ELEMENT_DELIM)) // get all key value pairs
				.forEach(element -> dataEnrichmentIndexClassPairs.put(element.split("\\" + KEY_VALUE_DELIM)[0], element.split("\\" + KEY_VALUE_DELIM)[1])); // split each key_val pair and add key & val present at index 0 & 1 to the map
	}

	@Override
	public String map(Optional<String> queryablePcn)/* throws Exception*/
	{
		String pcn = queryablePcn.get();
		QueryBuilder query = QueryBuilders.termQuery(ENRICHED_DATAMODEL_PK, pcn);

		Gson gson = new GsonBuilder()	.setDateFormat(INCOMING_ES_RECORD_DF) // Date eg :: 07-14-2015
								.setExclusionStrategies(CommonUtilityMethods.excludeKeyDuringSerializer(ENRICHED_DATAMODEL_PK))
								.create();

		EnrichedData enrichedData = new EnrichedData();
		enrichedData.setPcn(pcn);

		Map<Class, String> fields = CommonUtilityMethods.getFieldsOfDataModel(enrichedData);

		dataEnrichmentIndexClassPairs.forEach((indexName, classFQName) ->
		{
			try
			{
				SearchResponse response = esOps.performSearch(indexName, mappingType, query);

				SearchHits hits = response.getHits();
				LOG.debug(" pcn [{}]  indexName [{}]  hits [{}] ", pcn, indexName, hits.totalHits);

				// done to prevent ArrayIndexOutOfBounds : 0 in the unlikely chance that someone deleted the record from ES which ElasticsearchActivityStatefulMapper tracked as inserted
				if (hits.totalHits > 0)
				{
					String result = hits.getHits()[0].getSourceAsString();
					Object obj = gson.fromJson(result, Class.forName(classFQName));

					Field field = enrichedData	.getClass()
											.getDeclaredField(fields.get(Class.forName(classFQName)));

					field.setAccessible(true);
					field.set(enrichedData, obj);
				}
				else
					LOG.error("Record with pcn [{}] inserted in index [{}] was not found. It was probably deleted after being marked as Indexed by ElasticsearchActivityStatefulMapper.", pcn,
							indexName);
			}
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (JsonSyntaxException | ClassNotFoundException | NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});

		try
		{
			QueryBuilder getPatientQuery = QueryBuilders.termQuery("patientID", enrichedData.getBillingCost()
																			.getPatientID());

			SearchResponse response = esOps.performSearch(patientInfoIndexName, mappingType, getPatientQuery);

			String patientInfo = response	.getHits()
									.getHits()[0].getSourceAsString();

			enrichedData.setPatientInfo(gson.fromJson(patientInfo, PatientInfo.class));
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (NullPointerException e)
		{
			LOG.error(" ********************* " + enrichedData.getPcn());
			throw e;
		}


		return gson.toJson(enrichedData);
	}


	@Override
	public void close() throws Exception
	{
		super.close();
		esOps.close();
	}
}