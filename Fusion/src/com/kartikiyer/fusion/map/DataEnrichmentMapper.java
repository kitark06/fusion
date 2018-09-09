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
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

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


public class DataEnrichmentMapper extends RichMapFunction<Optional<String>, String> implements Serializable
{
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

		Gson gson = new GsonBuilder()	.setDateFormat(INCOMING_ES_RECORD_DF)
								.setExclusionStrategies(CommonUtilityMethods.excludeKeyDuringSerializer(ENRICHED_DATAMODEL_PK))
								.create(); // Date eg :: 07-14-2015

		EnrichedData enrichedData = new EnrichedData();
		enrichedData.setPcn(pcn);

		Map<Class, String> fields = CommonUtilityMethods.getFieldsOfDataModel(enrichedData);

		dataEnrichmentIndexClassPairs.forEach((indexName, classFQName) ->
		{
			try
			{
				SearchResponse response = esOps.performSearch(indexName, mappingType, query);

				String result = response	.getHits()
									.getHits()[0].getSourceAsString();

				Object obj = gson.fromJson(result, Class.forName(classFQName));

				Field field = enrichedData	.getClass()
										.getDeclaredField(fields.get(Class.forName(classFQName)));

				field.setAccessible(true);
				field.set(enrichedData, obj);
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


		return gson.toJson(enrichedData);
	}


	@Override
	public void close() throws Exception
	{
		super.close();
		esOps.close();
	}

	public static void main(String[] args) throws NoSuchFieldException, SecurityException, ClassNotFoundException, IllegalArgumentException, IllegalAccessException
	{
		Map<String, Class> dataEnrichmentIndexClassPairs = new HashMap<>();
		dataEnrichmentIndexClassPairs.put(TREATMENT, Treatment.class);


		ExclusionStrategy skipPcn = new ExclusionStrategy()
		{
			@Override
			public boolean shouldSkipField(FieldAttributes field)
			{
				return field	.getName()
							.equalsIgnoreCase(ENRICHED_DATAMODEL_PK);
			}

			@Override
			public boolean shouldSkipClass(Class<?> clazz)
			{
				return false;
			}
		};

		Gson gson = new GsonBuilder()	.setDateFormat(INCOMING_ES_RECORD_DF)
								.setExclusionStrategies(skipPcn)
								.create(); // Date eg :: 07-14-2015

		String result = "{\"pcn\":\"919\",\"icd\":\"C79.48\",\"treatmentDesc\":\"OTITIS MEDIA NOS\",\"visitDate\":\"30-06-2013\"}";

		EnrichedData enrichedData = new EnrichedData();
		Class classFQName = dataEnrichmentIndexClassPairs.get(TREATMENT);
		Object obj = gson.fromJson(result, (classFQName));

		Class<?> clazz = enrichedData.getClass();
		Map<Class, String> fields = new HashMap<>();

		for (int i = 0; i < clazz.getDeclaredFields().length; i++)
		{
			Field field = clazz.getDeclaredFields()[i];
			fields.put(field.getType(), clazz.getDeclaredFields()[i].getName());
		}

		Field field = enrichedData	.getClass()
								.getDeclaredField(fields.get((classFQName)));

		field.setAccessible(true);
		field.set(enrichedData, obj);

		System.out.println(enrichedData.toString());
	}
}