package com.kartikiyer.fusion.mapper;


import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.http.HttpHost;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.kartikiyer.fusion.io.ElasticSearchOperations;
import com.kartikiyer.fusion.model.BillingCost;
import com.kartikiyer.fusion.model.EnrichedData;
import com.kartikiyer.fusion.model.InsuranceDetails;
import com.kartikiyer.fusion.model.MedicineOrders;
import com.kartikiyer.fusion.model.PatientInfo;
import com.kartikiyer.fusion.model.Treatment;


public class DataEnrichmentMapper extends RichMapFunction<String, String>
{

	private HttpHost[]	hosts;
	private String		mappingType;
	private String		patientInfoIndexName;

	Map<String, String>	dataEnrichmentIndexClassPairs;

	@Override
	public void open(Configuration parameters) throws Exception
	{
		super.open(parameters);
		this.mappingType = parameters.getString("mappingType", "_doc");

		this.patientInfoIndexName = parameters.getString("patientInfoIndexName", "patientInfo");

		dataEnrichmentIndexClassPairs = new LinkedHashMap<>(); // Using Linked collection variant is VERY important to finalize column order (decided by the order we insert) of the emitted data.
		dataEnrichmentIndexClassPairs.put(parameters.getString("treatmentIndexName", "treatment"), Treatment.class.getName());
		dataEnrichmentIndexClassPairs.put(parameters.getString("medicineOrdersIndexName", "medicineOrders"), MedicineOrders.class.getName());
		dataEnrichmentIndexClassPairs.put(parameters.getString("billingCostIndexName", "billingCost"), BillingCost.class.getName());
		dataEnrichmentIndexClassPairs.put(parameters.getString("insuranceDetailsIndexName", "insuranceDetails"), InsuranceDetails.class.getName());

		String elasticSearchClusterIp = parameters.getString("elasticSearchClusterIp", "localhost");
		int elasticSearchClusterPort = parameters.getInteger("elasticSearchClusterPort", 9200);
		this.hosts[0] = new HttpHost(elasticSearchClusterIp, elasticSearchClusterPort);
	}

	@Override
	public String map(String pcn)/* throws Exception*/
	{
		ElasticSearchOperations esHelper = new ElasticSearchOperations(hosts);
		QueryBuilder query = QueryBuilders.termQuery("pcn", pcn);


		ExclusionStrategy skipPcn = new ExclusionStrategy()
		{
			@Override
			public boolean shouldSkipField(FieldAttributes field)
			{
				return field.getName().equalsIgnoreCase("pcn");
			}

			@Override
			public boolean shouldSkipClass(Class<?> clazz)
			{
				return false;
			}
		};

		Gson gson = new GsonBuilder().setDateFormat("dd-mm-yyyy").setExclusionStrategies(skipPcn).create(); // Date eg :: 07-14-2015

		EnrichedData enrichedData = new EnrichedData();
		enrichedData.setPcn(pcn);
		Class<?> clazz = enrichedData.getClass();
		Map<Class, String> fields = new HashMap<>();
		
		for (int i = 0; i < clazz.getDeclaredFields().length; i++)
		{
			Field field = clazz.getDeclaredFields()[i];
			fields.put(field.getType(), clazz.getDeclaredFields()[i].getName());
		}
		
		dataEnrichmentIndexClassPairs.forEach((indexName, classFQName) -> {
			try
			{
				SearchResponse response = esHelper.performSearch(indexName, mappingType, query);
				String result = response.getHits().getHits()[0].getSourceAsString();
				Object obj = gson.fromJson(result, Class.forName(classFQName));
				Field field = enrichedData.getClass().getField(fields.get(Class.forName(classFQName)));
				field.setAccessible(true);
				field.set(enrichedData, obj);
			}
			catch (IOException e)
			{
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
			QueryBuilder getPatientQuery = QueryBuilders.termQuery("patientID", enrichedData.getBillingCost().getPatientID());
			SearchResponse response = esHelper.performSearch(patientInfoIndexName, mappingType, getPatientQuery);
			String patientInfo = response.getHits().getHits()[0].getSourceAsString();
			enrichedData.setPatientInfo(gson.fromJson(patientInfo, PatientInfo.class));
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		return gson.toJson(enrichedData);
	}
}