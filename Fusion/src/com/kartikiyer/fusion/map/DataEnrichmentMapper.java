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
import org.elasticsearch.ElasticsearchException;
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


/**
 * The RichMapFunction does the data enrichment by joining data from multiple databases or indexes.
 * The input to the map method is the docID or primary key of the document to be joined.
 * <p>
 * It receives its input from the {@link com.kartikiyer.fusion.map.ElasticsearchActivityStatefulMapper ElasticsearchActivityStatefulMapper} which,
 * tracks inserts done on the multiple ES indexes which will be used to enrich the data by performing a join based on the primary key.
 * Thus the input to the map method is guaranteed to be present & searchable in all the indexes from which the data is to be joined.
 *
 * @author Kartik Iyer
 */
public class DataEnrichmentMapper extends RichMapFunction<Optional<String>, String> implements Serializable
{
	private static final long		serialVersionUID	= -3829979724908918007L;

	private static final Logger		log				= LoggerFactory.getLogger(DataEnrichmentMapper.class);

	private String					mappingType;
	private String					patientInfoIndexName;
	private ElasticSearchOperations	esOps;
	Map<String, String>				dataEnrichmentIndexClassPairs;

	/* (non-Javadoc)
	 * @see org.apache.flink.api.common.functions.AbstractRichFunction#open(org.apache.flink.configuration.Configuration)
	 */
	@Override
	public void open(Configuration config) throws Exception
	{
		super.open(config);

		ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

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

	/* (non-Javadoc)
	 * @see org.apache.flink.api.common.functions.RichMapFunction#map(java.lang.Object)
	 */
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

		// Uses reflection to get the [Class object , fieldName] of all the fields of the class.
		Map<Class, String> fields = CommonUtilityMethods.getFieldsOfDataModel(enrichedData);

		dataEnrichmentIndexClassPairs.forEach((indexName, classFQName) ->
		{
			try
			{
				SearchResponse response = esOps.performSearch(indexName, mappingType, query);

				SearchHits hits = response.getHits();
				log.debug(" pcn [{}]  indexName [{}]  hits [{}] ", pcn, indexName, hits.totalHits);

				// done to prevent ArrayIndexOutOfBounds : 0 in the unlikely chance that someone deleted the record from ES .. after ElasticsearchActivityStatefulMapper marked it as inserted
				if (hits.totalHits > 0)
				{
					String result = hits.getHits()[0].getSourceAsString();

					// loading the class to be used for deserialization in runtime
					Class<?> clazz = Class.forName(classFQName);

					// using the loaded class for deserialization 
					Object obj = gson.fromJson(result, clazz);

					// 1. Using the Class object, getting the actual name of the field as declared in the class :
					// this is required in the next step to get the reference of the field
					String fieldName = fields.get(clazz);

					// 2. Getting the actual reference to the declared field using the name of the field
					Field field = enrichedData.getClass().getDeclaredField(fieldName);

					// 3. Setting the field accessible and then setting the value represented by obj on enrichedData.
					field.setAccessible(true);
					field.set(enrichedData, obj);
				}
				else
					log.error("Record with pcn [{}] inserted in index [{}] was not found. It was probably deleted after being marked as Indexed by ElasticsearchActivityStatefulMapper.", pcn,
							indexName);
			}
			catch (ElasticsearchException e)
			{
				// TODO: handle exception
				e.printStackTrace();
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
			String indexName = patientInfoIndexName;
			QueryBuilder fetchPatientQuery = QueryBuilders.termQuery("patientID", enrichedData.getBillingCost().getPatientID());
			SearchResponse response = esOps.performSearch(indexName, mappingType, fetchPatientQuery);
			SearchHits hits = response.getHits();

			if (hits.totalHits > 0)
			{
				String patientInfo = hits.getHits()[0].getSourceAsString();
				enrichedData.setPatientInfo(gson.fromJson(patientInfo, PatientInfo.class));
			}
			else
				log.error("Record with pcn [{}] inserted in index [{}] was not found. It was probably deleted after being marked as Indexed by ElasticsearchActivityStatefulMapper.", pcn,
						indexName);
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return gson.toJson(enrichedData);
	}


	/* (non-Javadoc)
	 * @see org.apache.flink.api.common.functions.AbstractRichFunction#close()
	 */
	@Override
	public void close() throws Exception
	{
		super.close();
		esOps.close();
	}
}