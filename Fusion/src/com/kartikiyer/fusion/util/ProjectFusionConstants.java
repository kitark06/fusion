package com.kartikiyer.fusion.util;


public class ProjectFusionConstants
{
	public static final String	ENRICHED_DATAMODEL_PK				= "pcn";
	public static final String	KEY_VALUE_DELIM					= "|";
	public static final String	ELEMENT_DELIM						= "~";
	public static final int		FUSION_CORE_WINDOW_COUNT				= 4;

	// Flink Cluster details
	public static final int		FLINK_CLUSTER_PORT					= 6123;
	public static final String	FLINK_CLUSTER_IP					= "192.168.0.105";

	// Kafka Cluster details
	public static final String	KAFKA_CLUSTER_IP_PORT				= "192.168.0.104:9092";
	public static final String	FUSION_CONSUMER_GROUP				= "fusionConsumerGroup";

	// Kafka topic names
	public static final String	ACKNOWLEDGED_STREAM_SUFFIX			= "_committed";
	public static final String	PATIENTS_STREAM					= "patients-Stream";
	public static final String	BILLING_COST_FUSION_STREAM			= "billingCost-fusionStream";
	public static final String	INSURANCE_DETAILS_FUSION_STREAM		= "insuranceDetails-fusionStream";
	public static final String	MEDICINE_FUSION_STREAM				= "medicine-fusionStream";
	public static final String	TREATMENT_FUSION_STREAM				= "treatment-fusionStream";
	public static final String	COLUMN_DELIMITER					= "\\t";

	// Elastic Search Cluster details
	public static final String	ELASTIC_SEARCH_CLUSTER_IP			= "localhost";
	public static final int		ELASTIC_SEARCH_CLUSTER_PORT			= 9200;
	public static final String	ES_DEFAULT_INDEX_TYPE				= "_doc";
	public static final String	INCOMING_ES_RECORD_DF				= "dd-mm-yyyy";
	public static final int		ELASTICSEARCH_BULK_INSERT_WINDOW_COUNT	= 50;

	// Elastic Search indexes names
	public static final String	PATIENT_INFO						= "patient_info";
	public static final String	INSURANCE_DETAILS					= "insurance_details";
	public static final String	BILLING_COST						= "billing_cost";
	public static final String	MEDICINE_ORDERS					= "medicine_orders";
	public static final String	TREATMENT							= "treatment";

}
