package com.kartikiyer.fusion.map;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;


public class KeyByPcnMapper extends RichMapFunction<String, Tuple2<String, String>>
{
	private static final Logger	log	= LoggerFactory.getLogger(KeyByPcnMapper.class);

	Gson		gson;

	@Override
	public void open(Configuration parameters) throws Exception
	{
		super.open(parameters);
		gson = new Gson();
	}

	@Override
	public Tuple2<String, String> map(String value)
	{
		/*String key = null;
		try
		{
			key = gson.fromJson(value, JsonObject.class)
					.get("pcn")
					.getAsString();
			log.error("key [{}] -- value = [{}]",key,value);
		}
		catch (JsonSyntaxException e)
		{
			// TODO Auto-generated catch block
			log.error("key [{}] -- value = [{}]",key,value);
			e.printStackTrace();
		}*/

		return new Tuple2<String, String>(value, value);
	}
}
