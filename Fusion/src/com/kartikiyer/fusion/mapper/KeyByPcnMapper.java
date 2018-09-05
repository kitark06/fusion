package com.kartikiyer.fusion.mapper;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class KeyByPcnMapper extends RichMapFunction<String, Tuple2<String, String>>
{
	Gson gson;

	@Override
	public void open(Configuration parameters) throws Exception
	{
		super.open(parameters);
		gson = new Gson();
	}

	@Override
	public Tuple2<String, String> map(String value)
	{
		System.out.println(value);
		String key;
		key = gson.fromJson(value, JsonObject.class).get("pcn").getAsString();
		return new Tuple2<String, String>(key, value);
	}
}
