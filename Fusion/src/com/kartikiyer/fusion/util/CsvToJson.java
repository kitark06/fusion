package com.kartikiyer.fusion.util;


import static com.kartikiyer.fusion.util.ProjectFusionConstants.COLUMN_DELIMITER;


public class CsvToJson
{
	public static final String	MALFORMED_DATA	= "-1";
	String[]					headers;


	public CsvToJson(String[] headers)
	{
		this.headers = headers;
	}

	public String toJson(String line, String primaryKey)
	{
		String[] data = line.split(COLUMN_DELIMITER);

		if (data.length < headers.length)
			return MALFORMED_DATA; // keeping track of malformed records & continue;
		else
		{
			StringBuilder json = new StringBuilder();
			json.append("{");

			if (primaryKey.isEmpty() == false)
			{
				json.append("\"pcn\"").append(":").append("\"" + primaryKey + "\"").append(",");
			}

			for (int i = 0; i < headers.length; i++)
			{
				json.append("\"" + headers[i] + "\"").append(":").append("\"" + data[i] + "\"").append(",");
			}
			json.setLength(json.length() - 1);
			json.append("}");

			return json.toString();

		}
	}

	public String toJson(String line)
	{
		return toJson(line, "");
	}
}
