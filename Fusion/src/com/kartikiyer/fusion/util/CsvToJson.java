package com.kartikiyer.fusion.util;


import static com.kartikiyer.fusion.util.ProjectFusionConstants.*;


/**
 * CsvToJson is a class which converts a delimited line to json.
 * It needs to be initialized with a String array of headers which will be used to generate the JSON attributes.
 * <p>
 * Returns CsvToJson.MALFORMED_DATA if there is a mismatch between the number of headers supplied and the columns in the delimited line
 *
 * @author Kartik Iyer
 */
public class CsvToJson
{
	public static final String	MALFORMED_DATA	= "-1";
	String[]					headers;


	/**
	 * Instantiates a CsvToJson is a class which converts a delimited line to json.
	 * It needs to be initialized with a String array of headers which will be used to generate the JSON attributes.
	 * <p>
	 * Returns CsvToJson.MALFORMED_DATA if there is a mismatch between the number of headers supplied and the columns in the delimited line
	 *
	 * @param headers
	 *             the headers
	 *
	 * @author Kartik Iyer
	 */
	public CsvToJson(String[] headers)
	{
		this.headers = headers;
	}

	/**
	 * To json.
	 *
	 * @param line
	 *             the line
	 * @param primaryKey
	 *             the primary key
	 * @return the string
	 */
	public String toJson(String line, String primaryKey)
	{
		String[] data = line.split(COLUMN_DELIMITER);

		if (data.length < headers.length)
			return MALFORMED_DATA; // return -1 to indicate malformed records & continue;
		else
		{
			StringBuilder json = new StringBuilder();
			json.append("{");

			if (primaryKey.isEmpty() == false)
			{
				json.append("\"" + ENRICHED_DATAMODEL_PK + "\"").append(":").append("\"" + primaryKey + "\"").append(",");
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

	/**
	 * To json.
	 *
	 * @param line
	 *             the line
	 * @return the string
	 */
	public String toJson(String line)
	{
		return toJson(line, "");
	}
}
