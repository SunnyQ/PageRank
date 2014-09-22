import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Created by Wei on 9/15/14.
 */
public class DataProcessMapper extends Mapper<Text, Text, Text, Text> {
	@Override
	// Input: SequenceFile
	// Output: TextOutputFormat  <null, nodeName + "\t" + pageRank + "\t" + links>
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		String nodeName = key.toString();
		String line = value.toString();
		String linkListStr = "";

		// Read outgoing links from json string.
		if (!nodeName.contains(",")) {
			linkListStr = getLinkList(line, context);
		} else {
			linkListStr = "";
		}

		context.write(new Text(nodeName), new Text(linkListStr));
	}
	private String getLinkListx(String str, Context context) throws IOException {
		JsonFactory jf = new JsonFactory();
		JsonParser jp = jf.createParser(str);
		String resultStr = "";


		ObjectMapper objectMapper = new ObjectMapper();
		Map<String, Object> tmp = (Map<String, Object>) objectMapper.readValue(jp, Object.class);
		tmp = (Map<String, Object>) tmp.get("content");

		if (tmp != null) {
			List<Map<String, Object>> linksTmp = (List<Map<String, Object>>) tmp.get("links");
			if (linksTmp != null) {
				for (Map<String, Object> linkTmp : linksTmp) {
					if ("a".equals(linkTmp.get("type"))) {
						String ss = (String)linkTmp.get("href");
						//System.out.println(ss);
						resultStr += ("," + ss);

						//context.getCounter(MyCounter.Counter).increment(1);
						//System.out.println(link);
					}
				}
			}
		}

		return resultStr.length() == 0 ? "" : resultStr.substring(1);
	}

	private String getLinkList1(String str, Context context) {
		String resultStr = "";
		boolean isTypeA = false;

		JsonParser jp;


		JsonFactory jf = new JsonFactory();

		try {
			Map<String, Integer> map = new HashMap<String, Integer>();

			jp = jf.createParser(str);
			jp.nextToken(); // Will return JsonToken.START_OBJECT.

			while (jp.nextToken() != JsonToken.END_OBJECT) {
				String fieldName = jp.getCurrentName();
				if ("content-type".equals(fieldName)){
					jp.nextToken(); // Skip END_OBJECT of http_headers
				} else if ("links".equals(fieldName)) {
					if (jp.nextToken() == JsonToken.END_ARRAY) {
						// this.isDangling = true;
						break;
					} else {
						do {
							if ("type".equals(jp.getCurrentName())) {
								jp.nextToken();

								if ("a".equals(jp.getText())) {
									isTypeA = true;
								} else {
									isTypeA = false;
								}
							} else if ("href".equals(jp.getCurrentName())) {
								jp.nextToken();
								String link = jp.getText();

								// && !map.containsKey(link)
								if (isTypeA) {
									// SKip duplicated links.
									resultStr += ("," + link);
									map.put(link, 1);
									context.getCounter(MyCounter.Counter).increment(1);
									System.out.println(link);
								}
							}
						} while (jp.nextToken() != JsonToken.END_ARRAY);
						break; // Finish parsing outgoing links.
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Strip the first ",".
		return resultStr.length() == 0 ? "" : resultStr.substring(1);
	}

	private String getLinkList(String str, Context context) {
		String resultStr = "";
		boolean isTypeA = false;

		JsonParser jp;


		JsonFactory jf = new JsonFactory();

		try {
			str = "{\"json\":" + str + "}";
			jp = jf.createParser(str);
			jp.nextToken(); // Skip JsonToken.START_OBJECT.
			jp.nextToken(); // Skip field name "json"
			jp.nextToken(); // Skip JsonToken.START_OBJECT.
			int cnt = 0;

			while (jp.nextToken() != JsonToken.END_OBJECT || !"json".equals(jp.getCurrentName())) {
				if ("content".equals(jp.getCurrentName())) {
					jp.nextToken(); // JacksonToken.START_OBJECT

					while (jp.nextToken() != JsonToken.END_OBJECT ||
							!"content".equals(jp.getCurrentName())) {

						if ("links".equals(jp.getCurrentName())) {
							jp.nextToken(); // JacksonToken.START_ARRAY

							while (jp.nextToken() != JsonToken.END_ARRAY) {
								if ("type".equals(jp.getCurrentName())) {
									if ("a".equals(jp.getText())) {
										isTypeA = true;
									}
								} else if ("href".equals(jp.getCurrentName())) {
									jp.nextToken();
									String link = jp.getText();

									// Filter out (discard) all the urls which contain comma
									if (isTypeA && link != null && !link.contains(",")) {
										resultStr += ("\t" + link);
									}
								} else if (jp.getCurrentToken() == JsonToken.END_OBJECT){
									isTypeA = false;
								}
							}
							break;
						}
					}
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Strip the first ",".
		return resultStr.length() == 0 ? "" : resultStr.substring(1);
	}
}
