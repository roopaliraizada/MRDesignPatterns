package summarization.util;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class MRDPUtils {

	public static final String[] REDIS_INSTANCES = { "p0", "p1", "p2", "p3",
			"p4", "p6" };
	public static File file;

	// This helper function parses the stackoverflow into a Map for us.
	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3)
					.split("\"");

			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];

				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
	}

	/*public static void main(String[] args) {
		FileInputStream fis = null;
		String line;
		file = new File("/Users/rraizada/Documents/eclipse_workspaces/MRP/NumericalSummarization/src/com/java/sample.xml");
		try {
			fis = new FileInputStream(file);

			StringBuilder sb = new StringBuilder();
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));

			while ((line = br.readLine()) != null) {
				sb.append(line);
				sb.append('\n');
			}

			
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(sb.toString());
			String date = parsed.get("CreationDate");
			String userId = parsed.get("UserId");
			System.out.println(date + ":::::::::" + userId);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}*/
}
