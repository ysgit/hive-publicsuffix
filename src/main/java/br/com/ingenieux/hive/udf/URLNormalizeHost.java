package br.com.ingenieux.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

import br.com.ingenieux.hive.udf.util.PublicListService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@UDFType(deterministic = true)
@Description(name = "normalize_host", value = "_FUNC_(str) - Gets a normalized Host")
public class URLNormalizeHost extends UDF {
	private static PublicListService PUBLIC_LIST_SERVICE;

	static {
		try {
			PUBLIC_LIST_SERVICE = new PublicListService();
		} catch (Exception exc) {
			throw new RuntimeException(exc);
		}
	}

	public ArrayList<Text> evaluate(Text input) {
		if (null != input) {
			try {
				String domain = PUBLIC_LIST_SERVICE.getDomainFor(input
						.toString());

				if (null != domain){
					ArrayList<Text> result = new ArrayList<Text>();
					List<String> domainSplit = Arrays.asList(domain.split("\\."));
					result.add(new Text(domainSplit.get(0)));
					result.add(new Text(StringUtils.join(domainSplit.subList(1, domainSplit.size()), ".")));
					return result;
				}
			} catch (Exception ignored) {

			}
		}
		return null;
	}
}
