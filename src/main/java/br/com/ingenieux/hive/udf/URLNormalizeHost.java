package br.com.ingenieux.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

import br.com.ingenieux.hive.udf.util.PublicListService;

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

	public Text evaluate(Text input) {
		if (null != input) {
			try {
				String result = PUBLIC_LIST_SERVICE.getReverseDomainFor(input
						.toString());

				if (null != result)
					return new Text(result);
			} catch (Exception exc) {

			}
		}
		return null;
	}
}
