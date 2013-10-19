package br.com.ingenieux.hive.udf;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class TestURLNormalizeHost {
	@Test
	public void testSomething() throws Exception {
		URLNormalizeHost udf = new URLNormalizeHost();
		
		Text result = udf.evaluate(new Text("www.facebook.com"));
		
		Assert.assertEquals(result.toString(), "com.facebook");
	}

}
