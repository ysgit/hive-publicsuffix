package br.com.ingenieux.hive.udf;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class TestURLNormalizeHost {

	private ArrayList<Text> getExpected(String tld, String suffix){
		ArrayList<Text> res = new ArrayList<Text>();
		res.add(new Text(tld));
		res.add(new Text(suffix));
		return res;
	}

	@Test
	public void testFacebookCom() throws Exception {
		URLNormalizeHost udf = new URLNormalizeHost();
		
		ArrayList<Text> result = udf.evaluate(new Text("www.facebook.com"));
		
		Assert.assertEquals(getExpected("facebook","com"), result);
	}

	@Test
	public void testFacebookCoUk() throws Exception {
		URLNormalizeHost udf = new URLNormalizeHost();

		ArrayList<Text> result = udf.evaluate(new Text("www.facebook.co.uk"));

		Assert.assertEquals(getExpected("facebook","co.uk"), result);
	}

	@Test
	public void testMultipleStartsAndFinishes() throws Exception {
		URLNormalizeHost udf = new URLNormalizeHost();

		ArrayList<Text> result = udf.evaluate(new Text("12121.wrg.5y.yossisynett.co.jp"));

		Assert.assertEquals(getExpected("yossisynett","co.jp"), result);
	}

	@Test
	public void testComMx() throws Exception {
		URLNormalizeHost udf = new URLNormalizeHost();

		ArrayList<Text> result = udf.evaluate(new Text("inet.www.192.168.2.12.normme.com.mx"));

		Assert.assertEquals(getExpected("normme","com.mx"), result);
	}


}
