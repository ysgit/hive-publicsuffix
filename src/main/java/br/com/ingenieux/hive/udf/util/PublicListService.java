package br.com.ingenieux.hive.udf.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

public class PublicListService {
	Map<String, List<String>> tldList = new TreeMap<String, List<String>>();

	public PublicListService() throws Exception {
		load();
	}

	private void load() throws Exception {
		List<String> rules = IOUtils.readLines(getClass().getResourceAsStream(
				"public-tld-list.txt"));

		parseRules(rules);
	}

	private void parseRules(List<String> lines) {
		tldList.clear();
		ListIterator<String> listIterator = lines.listIterator();

		while (listIterator.hasNext()) {
			String line = listIterator.next().trim();
			if (StringUtils.isBlank(line) || line.startsWith("//"))
				continue;

			if (line.matches("^\\p{Alnum}.*")) {
				String revTld = getRevTld(line);

				tldList.put(revTld, new ArrayList<String>());
			} else if (line.matches("^\\*\\..*")) {
				List<String> elts = splitReverse(line);
				String key = StringUtils.join(
						elts.subList(0, -1 + elts.size()), ".");
				List<String> value = valueFor(key);

				value.add("*");
			} else if (line.matches("^!.+$")) {
				String subTld = line.substring(1);

				List<String> elts = splitReverse(subTld);
				String key = StringUtils.join(
						elts.subList(0, -1 + elts.size()), ".");

				List<String> value = valueFor(key);

				value.add(0, elts.get(-1 + elts.size()));
			}
		}
	}

	private List<String> valueFor(String key) {
		if (!tldList.containsKey(key))
			tldList.put(key, new ArrayList<String>());

		return tldList.get(key);
	}
	
	public String getReverseDomainFor(String hostname) {
		String result = getDomainFor(hostname);
		
		if (null != result)
			result = getRevTld(result);
		
		return result;
	}

	protected String getRevTld(String tld) {
		List<String> elts = splitReverse(tld);

		return StringUtils.join(elts, ".");
	}

	protected List<String> splitReverse(String tld) {
		List<String> elts = Arrays.asList(tld.split("\\."));

		Collections.reverse(elts);
		return elts;
	}

	public String getDomainFor(String hostname) {
		if (StringUtils.isBlank(hostname))
			throw new IllegalArgumentException("Invalid hostname");

		hostname = hostname.toLowerCase().trim()
				.replaceAll("^\\.*(.+)\\.*$", "$1");

		List<String> elts = splitReverse(hostname);

		String revHostname = getRevTld(hostname);

		if (tldList.containsKey(revHostname)
				&& (tldList.get(revHostname).isEmpty() || (tldList.get(
						revHostname).size() == 1 && tldList.get(revHostname)
						.contains("*"))))
			return null;

		for (int i = elts.size(); i != 0; i--) {
			List<String> rightHand = elts.subList(0, i);
			String left = i < elts.size() ? elts.get(i) : null;
			String key = StringUtils.join(rightHand, ".");

			if (tldList.containsKey(key)) {
				if (tldList.get(key).isEmpty()) {
					List<String> result = elts.subList(0,
							Math.min(1 + i, elts.size()));

					Collections.reverse(result);

					return StringUtils.join(result, ".");
				} else {
					List<String> rules = tldList.get(key);

					ListIterator<String> listIterator = rules.listIterator();
					String result = null;

					while (listIterator.hasNext()) {
						String nextRule = listIterator.next();
						if (nextRule.equals("*")) {
							int ruleLen = key.split("\\.").length;

							int upperIndex = Math.min(1 + i, elts.size());
							List<String> resultList = elts.subList(0,
									upperIndex);

							if (elts.size() == resultList.size()) {
								// result = null;
								return null;
							} else {
								if (elts.size() >= 2 + ruleLen)
									resultList = elts.subList(0, ruleLen + 2);

								result = StringUtils.join(resultList, ".");

								if (!tldList.containsKey(result)) {
									Collections.reverse(resultList);

									result = StringUtils.join(resultList, ".");
								} else {
									result = null;
								}
							}
						} else if (nextRule.equals(left)) {
							List<String> resultList = new ArrayList<String>(
									rightHand);

							resultList.add(left);

							Collections.reverse(resultList);

							return StringUtils.join(resultList, ".");
						}
					}

					if (null != result) {
						return result;
					}
				}
			}
		}

		return null;
	}
}
