package com.bwise.eUniverse;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class util {
	protected static final Logger parentLogger = LogManager.getLogger();
	
	public static Map<String, Properties> parseINI(Reader reader) throws IOException {
	    Map<String, Properties> result = new HashMap<String, Properties>();
	    new Properties() {

	        /**
			 * 
			 */
			private static final long serialVersionUID = -5486271072783041551L;
			private Properties section;

	        @Override
	        public Object put(Object key, Object value) {
	            String header = (((String) key) + " " + value).trim();
	            if (header.startsWith("[") && header.endsWith("]"))
	                return result.put(header.substring(1, header.length() - 1), 
	                        section = new Properties());
	            else
	                return section.put(key, value);
	        }

	    }.load(reader);
	    return result;
	}
	
	public static String removeCharAt(String s, int pos) {
		return s.substring(0, pos) + s.substring(pos + 1);
	}

	public static String makeName(String name, String prefix) {
		 int maxLength  = Main.config.namelength;
		String result = name;
		if (prefix != null && prefix.length() > 0 && name.endsWith(prefix)) {
			result = name.substring(0, name.length() - prefix.length());
		}

		int length = maxLength - prefix.length();
		if (result.length() > length) {
			String compare = result.replaceAll("[AEIOUaeiou]", ".");
			int key = compare.lastIndexOf(".", compare.length() - 1);
			while (result.length() > length && key != -1) {

				result = removeCharAt(result, key);
				key = compare.lastIndexOf(".", key - 1);
			}
		}
		parentLogger.log(Level.TRACE,"old name=" + name + " & new="+result);
		return result.length() <= Main.config.namelength ? result.concat(prefix) : result.concat(prefix).substring(0,Main.config.namelength);
	}
	
	public static int costOfSubstitution(char a, char b) {
		return a == b ? 0 : 1;
	}

	public static int min(int... numbers) {
		return Arrays.stream(numbers).min().orElse(Integer.MAX_VALUE);
	}

	static Map.Entry<String, String> findclosest(String tablename) {

		int bestvalue = 100000000;
		//String name = "";
		Map.Entry<String, String> result = null;

		if (Main.listalltables.containsKey(tablename)) {
			parentLogger.log(Level.TRACE,"found exact name for " + tablename );

			return new java.util.AbstractMap.SimpleEntry<String, String>(tablename, Main.listalltables.get(tablename));
		}

		if (tablename.length() < 30)
			return null;
		for (Map.Entry<String, String> entry : Main.listalltables.entrySet()) {
			int value = util.calculate(tablename, entry.getKey());
			if (value < bestvalue) {
				bestvalue = value;
				//name = entry.getKey();
				result = entry;
			}

		}
		if (result != null) {

			parentLogger.log(Level.DEBUG,"found closest name for " + tablename + " = " + result.getKey() + " with value = " + bestvalue);
			return result;
		}
		return null;
	}

	static int calculate(String x, String y) {
		int[][] dp = new int[x.length() + 1][y.length() + 1];

		for (int i = 0; i <= x.length(); i++) {
			for (int j = 0; j <= y.length(); j++) {
				if (i == 0) {
					dp[i][j] = j;
				} else if (j == 0) {
					dp[i][j] = i;
				} else {
					dp[i][j] = min(dp[i - 1][j - 1] + costOfSubstitution(x.charAt(i - 1), y.charAt(j - 1)),
							dp[i - 1][j] + 1, dp[i][j - 1] + 1);
				}
			}
		}

		return dp[x.length()][y.length()];
	}

}
