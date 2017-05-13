package com.marlabs.bigdata.piiwatcher;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class PIIWatcher extends UDF{
	
	private static final String MASK = "XXX";
	private static final String AT_SIGN = "@";
	private static final String DASH = "-";
	
	public Text evaluate(String unprotectedString) {
		if (unprotectedString == null) {
			return new Text();
		}
		String[] segments = unprotectedString.split("[@\\-]");
		String lastSegment = segments[segments.length - 1];
		if (segments.length == 2){
			return new Text(mask(lastSegment, AT_SIGN));
		}
		else {
			return new Text(mask(lastSegment, DASH));
		}
	}
	
	private String mask(String lastSegment, String delimiter) {
		StringBuilder sb = new StringBuilder();
		int i = delimiter.equals("@") ? 1 : 2; 
		while (i > 0) {
			sb.append(MASK);
			sb.append(delimiter);
			i--;
		}
		sb.append(lastSegment);
		return sb.toString();
	}
	
}
