package com.sou.MessageAnalysis;

import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Messagetest {

	@Test
	public void test1() throws ParseException {
		String dateStr = "2017-1-1 12:24:07";
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String sendTime = sdf2.format(sdf2.parse(dateStr));
		System.out.println(sendTime);
	}
}
