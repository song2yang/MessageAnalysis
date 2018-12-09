package com.sou.MessageAnalysis;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Messagetest {

	@Test
	public void test1() {
		Map<String, Map> rules = new HashMap<>();
		Map list1 = new HashMap();
		Map list2 = new HashMap();
		rules.put("1",list1);
		rules.put("1",list2);
		System.out.println(111);
	}
}
