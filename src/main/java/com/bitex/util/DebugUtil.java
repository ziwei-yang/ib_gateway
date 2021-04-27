package com.bitex.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

public class DebugUtil {
	static final SimpleDateFormat TIME_HEADER = new SimpleDateFormat("MM/dd-HH:mm:ss.S");
	static int MAX_HEAD_LEN = 20;
	public static void log(Object o) {
		String head = null;
		StackTraceElement[] stacks = Thread.currentThread().getStackTrace();
		for (StackTraceElement ste : stacks) {
			if (ste.getClassName().equals(DebugUtil.class.getCanonicalName()))
				continue;
			if (ste.getClassName().contains("java."))
				continue;
			head = ste.getClassName() + ":" + ste.getLineNumber();
			break;
		}
		
		if (head.length() > MAX_HEAD_LEN)
			MAX_HEAD_LEN = head.length();
		if (MAX_HEAD_LEN >= 30) MAX_HEAD_LEN = 30;
		head = StringUtils.rightPad(head, MAX_HEAD_LEN);
		if (head.length() > MAX_HEAD_LEN)
			head = head.substring(head.length()-MAX_HEAD_LEN);
		String time_str = TIME_HEADER.format(new Date());
		System.out.println(time_str + " " + head + " " + o.toString());
	}
	public static void printStackTrace() {
		printStackInfo();
	}
	public static void printStackInfo() {
		printStackInfo(8);
	}
	public static void printStackTrace(int maxLevel) {
		printStackInfo(maxLevel);
	}
	public static void printStackInfo(int maxLevel) {
		System.out.println(stackInfo(maxLevel));
	}
	public static String stackInfo() {
		return stackInfo(8);
	}
	public static String stackInfo(int maxLevel) {
		return stackInfo(maxLevel, true);
	}
	public static String stackInfo(int maxLevel, boolean withNewLine) {
		StackTraceElement[] stacks = Thread.currentThread().getStackTrace();
		StringBuilder sb = new StringBuilder();
		int count = 0;
		for (int i = 1; i < stacks.length && count < maxLevel; i++)
			if (stacks[i].getClassName().equals(DebugUtil.class.getCanonicalName()))
				continue;
			else {
				if (maxLevel > 1)
					sb.append("\t[" + (count++) + "] ");
				sb.append(stacks[i].getClassName() + '.' + stacks[i].getMethodName() + "():" + stacks[i].getLineNumber());
				if (withNewLine)
					sb.append('\n');
			}
		sb.append(new Date());
		return sb.toString();
	}
	public static void sleep(long t) {
		if (t <= 0) return;
		try {
			Thread.sleep(t);
		} catch (Exception e) {}
	}
}
