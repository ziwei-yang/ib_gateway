package com.bitex.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

public class DebugUtil {
	static final SimpleDateFormat TIME_HEADER = new SimpleDateFormat("MM/dd-HH:mm:ss.SSS");
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
	public static void err(Object o) {
		log(red(o));
	}
	public static void info(Object o) {
		log(blue(o));
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
	
	////////////////////////////////////////////////////////////////
	// CLI Colour control
	////////////////////////////////////////////////////////////////
	public static String reverse(Object o) { return ("\033[07m" + o.toString() + "\033[0m"); }
	
	public static String black  (Object o) { return ("\033[30m" + o.toString() + "\033[0m"); }
	public static String red    (Object o) { return ("\033[31m" + o.toString() + "\033[0m"); }
	public static String green  (Object o) { return ("\033[32m" + o.toString() + "\033[0m"); }
	public static String yellow (Object o) { return ("\033[33m" + o.toString() + "\033[0m"); }
	public static String blue   (Object o) { return ("\033[34m" + o.toString() + "\033[0m"); }
	public static String magenta(Object o) { return ("\033[35m" + o.toString() + "\033[0m"); }
	public static String cyan   (Object o) { return ("\033[36m" + o.toString() + "\033[0m"); }
	public static String white  (Object o) { return ("\033[37m" + o.toString() + "\033[0m"); }
	
	public static String l_black (Object o) { return ("\033[90m" + o.toString() + "\033[0m"); }
	public static String l_red   (Object o) { return ("\033[91m" + o.toString() + "\033[0m"); }
	public static String l_green (Object o) { return ("\033[92m" + o.toString() + "\033[0m"); }
	public static String l_yellow(Object o) { return ("\033[93m" + o.toString() + "\033[0m"); }
	public static String l_blue  (Object o) { return ("\033[94m" + o.toString() + "\033[0m"); }
	public static String l_magenta(Object o) { return ("\033[95m" + o.toString() + "\033[0m"); }
	public static String l_cyan  (Object o) { return ("\033[96m" + o.toString() + "\033[0m"); }
	public static String l_white (Object o) { return ("\033[97m" + o.toString() + "\033[0m"); }
	
	public static String on_black(Object o) { return ("\033[40m" + o.toString() + "\033[0m"); }
	public static String on_red  (Object o) { return ("\033[41m" + o.toString() + "\033[0m"); }
	public static String on_green(Object o) { return ("\033[42m" + o.toString() + "\033[0m"); }
	public static String on_yellow(Object o) { return ("\033[43m" + o.toString() + "\033[0m"); }
	public static String on_blue (Object o) { return ("\033[44m" + o.toString() + "\033[0m"); }
	public static String on_magenta(Object o) { return ("\033[45m" + o.toString() + "\033[0m"); }
	public static String on_cyan(Object o) { return ("\033[46m" + o.toString() + "\033[0m"); }
	public static String on_white(Object o) { return ("\033[47m" + o.toString() + "\033[0m"); }
}
