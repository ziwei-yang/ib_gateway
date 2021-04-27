package com.bitex.util;

import java.lang.reflect.*;
import java.util.*;

import com.alibaba.fastjson.*;

import static com.bitex.util.DebugUtil.*;

public class ReflectionUtil {
	private static final Class<?>[] PRIMITIVE_CLASS = { byte.class, boolean.class, short.class, int.class, long.class, float.class, double.class, char.class};
	private static final Class<?>[] WRAPPER_CLASS = { Byte.class, Boolean.class, Short.class, Integer.class, Long.class, Float.class, Double.class, Character.class};
	private static final HashMap<Class<?>, String> CLASS_CONVERT_METHODS = new HashMap<>();
	static {
		CLASS_CONVERT_METHODS.put(double.class, "doubleValue");
		CLASS_CONVERT_METHODS.put(Double.class, "doubleValue");
		CLASS_CONVERT_METHODS.put(float.class, "floatValue");
		CLASS_CONVERT_METHODS.put(Float.class, "floatValue");
		CLASS_CONVERT_METHODS.put(long.class, "longValue");
		CLASS_CONVERT_METHODS.put(Long.class, "longValue");
		CLASS_CONVERT_METHODS.put(int.class, "intValue");
		CLASS_CONVERT_METHODS.put(Integer.class, "intValue");
		CLASS_CONVERT_METHODS.put(boolean.class, "booleanValue");
		CLASS_CONVERT_METHODS.put(Boolean.class, "booleanValue");
	}

	public static Class<?> getWrapperClass(Class<?> clazz) {
		for (int i = 0; i < PRIMITIVE_CLASS.length; i++)
			if (PRIMITIVE_CLASS[i] == clazz)
				return WRAPPER_CLASS[i];
		return clazz;
	}

	public static Class<?> getPrimitiveClass(Class<?> clazz) {
		for (int i = 0; i < WRAPPER_CLASS.length; i++)
			if (WRAPPER_CLASS[i] == clazz)
				return PRIMITIVE_CLASS[i];
		return null;
	}

	public static Object convertType(Object o, Class<?> clazz) {
		String methodName = CLASS_CONVERT_METHODS.get(clazz);
		if (methodName == null) {
			log("Could not find convert map from " + o.getClass() + " to " + clazz);
			return o;
		}
		return invoke(o, methodName, new Object[0]);
	}

	public static final Method searchMethod(Class<?> clazz, String methodName, Class<?>... parameterTypes) throws NoSuchMethodException {
		if (clazz == null)
			return null;
		Class<?> targetClass = clazz;
		for (Method m : clazz.getMethods()) {
			if (m.getName().equals(methodName) == false)
				continue;
			Class<?>[] types = m.getParameterTypes();
			if (types.length != parameterTypes.length)
				continue;
			for (int i = 0; i < types.length; i++) {
				if (types[i] == parameterTypes[i])
					continue;
				if (types[i].isAssignableFrom(parameterTypes[i]))
					continue;
				if (getWrapperClass(types[i]).isAssignableFrom(getWrapperClass(parameterTypes[i])))
					continue;
			}
			return m;
		}
		return targetClass.getDeclaredMethod(methodName, parameterTypes);
	}

	/**
	 * @Deprecated Should be replaced by searchMethod
	 */
	public static final Method searchDeclaredMethod(Class<?> clazz, String methodName, Class<?>... parameterTypes) throws NoSuchMethodException, SecurityException {
		if (clazz == null)
			return null;
		Class<?> targetClass = clazz;
		while(true) {
			try {
				for (Method m : clazz.getDeclaredMethods()) {
					if (m.getName().equals(methodName) == false)
						continue;
					Class<?>[] types = m.getParameterTypes();
					if (types.length != parameterTypes.length)
						continue;
					for (int i = 0; i < types.length; i++) {
						if (types[i] == parameterTypes[i])
							continue;
						if (types[i].isAssignableFrom(parameterTypes[i]))
							continue;
						if (getWrapperClass(types[i]).isAssignableFrom(getWrapperClass(parameterTypes[i])))
							continue;
					}
					return m;
				}
				return targetClass.getDeclaredMethod(methodName, parameterTypes);
			} catch (NoSuchMethodException e) {
				// Search in parent class.
				targetClass = targetClass.getSuperclass();
				if (targetClass == null) {
					log("Could not find methods " + methodName + " for class: " + clazz + ", params: " + Arrays.asList(parameterTypes));
					try{
					for (Method m : clazz.getDeclaredMethods())
						log(m);
					} catch (Exception e2) {
						e2.printStackTrace();
					}
					throw e;
				}
				continue;
			} catch (Exception e) {
				throw e;
			}
		}
	}
	
	public static final Object executeByJSON(Object o, JSONObject json) {
		if (o == null || json == null)
			return o;
		for (Map.Entry<String, Object> entry: json.entrySet()) {
			String methodName = entry.getKey();
			// Value could be a JSONArray|String|Boolean|Integer|Float|Double|JSONObject
			Object value = entry.getValue();
			Object[] args = new Object[1];
			if (value instanceof JSONArray) 
				args = ((JSONArray)value).toArray();
			else
				args[0] = value;
			for (int i = 0; i < args.length; i++) {
				Object oldArg = args[i];
				if (args[i] instanceof JSONObject)
					args[i] = initializeByJSON((JSONObject)args[i]);
				if (args[i] == null) {
					log("Args [" + i + "] cannot be created for method " + methodName + ", original:" + oldArg);
					return o;
				}
			}
			Class<?>[] parameterTypes = new Class[args.length];
			for (int i = 0; i < args.length; i++)
				parameterTypes[i] = args[i].getClass();
			Class<?> clazz = o.getClass();
			try {
				Method m = searchMethod(clazz, methodName, parameterTypes);
				Class<?>[] finalArgClasses = m.getParameterTypes();
				// Argument type check.
				for (int i = 0; i < finalArgClasses.length; i++)
					if (!finalArgClasses[i].isAssignableFrom(args[i].getClass()))
						args[i] = convertType(args[i], finalArgClasses[i]);
				m.invoke(o, args);
			} catch (NoSuchMethodException|SecurityException|IllegalAccessException|IllegalArgumentException|InvocationTargetException e) {
				StringBuilder sb = new StringBuilder();
				for (Object a : args)
					sb.append(a.getClass().toString() + "\n");
				log("Can not invoke method [" + methodName + "] for " + clazz.getName() + " with args type:\n" + sb.toString());
				e.printStackTrace();
				return o;
			}
		}
		return o;
	}

	public static final Object initializeByJSON(JSONObject json) {
		String className = json.getString("class");
		String enumName = json.getString("enum");
		if (className != null) {
			// Create args.
			JSONArray argsJson = json.getJSONArray("args");
			Object[] args = new Object[argsJson.size()];
			for (int i = 0; i < args.length; i++) {
				args[i] = argsJson.get(i);
				if (args[i] instanceof JSONObject)
					args[i] = initializeByJSON((JSONObject)args[i]);
				if (args[i] == null) {
					log("Cannot create arg[" + i + "] " + args[i] + " of " + json.toJSONString() + ", abort creating for " + className);
					return null;
				}
			}
			Class<?>[] parameterTypes = new Class[args.length];
			for (int i = 0; i < args.length; i++)
				parameterTypes[i] = args[i].getClass();
			try {
				Class<?> clazz = Class.forName(className);
				Constructor<?> cons = clazz.getConstructor(parameterTypes);
				return cons.newInstance(args);
			} catch (ClassNotFoundException|InstantiationException|IllegalAccessException|NoSuchMethodException|InvocationTargetException e) {
				log("Instance cannot be created for " + className);
				e.printStackTrace();
				return null;
			}
		} else if (enumName != null) {
			String enumValue = json.getString("value");
			try {
				Class<?> clazz = Class.forName(enumName);
				if (clazz.isEnum() == false) {
					log("Class<?> " + enumName + " is not Enum, abort initialization.");
					return null;
				}
				Object[] enums = clazz.getEnumConstants();
				for (Object e : enums)
					if (e.toString().equals(enumValue))
						return e;
				return null;
			} catch (ClassNotFoundException e) {
				log("Instance cannot be created for " + enumName);
				e.printStackTrace();
				return null;
			}
		}
		return null;
	}

	public static final Object newInstance(Class<?> clazz, Object... args) {
		Class<?>[] parameterTypes = new Class[args.length];
		for (int i = 0; i < args.length; i++)
			parameterTypes[i] = args[i].getClass();
		try {
			Constructor<?> cons = clazz.getConstructor(parameterTypes);
			return cons.newInstance(args);
		} catch (InstantiationException|IllegalAccessException|NoSuchMethodException|InvocationTargetException e) {
			log("Instance cannot be created for " + clazz.getCanonicalName());
			e.printStackTrace();
			return null;
		}
	}
	public static final Object newInstance(String className, Object... args) {
		try {
			Class<?> clazz = Class.forName(className);
			return newInstance(clazz, args);
		} catch (ClassNotFoundException e) {
			log("Instance cannot be created for " + className);
			e.printStackTrace();
			return null;
		}
	}

	public static final Object invokeStatic(String className, String methodName, Object... args) {
		try {
			Class<?> clazz = Class.forName(className);
			return invokeStatic(clazz, methodName, args);
		} catch (ClassNotFoundException e) {
			log("Class<?> not found for " + className);
			e.printStackTrace();
			return null;
		}
	}

	public static final Object invokeStatic(Class<?> clazz, String methodName, Object... args) {
		Class<?>[] parameterTypes = new Class[args.length];
		for (int i = 0; i < args.length; i++)
			parameterTypes[i] = args[i].getClass();
		try {
			Method m = searchMethod(clazz, methodName, parameterTypes);
			if (m == null) {
				log("Can not find method [" + methodName + "] for " + clazz.getName());
				return null;
			}
			return m.invoke(null, args);
		} catch (NoSuchMethodException|SecurityException|IllegalAccessException|IllegalArgumentException|InvocationTargetException e) {
			log("Can not invoke method [" + methodName + "] for " + clazz.getName());
			e.printStackTrace();
			return null;
		}
	}

	public static final Object invoke(Object o, String methodName, Object... args) {
		if (o == null)
			return null;
		Class<?>[] parameterTypes = new Class[args.length];
		for (int i = 0; i < args.length; i++)
			parameterTypes[i] = args[i].getClass();
		Class<?> clazz = o.getClass();
		try {
			Method m = searchMethod(clazz, methodName, parameterTypes);
			if (m == null) {
				log("Can not find method [" + methodName + "] for " + clazz.getName());
				return null;
			}
			return m.invoke(o, args);
		} catch (NoSuchMethodException|SecurityException|IllegalAccessException|IllegalArgumentException|InvocationTargetException e) {
			log("Can not invoke method [" + methodName + "] for " + clazz.getName());
			e.printStackTrace();
			return null;
		}
	}

	public static JSONObject enumToJSON(Object o) {
		JSONObject j = new JSONObject();
		j.put("enum", o.getClass().getName());
		j.put("value", o.toString());
		return j;
	}

	public static void main(String[] args) throws Exception {
		ReflectionUtil.invoke(System.out, "println");
	}
}
