package com.tensult.utils;

import javax.script.ScriptEngineManager;
import javax.script.ScriptEngine;
import javax.script.SimpleScriptContext;

import java.util.Map.Entry;
import java.util.Properties;

import javax.script.ScriptContext;
import javax.script.ScriptException;

import org.apache.commons.lang3.StringUtils;

public class ScriptUtils {

	private static final ScriptEngine nashorn = new ScriptEngineManager().getEngineByName("nashorn");

	public static Object execute(String script, Properties context) {
		try {
			if (StringUtils.isNotBlank(script)) {
				SimpleScriptContext scriptContext = new SimpleScriptContext();
				for (Entry<Object, Object> property : context.entrySet()) {
					scriptContext.setAttribute((String) property.getKey(), (String) property.getValue(),
							ScriptContext.ENGINE_SCOPE);
				}
				return nashorn.eval(script, scriptContext);
			}
		} catch (ScriptException e) {
			e.printStackTrace();
		}
		return null;
	}
}
