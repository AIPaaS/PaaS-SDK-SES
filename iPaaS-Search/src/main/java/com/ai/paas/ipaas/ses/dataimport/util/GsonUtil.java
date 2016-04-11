package com.ai.paas.ipaas.ses.dataimport.util;

import java.lang.reflect.Type;

import com.google.gson.Gson;

public class GsonUtil {
	
	private GsonUtil(){
		
	}
	
	public static String objToGson(Object p){
		Gson gson = new Gson();
		return gson.toJson(p);
	}
	public static <T> T  gsonToObject(String json,Class<T> classOfT){
		Gson gson = new Gson();
		return gson.fromJson(json, classOfT);
	}
	public static <T> T  gsonToObject(String json,Type typeOfT){
		Gson gson = new Gson();
		return gson.fromJson(json, typeOfT);
	}
}
