package com.ai.paas.ipaas.ses.dataimport.util;

import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonUtil {
	
	private GsonUtil(){
		
	}
	
	public static String objToGson(Object p){
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZ")
				.create();
		return gson.toJson(p);
	}
	public static <T> T  gsonToObject(String json,Class<T> classOfT){
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZ")
				.create();
		return gson.fromJson(json, classOfT);
	}
	public static <T> T  gsonToObject(String json,Type typeOfT){
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZ")
				.create();
		return gson.fromJson(json, typeOfT);
	}
}
