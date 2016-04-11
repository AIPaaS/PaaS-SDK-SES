package test.tom.ai.paas.ipaas.ses;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

public class test {
	public static void main(String[] args){
		String data = "{\"a\":1,\"b\":\"24\"}";
		 Map<String,Object> dataMap = new HashMap<String,Object>();
	      Gson gson = new Gson();
	      dataMap = gson.fromJson(data,dataMap.getClass());
	      System.out.println(dataMap);
	     System.out.println(Double.valueOf(dataMap.get("a").toString()).intValue());
	     
	     
	}

}
