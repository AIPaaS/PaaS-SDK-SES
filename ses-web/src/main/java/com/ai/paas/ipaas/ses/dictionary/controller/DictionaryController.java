package com.ai.paas.ipaas.ses.dictionary.controller;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import com.ai.paas.ipaas.PaaSConstant;
import com.ai.paas.ipaas.PaasException;
import com.ai.paas.ipaas.ses.common.constants.SesConstants;
import com.ai.paas.ipaas.ses.dataimport.util.ParamUtil;
import com.ai.paas.ipaas.ses.manage.rest.interfaces.IRPCIKDictionary;
import com.ai.paas.ipaas.ses.manage.rest.interfaces.IRPCIndexMapping;
import com.ai.paas.ipaas.ses.manage.rest.interfaces.IRPCSesUserInst;
import com.ai.paas.ipaas.ses.dataimport.util.HttpClientUtil;
import com.ai.paas.ipaas.util.StringUtil;
import com.ai.paas.ipaas.vo.ses.RPCDictionay;
import com.ai.paas.ipaas.vo.ses.SesUserInstance;
import com.ai.paas.ipaas.vo.ses.SesUserMapping;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

@Controller
@RequestMapping(value = "/dic")
public class DictionaryController {
	private static final transient Logger LOGGER = LoggerFactory
			.getLogger(DictionaryController.class);

	@Autowired
	IRPCIndexMapping indexMappingSRV;
	@Autowired
	IRPCSesUserInst sesUserInst;
	@Autowired
	IRPCIKDictionary dictSRV;

	@SuppressWarnings({ "unused", "rawtypes" })
	@RequestMapping(value = "/index")
	public String mappingSuccess(ModelMap model, HttpServletRequest request,
			HttpServletResponse response) {
		Map<String, String> userMap = ParamUtil.getUser(request);
		String userId = ParamUtil.getUser(request).get("userId");
		String serviceId = ParamUtil.getUser(request).get("sid");
		List<SesUserMapping> fildList = new ArrayList<SesUserMapping>();
		String indexName= null;   
		try {
			Map<String, List> allIndexWordMap = dictSRV.getUserDictionary(userId,
					serviceId, 1, 500);
			model.addAttribute("allIndexWordList",
					allIndexWordMap.get("allIndexWordList"));
			model.addAttribute("allStopWordList",
					allIndexWordMap.get("allStopWordList"));
			SesUserMapping mapping = indexMappingSRV.loadMapping(userId,serviceId);
			SesUserInstance ins = sesUserInst.queryInst(userId, serviceId);
			String addr = "http://" + ins.getHostIp() + ":" + ins.getSesPort();
			model.addAttribute("addr", addr);
			//获得索引值
			indexName= mapping.getIndexName();   
			String mappings = mapping.getMapping();
			//循环截取出json串中 分词字段 的 analyze的值为true的
			String filds = "";
			JSONObject obj=new JSONObject(mappings);
	        Iterator it = obj.keys();  
	        filds = (String) it.next();  
			model.addAttribute("filds",filds);
			model.addAttribute("indexName",indexName);
		} catch (Throwable e) {
			model.addAttribute("filds","");
			model.addAttribute("indexName","");
			model.addAttribute("addr", "");
			LOGGER.info(SesConstants.EXPECT_ONE_RECORD_FAIL, e);
		}
		return "/dictionary/index";
	}
	
	@ResponseBody
	@RequestMapping(value = "/getFildWords", produces = "application/text;charset=utf-8")
	public String getFildWords(HttpServletRequest request,
			HttpServletResponse response) {
		String filds = request.getParameter("filds");
		String inputText = request.getParameter("inputText");
		String addr = request.getParameter("addr");
		String indexName = request.getParameter("indexName");
		addr = addr +"/"+ indexName +"/_analyze?field="  + filds;
		String result=null;
		StringBuffer keyVals = new StringBuffer();
		try {
		    result = HttpClientUtil.sendPostRequest(addr, new Gson().toJson(inputText));
			JSONObject obj=new JSONObject(result);
	        Iterator it = obj.keys();  
	        while (it.hasNext()) {  
	            String key = (String) it.next();  
	            String value = obj.get(key).toString();  
	            JSONArray obj_cs= new JSONArray(value); 
	            for(int i=0;i<obj_cs.length();i++){
	            	JSONObject obj_c = (JSONObject)obj_cs.get(i);
	            	 Iterator it_c = obj_c.keys();  
	 	            while (it_c.hasNext()) {  
	 	            	   String key_c = (String) it_c.next();  
	 	                   String value_c = obj_c.get(key_c).toString();
	 	                   System.out.println("key: "+key_c + " value: "+ value_c);
	 	                   if( "token".equals(key_c) ){
	 	                	    keyVals.append(value_c).append(",");
	 	                   }
	 	            }
	            }
	        }
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return keyVals.toString();
	}

	@ResponseBody
	@RequestMapping(value = "/getIndexWords", produces = "application/text;charset=utf-8")
	public String getIndexWords(ModelMap model, HttpServletRequest request,
			HttpServletResponse response) {
		String userId = ParamUtil.getUser(request).get("userId");
		String serviceId = ParamUtil.getUser(request).get("sid");
		int start = Integer.parseInt(request.getParameter("start"));
		int rows = Integer.parseInt(request.getParameter("rows"));
		String indexWords = dictSRV.getUserIndexWords(userId, serviceId, start,
				rows);
		return indexWords;
	}

	@ResponseBody
	@RequestMapping(value = "/getStopWords", produces = "application/text;charset=utf-8")
	public String getStopWords(ModelMap model, HttpServletRequest request,
			HttpServletResponse response) {
		String userId = ParamUtil.getUser(request).get("userId");
		String serviceId = ParamUtil.getUser(request).get("sid");
		int start = Integer.parseInt(request.getParameter("start"));
		int rows = Integer.parseInt(request.getParameter("rows"));
		String stopWords = dictSRV.getUserStopWords(userId, serviceId, start,
				rows);

		return stopWords;
	}

	@ResponseBody
	@RequestMapping(value = "/clearIndexWords")
	public String clearIndexWords(ModelMap model, HttpServletRequest request,
			HttpServletResponse response) {
		String userId = ParamUtil.getUser(request).get("userId");
		String serviceId = ParamUtil.getUser(request).get("sid");
		dictSRV.clearAllIndexWords(userId, serviceId);

		return "1";
	}

	@ResponseBody
	@RequestMapping(value = "/clearStopWords")
	public String clearStopWords(ModelMap model, HttpServletRequest request,
			HttpServletResponse response) {
		String userId = ParamUtil.getUser(request).get("userId");
		String serviceId = ParamUtil.getUser(request).get("sid");
		dictSRV.clearAllStopWords(userId, serviceId);

		return "1";
	}

	@ResponseBody
	@RequestMapping(value = "/updateEngineWords")
	public String updateEngineWords(ModelMap model, HttpServletRequest request,
			HttpServletResponse response) {
		String userId = ParamUtil.getUser(request).get("userId");
		String serviceId = ParamUtil.getUser(request).get("sid");
		String rootPath = request.getServletContext().getRealPath("/");
		createEngineWordsDir(rootPath, userId, serviceId);
		// 开始查询所有的热词和停用词，500条一次，文件不能关闭
		BufferedWriter bw = null;
		try {
			String ext = rootPath + "dict/ext/" + userId + "/" + serviceId
					+ "/ext.dict";
			bw = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(ext, false)));
			int start = 1;
			int rows = 500;
			String words = dictSRV.getUserIndexWords(userId, serviceId, start,
					rows);
			while (!StringUtil.isBlank(words)) {
				// 保存到文件
				saveEngineIndexWords(bw, words);
				words = null;
				start++;
				words = dictSRV.getUserIndexWords(userId, serviceId, start,
						rows);
			}
			bw.close();
			ext = rootPath + "dict/stop/" + userId + "/" + serviceId
					+ "/stop.dict";
			bw = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(ext, false)));
			start = 1;
			rows = 500;
			words = dictSRV.getUserStopWords(userId, serviceId, start, rows);
			while (!StringUtil.isBlank(words)) {
				// 保存到文件
				saveEngineStopWords(bw, words);
				words = null;
				start++;
				words = dictSRV
						.getUserStopWords(userId, serviceId, start, rows);
			}
		} catch (Exception e) {
			LOGGER.error("", e);
		} finally {
			if (null != bw) {
				try {
					bw.close();
				} catch (IOException e) {
					LOGGER.error(bw.toString(), e);
				}
			}
		}
		return "1";
	}

	@ResponseBody
	@RequestMapping(value = "/save")
	public String saveDictionary(HttpServletRequest request,
			MultipartHttpServletRequest multipartRequest,
			HttpServletResponse response) {
		MultipartFile indexWordFile = multipartRequest.getFile("indexWord");
		MultipartFile stopWordFile = multipartRequest.getFile("stopWord");
		String ss = "";
		Map<String, String> userMap = ParamUtil.getUser(request);
		StringBuilder indexList = new StringBuilder();
		StringBuilder stopList = new StringBuilder();
		try {
			if (indexWordFile != null) {
				File indexFile = new File("indexFile");
				indexWordFile.transferTo(indexFile);
				InputStreamReader indexRead = new InputStreamReader(
						new FileInputStream(indexFile),
						PaaSConstant.CHARSET_UTF8);
				BufferedReader indexBufferedReader = new BufferedReader(
						indexRead);
				String indexTxt = null;
				while ((indexTxt = indexBufferedReader.readLine()) != null) {
					if (!StringUtil.isBlank(indexTxt)) {
						indexList.append(indexTxt).append(" ");
					}
				}
				indexBufferedReader.close();
			}
			if (stopWordFile != null) {
				File stopFile = new File("stopFile");
				stopWordFile.transferTo(stopFile);
				InputStreamReader stopRead = new InputStreamReader(
						new FileInputStream(stopFile),
						PaaSConstant.CHARSET_UTF8);
				BufferedReader stopBufferedReader = new BufferedReader(stopRead);
				String stopTxt = null;
				while ((stopTxt = stopBufferedReader.readLine()) != null) {
					stopList.append(stopTxt).append(" ");
				}
				stopBufferedReader.close();
			}
		} catch (IllegalStateException | IOException e) {
			LOGGER.error("", e);
		}
		RPCDictionay rpcDict = new RPCDictionay();
		rpcDict.setIndexWordList(indexList.toString().trim());
		rpcDict.setStopWordList(stopList.toString().trim());
		rpcDict.setUserId(userMap.get("userId"));
		rpcDict.setServiceId(userMap.get("sid"));
		dictSRV.saveDictonaryWord(rpcDict);
		String rootPath = request.getServletContext().getRealPath("/");
		saveEngineWords(rootPath, userMap.get("userId"), userMap.get("sid"),
				indexList.toString(), stopList.toString(), true);
		ss = "1";
		return ss;
	}

	@ResponseBody
	@RequestMapping(value = "/saveAllIndexWords")
	public String saveAllIndexWords(HttpServletRequest request,
			HttpServletResponse response) {
		String ss = "";
		// 获取参数
		String allIndexWrods = request.getParameter("words");
		Map<String, String> userMap = ParamUtil.getUser(request);
		Gson gson = new Gson();
		JsonObject json = new JsonObject();
		json.addProperty("userId", userMap.get("userId"));
		json.addProperty("serviceId", userMap.get("sid"));
		json.addProperty("words", allIndexWrods);
		String result = dictSRV.saveAllIndexWords(gson.toJson(json));
		json = gson.fromJson(result, JsonObject.class);
		String retCode = json.get("resultCode").getAsString();
		if (retCode.equalsIgnoreCase("000000")) {
			// 开始新生成文件
			String rootPath = request.getServletContext().getRealPath("/");
			saveEngineWords(rootPath, userMap.get("userId"),
					userMap.get("sid"), allIndexWrods, null, false);
			ss = "1";
		} else
			ss = "0";
		return ss;
	}

	private void saveEngineWords(String rootPath, String userId,
			String serviceId, String indexList, String stopList, boolean append) {
		// 保存成功后，向本地目录追加词典，如果用不进行了修改，则需要全部重写，后续实现?
		createEngineWordsDir(rootPath, userId, serviceId);
		if (!StringUtil.isBlank(indexList)) {
			// 开始写
			String ext = rootPath + "dict/ext/" + userId + "/" + serviceId
					+ "/ext.dict";
			BufferedWriter bw = null;
			try {
				bw = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(ext, append)));
				saveEngineIndexWords(bw, indexList);
			} catch (FileNotFoundException e) {
				LOGGER.error("", e);
			} finally {
				if (null != bw) {
					try {
						bw.close();
					} catch (IOException e) {
						LOGGER.error(bw.toString(), e);
					}
				}
			}
		}
		if (!StringUtil.isBlank(stopList)) {
			String ext = rootPath + "dict/stop/" + userId + "/" + serviceId
					+ "/stop.dict";
			BufferedWriter bw = null;
			try {
				bw = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(ext, append)));
				saveEngineStopWords(bw, stopList);
			} catch (FileNotFoundException e) {
				LOGGER.error("", e);
			} finally {
				if (null != bw) {
					try {
						bw.close();
					} catch (IOException e) {
						LOGGER.error(bw.toString(), e);
					}
				}
			}
		}
	}

	private void createEngineWordsDir(String rootPath, String userId,
			String serviceId) {
		File extDir = new File(rootPath + "/dict/ext/" + userId + "/"
				+ serviceId);
		extDir.mkdirs();
		extDir = null;
		File stopDir = new File(rootPath + "dict/stop/" + userId + "/"
				+ serviceId);
		stopDir.mkdirs();
		stopDir = null;
	}

	private void saveEngineIndexWords(BufferedWriter bw, String indexList) {
		// 保存成功后，向本地目录追加词典，如果用不进行了修改，则需要全部重写，后续实现?
		if (!StringUtil.isBlank(indexList)) {
			// 开始写
			String[] splits = indexList.trim().split(" ");
			for (String indexWord : splits) {
				try {
					bw.write(indexWord);
					bw.newLine();
				} catch (IOException e) {
					LOGGER.error("", e);
				}
			}
		}

	}

	private void saveEngineStopWords(BufferedWriter bw, String stopList) {
		// 保存成功后，向本地目录追加词典，如果用不进行了修改，则需要全部重写，后续实现?
		if (!StringUtil.isBlank(stopList)) {
			// 开始写
			String[] splits = stopList.trim().split(" ");
			for (String indexWord : splits) {
				try {
					bw.write(indexWord);
					bw.newLine();
				} catch (IOException e) {
					LOGGER.error("", e);
				}
			}
		}
	}
}
