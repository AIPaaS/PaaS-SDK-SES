package com.ai.paas.ipaas.ses.dictionary.controller;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import com.ai.paas.ipaas.PaasException;
import com.ai.paas.ipaas.ses.common.constants.SesConstants;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserIndexWord;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserMapping;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserStopWord;
import com.ai.paas.ipaas.ses.dataimport.util.ParamUtil;
import com.ai.paas.ipaas.ses.dictionary.service.IDictonaryService;
import com.ai.paas.ipaas.ses.mapping.service.IMappingService;

@Controller
@RequestMapping(value = "/dic")
public class DictionaryController {
	private static final transient Logger LOGGER = LoggerFactory.getLogger(DictionaryController.class);
	
	@Autowired
	IDictonaryService iDictonaryService;
	
	
	@RequestMapping(value = "/index")
	public String mappingSuccess(ModelMap model,HttpServletRequest request,HttpServletResponse response) {
		Map<String,String> userMap = ParamUtil.getUser(request);
		String userId =  ParamUtil.getUser(request).get("userId");
		String serviceId =  ParamUtil.getUser(request).get("sid");
		Map<String,List> allIndexWordMap = iDictonaryService.searchUserIndex(userId, serviceId);
		model.addAttribute("allIndexWordList", allIndexWordMap.get("allIndexWordList"));
		model.addAttribute("allStopWordList", allIndexWordMap.get("allStopWordList"));
		
		
		return "/dictionary/index";
	}
	
	@ResponseBody
	@RequestMapping(value = "/save")
	public String saveDictionary(HttpServletRequest request,MultipartHttpServletRequest multipartRequest,  HttpServletResponse response) {
		MultipartFile indexWordFile = multipartRequest.getFile("indexWord");
		MultipartFile stopWordFile = multipartRequest.getFile("stopWord");
		String ss = "";
		Map<String,String> userMap = ParamUtil.getUser(request);
		List<SesUserIndexWord> indexList = new ArrayList<SesUserIndexWord>();
		List<SesUserStopWord> stopList = new  ArrayList<SesUserStopWord>();
		try {
			if(indexWordFile!=null){
				File indexFile = new File("indexFile");
				indexWordFile.transferTo(indexFile);
				InputStreamReader indexRead = new InputStreamReader(
	                    new FileInputStream(indexFile),"GBK");
				BufferedReader indexBufferedReader = new BufferedReader(indexRead);
	            String indexTxt = null;
	            while((indexTxt = indexBufferedReader.readLine()) != null){
	                System.out.println(indexTxt);
	                SesUserIndexWord indexWord = new SesUserIndexWord();
	                indexWord.setWord(indexTxt);
	                indexWord.setServiceId(userMap.get("sid"));
	                indexWord.setUserId(userMap.get("userId"));
	                indexList.add(indexWord);
	            }
	            indexBufferedReader.close();
			}
			if(stopWordFile!=null){
				File stopFile = new File("stopFile");
				stopWordFile.transferTo(stopFile);
				InputStreamReader stopRead = new InputStreamReader(
	                    new FileInputStream(stopFile),"GBK");
				BufferedReader stopBufferedReader = new BufferedReader(stopRead);
	            String stopTxt = null;
	            while((stopTxt = stopBufferedReader.readLine()) != null){
	                System.out.println(stopTxt);
	                SesUserStopWord indexWord = new SesUserStopWord();
	                indexWord.setWord(stopTxt);
	                indexWord.setServiceId(userMap.get("sid"));
	                indexWord.setUserId(userMap.get("userId"));
	                stopList.add(indexWord);
	            }
	            stopBufferedReader.close();
			}
		} catch (IllegalStateException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		iDictonaryService.saveDictonaryWord(indexList, stopList, userMap.get("userId"), userMap.get("sid"));
		
//		indexWord.set
		ss="1";
		return ss;
	}
	
	
	
	
	
}
