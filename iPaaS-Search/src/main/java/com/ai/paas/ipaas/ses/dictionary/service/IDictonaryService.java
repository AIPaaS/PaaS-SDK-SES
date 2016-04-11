package com.ai.paas.ipaas.ses.dictionary.service;

import java.util.List;
import java.util.Map;

import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserIndexWord;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserStopWord;

public interface IDictonaryService {
	
	void saveDictonaryWord(List<SesUserIndexWord> indexWordList,List<SesUserStopWord> stopWordList ,
			String userId , String serviceId);
	
	
	public Map<String,List> searchUserIndex(String userId , String serviceId);
}
