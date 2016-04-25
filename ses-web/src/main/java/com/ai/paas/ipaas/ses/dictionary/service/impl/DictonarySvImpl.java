package com.ai.paas.ipaas.ses.dictionary.service.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ai.paas.agent.client.AgentClient;
import com.ai.paas.ipaas.ServiceUtil;
import com.ai.paas.ipaas.search.service.ISearchClient;
import com.ai.paas.ipaas.ses.dao.interfaces.SesResourcePoolMapper;
import com.ai.paas.ipaas.ses.dao.interfaces.SesUserIndexWordMapper;
import com.ai.paas.ipaas.ses.dao.interfaces.SesUserInstanceMapper;
import com.ai.paas.ipaas.ses.dao.interfaces.SesUserStopWordMapper;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesResourcePool;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesResourcePoolCriteria;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserIndexWord;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserIndexWordCriteria;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserInstance;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserInstanceCriteria;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserStopWord;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserStopWordCriteria;
import com.ai.paas.ipaas.ses.dataimport.constant.SesDataImportConstants;
import com.ai.paas.ipaas.ses.dataimport.service.ILoginService;
import com.ai.paas.ipaas.ses.dataimport.util.ConfUtil;
import com.ai.paas.ipaas.ses.dataimport.util.GsonUtil;
import com.ai.paas.ipaas.ses.dataimport.util.SesUtil;
import com.ai.paas.ipaas.ses.dictionary.service.IDictonaryService;
import com.ai.paas.ipaas.uac.service.UserClientFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.ai.paas.ipaas.uac.vo.AuthResult;
@Service
public class DictonarySvImpl implements IDictonaryService {

	private static transient final Logger log = LoggerFactory.getLogger(DictonarySvImpl.class);

	@Override
	public void saveDictonaryWord(List<SesUserIndexWord> indexWordList,
			List<SesUserStopWord> stopWordList,
			String userId , String serviceId) {
		SesUserIndexWordMapper indexWordMapper = ServiceUtil.getMapper(SesUserIndexWordMapper.class);
		SesUserStopWordMapper stopWordMapper = ServiceUtil.getMapper(SesUserStopWordMapper.class);
		
		SesUserInstanceMapper userInstanceMapper = ServiceUtil.getMapper(SesUserInstanceMapper.class);
		SesUserInstanceCriteria userCriteria = new SesUserInstanceCriteria();
		userCriteria.createCriteria()
			.andUserIdEqualTo(userId)
			.andServiceIdEqualTo(serviceId);
		List<SesUserInstance> list  = userInstanceMapper.selectByExample(userCriteria);
//		for(SesUserIndexWord indexWord:indexWordList){
//			indexWordMapper.insert(indexWord);
//		}
//		for(SesUserStopWord stopWord : stopWordList){
//			stopWordMapper.insert(stopWord);
//		}
		
		if(indexWordList.size()>500){
			List <SesUserIndexWord> newIndexList = new ArrayList<SesUserIndexWord>();
			for(int i=0 ;i<indexWordList.size();i++){
				newIndexList.add(indexWordList.get(i));
				if(i!=0&&(i%500==0)){
					indexWordMapper.insertBatch(newIndexList);
					newIndexList= new ArrayList<SesUserIndexWord>();
					System.out.println("i+++++++++++++++++++++++++++++++++++++="+i);
					log.info("i+++++++++++++++++++++++++++++++++++++=",i);
				}
			}
		}
		else if(indexWordList.size()>0){
			indexWordMapper.insertBatch(indexWordList);
		}
		if(stopWordList.size()>500){
			List <SesUserStopWord> newStopList = new ArrayList<SesUserStopWord>();
			for(int i=0 ;i<newStopList.size();i++){
				newStopList.add(stopWordList.get(i));
				if(i!=0&&(i%500==0)){
					stopWordMapper.insertBatch(newStopList);
					newStopList = new ArrayList<SesUserStopWord>();
				}
			}
		}
		else if(stopWordList.size()>0){
			stopWordMapper.insertBatch(stopWordList);
		}
		SesUserIndexWordCriteria indexWordCriteria = new SesUserIndexWordCriteria();
		indexWordCriteria.createCriteria()
			.andUserIdEqualTo(userId)
			.andServiceIdEqualTo(serviceId);
		List<SesUserIndexWord> allIndexWordList = indexWordMapper.selectByExample(indexWordCriteria);
		
		File indexWordFile = new File("index"+userId+serviceId);
		FileOutputStream indexOut = null;
		
		try{
			indexOut = new FileOutputStream(indexWordFile);
			for(SesUserIndexWord index:allIndexWordList){
				indexOut.write(index.getWord().getBytes("UTF-8"));
				 indexOut.write("\n".getBytes());  
			}
			indexOut.close();
			
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}
		
		SesUserStopWordCriteria stopWordCriteria = new SesUserStopWordCriteria();
		stopWordCriteria.createCriteria()
			.andUserIdEqualTo(userId)
			.andServiceIdEqualTo(serviceId);
		
		
		List<SesUserStopWord> allStopWordList = stopWordMapper.selectByExample(stopWordCriteria);
		File stopWordFile = new File("stop"+userId+serviceId);
		FileOutputStream stopOut = null;
		try{
			stopOut = new FileOutputStream(stopWordFile);
			for(SesUserStopWord index:allStopWordList){
				stopOut.write(index.getWord().getBytes("UTF-8"));
				stopOut.write("\n".getBytes());  
			}
			stopOut.close();
			
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}
		
		SesResourcePoolMapper resourcePooMapper = ServiceUtil.getMapper(SesResourcePoolMapper.class);
		SesResourcePoolCriteria poolCriteria = new SesResourcePoolCriteria();
		poolCriteria.createCriteria();
		List<SesResourcePool> resourceList = resourcePooMapper.selectByExample(poolCriteria);
		for(SesUserInstance sesUser : list){
			
			SesResourcePool sesResPool = new SesResourcePool();
			for(SesResourcePool  pool : resourceList){
				String resourceIp = pool.getHostIp();
				if(resourceIp.equals(sesUser.getHostIp())){
					sesResPool = pool;
				}
			}
			
			AgentClient ac = new AgentClient(sesUser.getHostIp(), sesResPool.getAgentPort());
			FileInputStream inIndexWord = null;
			try	{
				inIndexWord=new FileInputStream(indexWordFile);
			
			}catch (FileNotFoundException e) {			
				// TODO Auto-generated catch block			
				e.printStackTrace();		
			}
			FileInputStream inStopWord = null;
			try	{
				inStopWord=new FileInputStream(stopWordFile);
			
			}catch (FileNotFoundException e) {			
				// TODO Auto-generated catch block			
				e.printStackTrace();		
			}
			ac.saveFileStream(sesResPool.getBinPath()+"/config/ik/"+userId+"/"+serviceId+"/"+serviceId+"_index.dic", inIndexWord);
			ac.saveFileStream(sesResPool.getBinPath()+"/config/ik/"+userId+"/"+serviceId+"/"+serviceId+"_stop.dic", inStopWord);
		}
	}
	
	public Map<String,List> searchUserIndex(String userId , String serviceId){
		Map<String,List> returnMap = new HashMap<String,List>();
		SesUserIndexWordMapper indexWordMapper = ServiceUtil.getMapper(SesUserIndexWordMapper.class);
		SesUserStopWordMapper stopWordMapper = ServiceUtil.getMapper(SesUserStopWordMapper.class);
		SesUserIndexWordCriteria indexWordCriteria = new SesUserIndexWordCriteria();
		indexWordCriteria.createCriteria()
			.andUserIdEqualTo(userId)
			.andServiceIdEqualTo(serviceId);
		List<SesUserIndexWord> allIndexWordList = indexWordMapper.selectByExample(indexWordCriteria);
		SesUserStopWordCriteria stopWordCriteria = new SesUserStopWordCriteria();
		stopWordCriteria.createCriteria().andUserIdEqualTo(userId).andServiceIdEqualTo(serviceId);
		List<SesUserStopWord> allStopWordList = stopWordMapper.selectByExample(stopWordCriteria);
		returnMap.put("allIndexWordList", allIndexWordList);
		returnMap.put("allStopWordList", allStopWordList);
		return returnMap;
	}
	


}
