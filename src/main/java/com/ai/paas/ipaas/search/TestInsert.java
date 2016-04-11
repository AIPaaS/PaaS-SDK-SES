package com.ai.paas.ipaas.search;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ai.paas.ipaas.search.service.ISearchClient;
import com.ai.paas.ipaas.search.service.SearchClientFactory;
import com.ai.paas.ipaas.search.vo.Results;
import com.ai.paas.ipaas.search.vo.SearchOption;
import com.ai.paas.ipaas.search.vo.SearchOption.DataFilter;
import com.ai.paas.ipaas.search.vo.SearchOption.SearchLogic;
import com.ai.paas.ipaas.search.vo.SearchOption.SearchType;
import com.ai.paas.ipaas.search.vo.SearchfieldVo;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.google.gson.Gson;

public class TestInsert extends Thread {

	
	private int i ;
	private static final String AUTH_ADDR = "http://10.1.228.198:14821/iPaas-Auth/service/auth";
	private static AuthDescriptor ad = null;
	private static ISearchClient is = null;
	
	
	
	static{
		ad =  new AuthDescriptor(AUTH_ADDR, "A9A601C3552C49AF84D7FAE689A9D53D", "111111","SES004");
		try {
			is = SearchClientFactory.getSearchClient(ad);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public void run() {
		
		/**
		 * 设置搜索参数
		 */
		List<SearchfieldVo> fieldList = new ArrayList<SearchfieldVo>();

//		SearchfieldVo searchTitleVo = new SearchfieldVo();
//		searchTitleVo.setFiledName("title");
//
//		Set<String> titleFvSet = new HashSet<String>();
//		titleFvSet.add("第一");
//		titleFvSet.add("第二");
//
//		searchTitleVo.setFiledValue(titleFvSet);
//		searchTitleVo.setOption(new SearchOption(SearchType.querystring, SearchOption.SearchLogic.should, "100", DataFilter.exists, 1.0f, 1));

		SearchfieldVo searchAbsTractVo = new SearchfieldVo();
		searchAbsTractVo.setFiledName("user_code");

		List<String> fvSet = new ArrayList<String>();
		fvSet.add("太烂");
		searchAbsTractVo.setFiledValue(fvSet);
		searchAbsTractVo.setOption(new SearchOption(SearchType.querystring, SearchLogic.should, "100", DataFilter.exists, 1.0f, 1));
		
		
		//fieldList.add(searchTitleVo);
		fieldList.add(searchAbsTractVo);
		Results<Map<String, Object>> result = is.searchIndex(fieldList, 0, 10, SearchLogic.should, null, "desc");
//		is.cleanData();
		System.out.println(result.getCount());
		
		
//		List<String> list = new ArrayList<String>();
//		for(int i=0;i<10;i++){
//			
//			Map notice = new HashMap();
//			notice.put("user_id",152+i);
//			notice.put("user_code","小时代这部大电影实在是太烂了，有道云又不好用，中国话都说不好!林疯狂又怎样");
//			notice.put("ses_sid","haha");
//			notice.put("alias","haha");
//			Gson gson = new Gson();
//			list.add(gson.toJson(notice));
//		}
//		is.bulkInsertData(list);
	}
	
	
	
//	@Test
//	public void inset(){
//		List<String> list = new ArrayList<String>();
//		for(int i=0;i<10;i++){
//			
//			Map notice = new HashMap();
//			notice.put("notice_id",112+i);;
//			notice.put("title","测试title!");
//			notice.put("abstract","你好测试添加索引!");
//			notice.put("creat_date","2015-11-09 14:48:3"+i);
//			notice.put("text_link","http://www.baidu.com");
//			Gson gson = new Gson();
//			list.add(gson.toJson(notice));
//		}
//		is.bulkInsertData(list);
//		
//	}
	
	public TestInsert(int i ){
		
		this.i = i;
	}
	
}
