package com.ai.paas.ipaas.ses.dataimport.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ai.paas.ipaas.ses.dataimport.constant.SesDataImportConstants;
import com.ai.paas.ipaas.ses.dataimport.model.DataBaseAttr;
import com.ai.paas.ipaas.ses.dataimport.model.DataSql;
import com.ai.paas.ipaas.ses.dataimport.service.IDataService;
import com.ai.paas.ipaas.ses.dataimport.util.ParamUtil;

@Controller
@RequestMapping(value = "/dataimport")
public class DataImportController {
	private static final transient Logger log = LoggerFactory.getLogger(DataImportController.class);
	
	@Autowired
	private IDataService iDataService;

	
	@RequestMapping(value = "/toOne")
	public String toOne(HttpServletRequest request) {
		
		List<DataBaseAttr> dbAttrs = iDataService.loadDataSource(
					ParamUtil.getUser(request).get(SesDataImportConstants.USER_ID_STR),
					ParamUtil.getUser(request).get(SesDataImportConstants.SID_STR),
					SesDataImportConstants.GROUP_ID_1);
		if(dbAttrs!=null&&dbAttrs.size()>0){
			request.setAttribute("oneds",dbAttrs.get(0));
		}
		DataSql sql = iDataService.loadDataSql(
				ParamUtil.getUser(request).get(SesDataImportConstants.USER_ID_STR),
				ParamUtil.getUser(request).get(SesDataImportConstants.SID_STR),
				SesDataImportConstants.GROUP_ID_1);
		request.setAttribute("onesql",sql);
		
		return "import/one";
	}
	
	@RequestMapping(value = "/toMany")
	public String toMany(HttpServletRequest request) {
		List<DataBaseAttr> dbAttrs = iDataService.loadDataSource(
				ParamUtil.getUser(request).get(SesDataImportConstants.USER_ID_STR),
				ParamUtil.getUser(request).get(SesDataImportConstants.SID_STR),
				SesDataImportConstants.GROUP_ID_2);
		if(dbAttrs!=null&&dbAttrs.size()>0){
			request.setAttribute("manyds",dbAttrs);
		}
		DataSql sql = iDataService.loadDataSql(
				ParamUtil.getUser(request).get(SesDataImportConstants.USER_ID_STR),
				ParamUtil.getUser(request).get(SesDataImportConstants.SID_STR),
				SesDataImportConstants.GROUP_ID_2);
		request.setAttribute("manysql",sql);
		return "import/many";
	}
	
	
	/**测试数据源*/
	@ResponseBody
	@RequestMapping(value = "/validateSource")
	public String validateSource(HttpServletRequest request) {
		try {
			return iDataService.validateDataSource(ParamUtil.getDs(request,null),ParamUtil.getUser(request));
		} catch (Exception e) {
			log.error("",e);
			return ParamUtil.getERRORMSG(e.getMessage());

		}
	}
	
	/**保存数据源*/
	@ResponseBody
	@RequestMapping(value = "/saveDs")
	public String saveDs(HttpServletRequest request) {
		try {
			return iDataService.saveDs(ParamUtil.getDs(request,null),ParamUtil.getUser(request));
		} catch (Exception e) {
			log.error("",e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}
	/**删除数据源*/
	@ResponseBody
	@RequestMapping(value = "/deleteDs")
	public String deleteDs(HttpServletRequest request) {
		try {
			return iDataService.deleteDs(ParamUtil.getDs(request,"delete"),ParamUtil.getUser(request));
		} catch (Exception e) {
			log.error("",e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}
	
	/**测试Sql*/
	@ResponseBody
	@RequestMapping(value = "/validateSql")
	public String validateSql(HttpServletRequest request) {
		try {
			return iDataService.validateSql(null,ParamUtil.getUser(request),
					request);
		} catch (Exception e) {
			log.error("",e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}
	
	@ResponseBody
	@RequestMapping(value = "/saveSql")
	public String saveSql(HttpServletRequest request) {
		try {
			return iDataService.saveSql(request,ParamUtil.getUser(request));
		} catch (Exception e) {
			log.error("",e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}
	@ResponseBody
	@RequestMapping(value = "/deleteSql")
	public String deleteSql(HttpServletRequest request) {
		try {
			return iDataService.deleteSql(request,ParamUtil.getUser(request));
		} catch (Exception e) {
			log.error("",e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}
	
	@ResponseBody
	@RequestMapping(value = "/import")
	public String importData(HttpServletRequest request) {
		try {
			return iDataService.importData(request);
		} catch (Exception e) {
			log.error("",e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}
	
	@ResponseBody
	@RequestMapping(value = "/running")
	public String running(HttpServletRequest request) {
		try {
			return iDataService.running(request);
		} catch (Exception e) {
			log.error("",e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}
	
	
	@ResponseBody
	@RequestMapping(value = "/loadDs")
	public String loadDs(HttpServletRequest request) {
		try {
			List<DataBaseAttr> dbAttrs = iDataService.loadDataSource(
					ParamUtil.getUser(request).get(SesDataImportConstants.USER_ID_STR),
					ParamUtil.getUser(request).get(SesDataImportConstants.SID_STR),
					SesDataImportConstants.GROUP_ID_2);
			if(dbAttrs!=null&&dbAttrs.size()>0){
				request.setAttribute("manyds",dbAttrs);
			}
			return "{\"CODE\":\"000\",\"MSG\":\""+ParamUtil.getDsInfo(dbAttrs)+"\"}";
		} catch (Exception e) {
			log.error("",e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}
	
}
