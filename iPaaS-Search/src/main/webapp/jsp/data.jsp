<%@ page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%
	String ctx = request.getContextPath();
	request.setAttribute("ctx", ctx);
%>
<!DOCTYPE html>
<html>
<head>
	<meta charset="UTF-8">
	<title>数据导入</title>
	<link rel="stylesheet" type="text/css" href="${ctx}/resources/css/ses.css"/>
	<script type="text/javascript" src="${ctx}/resources/js/jquery-1.10.2.min.js"></script>
	<script type="text/javascript" src="${ctx}/resources/js/data.js"></script>
	<script type="text/javascript">
	var ctx = "${ctx}";
	</script>
</head>
<body>
	<div class="head">
		<div class="head-left" style=""><a href="${ctx}" style="color:white">主页</a> <span class="divider">/</span>SES 数据导入</div>
		<div class="head-right" onclick="LogOut()" style="cursor:pointer">${ SES_USER["userName"]}</div>
	</div>
	<div class="tittle">
		<div class="tittle-one">配置数据源 	<span class="edit_right" id="ds" onclick="toDs()">编辑</span>
		</div>
		<div class="tittle-line"></div>
		<div class="data">
			<div class="data-tittle">
				<div class="tittle-name">别名</div>
				<div class="dataType">数据类</div>
				<div class="data-from">数据源</div>
			</div>
		</div>
		<div class="tittle-one">配置SQL  <span class="edit_right" id="sql" onclick="toSql()">编辑</span></div>
		<div class="tittle-line"></div>
		<div class="table">
			<div class="table-tittle">
				<div class="table-short">别名</div>
				<div class="table-short">数据源</div>
				<div class="table-short">主键</div>
				<div class="table-short">对应关系</div>
				<div class="table-short">索引名称</div>
				<div class="table-SQL">索引SQL</div>
				<div class="table-SQL">SQL</div>
			</div>
		</div>
				<div id="result" style="color:green;text-align:center"></div>
		
				<button class="next" onclick="toImport()" id="importBut">导入</button>
		
	</div>
	<div id="query_hint" class="query_hint">
	   <img src="${ctx}/resources/images/wait.gif" />正在导入，请稍等...
	 </div>
</body>
<script type="text/javascript">
$(document).ready(function() {
	initDataSources();
	initDataSqls();
    $("#query_hint").hide();
});
function initDataSources(){
	<c:forEach items="${ds}" var="item">
 	<c:if test="${item.type == 1 }">
	appendDs("${item.alias}","${item.ip}","${item.port}","${item.sid}","${item.username}",
			"${item.pwd}","${item.database}");
	</c:if>
	<c:if test="${item.type == 2 }">
		appendDBS("${item.alias}","${item.user}","${item.serviceId}","${item.servicePwd}",
				"${item.vsql}");
	</c:if> 
	</c:forEach>

}
function initDataSqls(){
	appendSql("${sql.primarySql.alias}","${sql.primarySql.drAlias}",
			"${sql.primarySql.primaryKey}","","","","","${sql.primarySql.sql}");
	<c:forEach items="${sql.filedSqls}" var="item">
	appendSql("${item.alias}","${item.drAlias}","",getRelationName("${item.relation}"),"${item.relation}",
			"${item.indexAlias}","${item.indexSql}","${item.sql}");
	</c:forEach>
}
</script>
</html>