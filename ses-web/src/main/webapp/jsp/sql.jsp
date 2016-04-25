<%@ page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%
	String ctx = request.getContextPath();
	request.setAttribute("ctx", ctx);
%>
<html>
<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<title>数据导入</title>
	<link rel="stylesheet" type="text/css" href="${ctx}/resources/css/ses.css"/>
	<script type="text/javascript" src="${ctx}/resources/js/jquery-1.10.2.min.js"></script>
	<script type="text/javascript" src="${ctx}/resources/js/jquery.form.min.js"></script>
	<script type="text/javascript" src="${ctx}/resources/js/sql.js"></script>
	<script type="text/javascript" src="${ctx}/resources/js/quoteStringUtil.js"></script>
	<script type="text/javascript">
	var ctx = "${ctx}";
	</script>
</head>
<body>
	<div class="head">
		<div class="head-left" style=""><a href="${ctx}" style="color:white">主页</a> <span class="divider">/</span>SES 数据导入</div>
		<div class="head-right" onclick="LogOut()" style="cursor:pointer">${ SES_USER["userName"]}</div>
	</div>
	<div class="content">
		<div class="content-step">
			<div class="step-one">
				<div class="step-one-left"></div>
				<div class="step-one-middle">
					<span class="step-one-num">1</span>
					<span class="step-one-name">配置数据源</span>
				</div>
				<div class="step-one-right no-mar-right"></div>
			</div>
			<div class="step-two">
				<div class="step-one-left"></div>
				<div class="step-two-middle">
					<span class="step-one-num">2</span>
					<span class="step-one-name">配置SQL</span>
				</div>
				<div class="step-one-right no-mar-right"></div>
			</div>
			<div class="step-three">
				<div class="step-three-left"></div>
				<div class="step-three-middle">
					<span class="step-three-num">3</span>
					<span class="step-three-name">完成</span>
				</div>
				<div class="step-three-right no-mar-right"></div>
			</div>
		</div>
		<div class="content-line">
			<span class="line"></span>
			<span class="add"></span>
		</div>
		<div class="content-box">			
			<div class="main">结构主体表</div>			
			<div class="main-content">
				<label class="label">别名: </label>
				<input type="text" id="palias" name="palias">
				<label class="label">数据源: </label>
				<select id="pdsalias" name="pdsalias">
				</select>
				<label class="label">主键: </label>
				<input type="text" id="primaryKey" name="primaryKey">
<!-- 				<label>SQL: </label>
 -->				<button class="edit-sql" onclick="boxSee()" id="psqlBut">编辑SQL值</button>
				<div class="SQL-txt" id="main-sql">
					<div class="SQL-txt-tittle">编辑SQL值</div>
					<textarea class="SQL-txt-content" id="sql" name="sql"></textarea>
<!-- 					<button class="cancel" onclick="boxNone()">取消</button>
 -->					<button class="saveSQLTxt" onclick="boxNone()">保存</button>
				</div>
			</div>
			<div class="assist">结构体辅助表</div>
			<div class="assist-content">
				<div class="assistTable-content">
					<label class="label">别名: </label>
					<input type="text" id="falias" name="falias">
					<label class="label">数据源: </label>
					<select id="drAlias" name="drAlias" style="margin-right:110px">
					</select>
					<label class="label">对应关系: </label>
					<ul class="relation">
					<li class="double">一对一</li>
					<li>一对多</li>
					</ul>
					
					<br/>
					<label class="label">索引别名: </label>
					<input type="text" id="indexAlias" name="indexAlias">
					<label class="label"></label>
					<button class="edit-sql"  onclick="boxSeeIndexSql()" style="margin-right:110px">编辑索引SQL</button>
					<div class="SQL-txt" id="assist-index-sql">
						<div class="SQL-txt-tittle">编辑索引SQL</div>
						<textarea class="SQL-txt-content" id="indexSql" name="indexSql"></textarea>
<!-- 						<button class="cancel" onclick="boxNone()">取消</button>
 -->						<button class="saveSQLTxt" onclick="boxNone()">保存</button>
					</div>
					<label class="label"></label>
					<button class="edit-sql"  onclick="boxSeeFiledSql()">编辑SQL值</button>
					<div class="SQL-txt" id="assist-filed-sql">
						<div class="SQL-txt-tittle">编辑SQL值</div>
						<textarea class="SQL-txt-content" id="filedSql" name="filedSql"></textarea>
<!-- 						<button class="cancel" onclick="boxNone()">取消</button>
 -->						<button class="saveSQLTxt" onclick="boxNone()">保存</button>
					</div>
					
					<button class="saveTable" onclick="saveTable()">添加</button>
				</div>
				
			</div>
		</div>
		<div class="table">
			<div class="table-tittle">
				<div class="table-short">别名</div>
				<div class="table-short">数据源</div>
				<div class="table-short">对应关系</div>
				<div class="table-short">索引名称</div>
				<div class="table-SQL">索引SQL</div>
				<div class="table-SQL">SQL</div>
				<div class="table-operate">操作</div>
			</div>
		</div>
		<form id="hiddenSqlData"></form>
		<button class="pre" onclick="toDs()" id="pre">上一步</button>
		<button class="nex" onclick="saveSql()" id="next">保存并下一步</button>
	</div>
	<div id="query_hint" class="query_hint">
	   <img src="${ctx}/resources/images/wait.gif" />正在处理，请稍等...
	 </div>
</body>
<script type="text/javascript">
$(document).ready(function() {
	initDataSources();
	initDataSql();
    $("#query_hint").hide();
});
function initDataSources(){
	$("#pdsalias").empty();
	$("#drAlias").empty();
	<c:forEach items="${ds}" var="item">
		$("#pdsalias").append("<option value=${item.alias}>${item.alias}</option>");
		$("#drAlias").append("<option value=${item.alias}>${item.alias}</option>");
	</c:forEach>
}
function initDataSql(){
	initPriSql("${sql.primarySql.alias}","${sql.primarySql.drAlias}",
			"${sql.primarySql.primaryKey}","${sql.primarySql.sql}");
	<c:forEach items="${sql.filedSqls}" var="item">
	appendFsql("${item.alias}","${item.drAlias}",getRelationName("${item.relation}"),"${item.relation}",
			"${item.indexAlias}","${item.indexSql}","${item.sql}");
	</c:forEach>

}

</script>
</html>