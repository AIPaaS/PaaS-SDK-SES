<%@ page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%
	String ctx = request.getContextPath();
	request.setAttribute("ctx", ctx);
%>
<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<title>数据导入</title>
	<link rel="stylesheet" type="text/css" href="${ctx}/resources/css/ses.css"/>
<%-- 	<link href="${ctx}/resources/bootstrap/dist/css/bootstrap.css"  type="text/css" rel="stylesheet"/>
 --%>	<script type="text/javascript" src="${ctx}/resources/js/jquery-1.10.2.min.js"></script>
	<script type="text/javascript" src="${ctx}/resources/js/jquery.form.min.js"></script>
	<script type="text/javascript" src="${ctx}/resources/js/ds.js?1422"></script>
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
				<div class="step-two-left"></div>
				<div class="step-two-middle">
					<span class="step-two-num">2</span>
					<span class="step-two-name">配置SQL</span>
				</div>
				<div class="step-two-right no-mar-right"></div>
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
			<div class="content-box-top">
				<label>别名: </label>
				<input type="text" class="other-name" name="alias">
				<label>数据类型: </label>
				<ul class="data-type">
					<li class="cur" onclick="cutLi(0)">DBS</li>
					<li onclick="cutLi(1)">普通数据库</li>
				</ul>
			</div>
			<div class="content-box-center">
				<div class="DBS-data">
					<label>云用户: </label>
					<input type="text" class="user" name="user">
					<label>服务ID: </label>
					<input type="text" class="serviceId" name="serviceId">
					<label>服务密码: </label>
					<input type="text" class="servicePwd" name="servicePwd">
					<!-- <label>事务保障: </label>
					<input type="checkbox" class="haveTXS" name="haveTXS"/> -->
					<label>验证sql: </label>
					<input type="text" class="vsql" name="vsql">
					<button class="DBS-save" onclick="saveDBS()">添加</button>
				</div>
				<div class="SQL-data">
					<select class="database" name="database">
						<option value=1>mysql</option>
						<option value=2>oracle</option>
					</select>
					<label>IP: </label>
					<input type="text" class="ip" name="ip">
					<label>Port: </label>
					<input type="text" class="port" name="port">
					<label>Sid: </label>
					<input type="text" class="sid" name="sid">
					<label>用户名: </label>
					<input type="text" class="username" name="username">
					<label>密码: </label>
					<input type="text" class="pwd" name="pwd">
					<button class="SQL-save" onclick="saveDs()">添加</button>
				</div>
			</div>
		</div>
		<div class="data">
			<div class="data-tittle">
				<div class="tittle-name">别名</div>
				<div class="dataType">数据类型</div>
				<div class="data-from">数据源</div>
				<div class="operate">操作</div>
			</div>
		</div>
		<form id="hiddenData"></form>
		<div id="errorInfo" style="color:red;text-align:center"></div>
		<button class="next" onclick="saveDataSource()">保存并下一步</button>
	</div>
</body>
<script type="text/javascript">
$(document).ready(function() {
	initDataSources();
});
function initDataSources(){
	$("#hiddenData").empty();
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
</script>

</html>
