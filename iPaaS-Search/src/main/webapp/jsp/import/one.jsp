<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>

<!DOCTYPE html>
<html lang="zh-cn">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<title>单数据库导入</title>
<%@include file="/jsp/common/header.jsp"%>
<link href="${ctx}/resources/res/css/import.css" rel="stylesheet"
	type="text/css">
<script type="text/javascript"
	src="${ctx}/resources/js/ses/one.js"></script>
<script type="text/javascript" src="${ctx}/resources/js/jquery.form.min.js"></script>
<script type="text/javascript" src="${ctx}/resources/js/security.js"></script>


</head>
<body class="ui-v3 buildflow" id="main">
	<nav class="navbar dao-navbar ng-scope">
		<div class="clearfix dao-container">
			<div class="navbar-header">
				<div class="back-link ng-scope">
					<div class="ng-binding ng-scope">
						<div class="daima">单库导入</div>


						<ul class="nav navbar-nav navbar-right">
							<li><a href="${ctx}/doc/"><i class="icon-file-alt"></i> 文档</a></li>
							<li class="dropdown"><a href="#" class="dropdown-toggle"
								data-toggle="dropdown" role="button" aria-haspopup="true"
								aria-expanded="false"><span class="icon-user"></span> ${ SES_USER["userName"]}
									<span class="icon-angle-down"></span></a>
								<ul class="dropdown-menu">
									<li><a href="${ctx}/login/doLogout">登出</a></li>
								</ul></li>
						</ul>
					</div>

				</div>
			</div>
		</div>
	</nav>


	<%@include file="/jsp/common/menu.jsp"%>
	
	
	<!-- 主体功能区 -->
	<div class="main ng-scope">
		<div class="dao-container ng-scope">
			<div class="panel panel-default panel-page-header">
				<div class="panel-body">
					<div class="primary-section"
						style="border: 0; margin: 0; padding-bottom: 0;">
						<h2>配置数据源与SQL</h2>
						<p style="margin-bottom:2px">搜素文档的模型字段来源于一个数据库。单库特点如下：</p>
						<p style="margin-bottom:2px">1.只允许一个数据库</p>
						<p style="margin-bottom:2px">2.支持多表联合查询</p>
						<p style="margin-bottom:2px">3.select字段与模型字段对应，可以使用as</p>
					</div>
				</div>
			</div>
			<div class="panel panel-default">
				<div class="panel-body">
				<form id="one">
					<div class="setting-section">
							<div class="col-md-10 col-lg-10">
								<label class="setting-label">数据库类型 </label>
								<div class="setting-info">
									<select name="database" id="database" class="form-control ng-pristine ng-valid ng-touched" ng-model="buildflow.package_name" 
										autofocus tabindex="0" aria-invalid="false" style="max-width:450px">
										<option>请选择</option>	
										<option value="1">MYSQL</option>	
										<option value="2">ORACLE</option>	
									</select>
								</div>
								
							</div>
							<div class="col-md-10 col-lg-10" style="margin-top:15px">
								<label class="setting-label">数据库主机 </label>
								<div class="setting-info" id="ipDiv">
									<input class="form-control ng-pristine ng-valid ng-touched"
										type="text" placeholder="数据库主机 ip..."
										ng-model="buildflow.package_name" onblur="checkIp()"
										autofocus tabindex="0" aria-invalid="false" name="ip" id="ip">
								</div>
							</div>
							<div class="col-md-10 col-lg-10" style="margin-top:15px">
								<label class="setting-label">数据库端口 </label>
								<div class="setting-info" id="portDiv">
									<input class="form-control ng-pristine ng-valid ng-touched"
										type="text" placeholder="数据库端口..."
										ng-model="buildflow.package_name" onblur="checkPort()"
										autofocus tabindex="0" aria-invalid="false" name="port" id="port">
								</div>
							</div>
							<div class="col-md-10 col-lg-10" style="margin-top:15px">
								<label class="setting-label">数据库ID </label>
								<div class="setting-info" id="sidDiv">
									<input class="form-control ng-pristine ng-valid ng-touched"
										type="text" placeholder="数据库ID 名称..."
										ng-model="buildflow.package_name" onblur="checkSid()"
										autofocus tabindex="0" aria-invalid="false" name="sid" id="sid" >
								</div>
							</div>
							<div class="col-md-10 col-lg-10" style="margin-top:15px">
								<label class="setting-label">用户名 </label>
								<div class="setting-info" id="usernameDiv">
									<input class="form-control ng-pristine ng-valid ng-touched"
										type="text" placeholder="数据库用户名..."
										ng-model="buildflow.package_name" onblur="checkUsername()"
										autofocus tabindex="0" aria-invalid="false" name="username" id="username" >
								</div>
							</div>
							<div class="col-md-10 col-lg-10" style="margin-top:15px">
								<label class="setting-label">密码 </label>
								<div class="setting-info" id="pwdDiv">
									<input type="password" style="display:none"/>
									<input class="form-control ng-pristine ng-valid ng-touched"
										type="password" placeholder="密码..."
										ng-model="buildflow.package_name" onblur="checkPwd()"
										autofocus tabindex="0" aria-invalid="false" name="pwd" id="pwd">
								</div>
							</div>
							<div style="float:right;">
								<button type="button" class="btn btn-default" style="border-radius:4px" id="checkDbCon" onclick="checkDb()">测试</button>
							  	<button type="button" class="btn btn-default" style="border-radius:4px" id="saveDb" onclick="saveDs()">保存</button>
							  	<button type="button" class="btn btn-default" style="border-radius:4px" id="ddb" onclick="deleteDb()">删除</button>
							 </div>
							 
							 
							<div id="dbinfo" style="display: inline-block;text-align: center;width:100%;margin:5px 0px 0px 0px; "></div>
							 
						</div>
						<!-- <div class="setting-section">
							<div class="btn-group" role="group" aria-label="...">
							  <button type="button" class="btn btn-default" id="checkDbCon" onclick="checkDb()">测试连接</button>
							  <button type="button" class="btn btn-default" id="saveDb" onclick="saveDs()">保存</button>
							  <button type="button" class="btn btn-default" id="ddb" onclick="deleteDb()">删除</button>
							  <div  id="dbinfo" style="float:right;display:none;height:32px;line-height:32px;"></div>
							</div>
						</div> -->
						<!-- <div style="display: inline-block; width: 100%">
							<div role="alert" id="dbinfo"
								style="text-align: center; display: none;">success</div>
						</div> -->
<!--  						<div id="dbinfo" style="display: inline-block;text-align: center;width:96%;margin:0px 20px 0px 20px; "></div>
 --> 				<div class="setting-section">
						<div class="col-md-9 col-lg-9">
							<label class="setting-label">SQL</label>
							<div class="setting-info" id="sqlDiv">
									<textarea style="width:450px;height:150px;border: 1px solid #e5e5e5;color:#555"
										placeholder="sql"
										ng-model="buildflow.package_name" onblur="checkSql()"
										autofocus tabindex="0" aria-invalid="false" name="sql" id="sql"></textarea>
								</div>
						</div>

							<div style="float: right;display: inline-block; margin-top:115px">
								<button type="button" class="btn btn-default"
									style="border-radius: 4px" id="checkSqlCon" onclick="sqlCon()">测试</button>
								<button type="button" class="btn btn-default"
									style="border-radius: 4px" id="saveSql" onclick="saveSqlFun()">保存</button>
								<button type="button" class="btn btn-default"
									style="border-radius: 4px" id="dSql" onclick="deleteSql()">删除</button>
							</div>
							
							
							<div id="sqlinfo" style="display: inline-block;text-align: center;width:100%;margin:5px 0px 0px 0px; "></div>
							
						</div><!-- 
						<div class="setting-section">
							<div class="btn-group" role="group" aria-label="...">
								<button type="button" class="btn btn-default" id="checkSqlCon"
									onclick="sqlCon()">测试</button>
								<button type="button" class="btn btn-default" id="saveSql"
									onclick="saveSqlFun()">保存</button>
								<button type="button" class="btn btn-default" id="dSql"
									onclick="deleteSql()">删除</button>
								<div id="sqlinfo"
									style="float: right; display: none; height: 32px; line-height: 32px;"></div>
							</div>
						</div> -->
						<!-- <div style="display: inline-block; width: 100%">
							<div role="alert" id="sqlinfo"
								style="text-align: center; display: none;">success</div>
						</div> -->
<!-- 						<div id="sqlinfo" class="alert alert-success" style="text-align: center; display: none;"></div>
 -->						
 
 
<!--  				<div id="sqlinfo" style="display: inline-block;text-align: center;width:96%;margin:0px 20px 0px 20px; "></div>
 -->						
						<div class="setting-section">
						<p class="info-block open">
							<i class="fa fa-info-circle"></i>依据配置，一次提取所有文档字段，逐条封装文档，批量提交到搜索服务器；搜索服务器中如果已有文档，会被更新。
						</p>
						<input type="hidden"  name="groupId" value="1"/>
						<input type="hidden"  name="uId" value="${oneds.uId }"/>
						<input type="hidden"  name="id" value="${oneds.id }"/>
						<input type="hidden"  name="sql.id" value="${onesql.id }"/>
						<button type="button" data-loading-text="正在导入......"
							class="btn btn-lg btn-block project-creation-btn ng-binding" id="imp"  onclick="startImport()">开始导入</button>
			            <div id="info" class="biianx" style="overflow:scroll; ">
                        </div>
					</div>
					
					</form>
				</div>
			</div>
		</div>
	</div>



	<div class="help">
		<a href="javascript:return false;"><i class="icon-comment"></i><span>?</span></a>
	</div>


	
</body>

<script type="text/javascript">

$(document).ready(function() {
	 try{
		 initDisplay("${onesql.primarySql.sql}","${oneds.ip}","${oneds.port}","${oneds.sid}","${oneds.username}",
					"${oneds.pwd}","${oneds.database}");
	 }catch(exception){
		 initDisplay("","${oneds.ip}","${oneds.port}","${oneds.sid}","${oneds.username}",
					"${oneds.pwd}","${oneds.database}");
	 }finally{

	 }

	
});

</script>
</html>
