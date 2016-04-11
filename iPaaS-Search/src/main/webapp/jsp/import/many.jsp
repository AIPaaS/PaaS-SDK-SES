<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions"%>

<!DOCTYPE html>
<html lang="zh-cn">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<title>多数据库导入</title>
<%@include file="/jsp/common/header.jsp"%>
<link href="${ctx}/resources/res/css/import.css" rel="stylesheet"
	type="text/css">
<script type="text/javascript" src="${ctx}/resources/js/ses/many.js"></script>
<script type="text/javascript"
	src="${ctx}/resources/js/jquery.form.min.js"></script>
<script type="text/javascript" src="${ctx}/resources/js/security.js"></script>
<style>
.form-control-select {
    width: 100%;
    height: 34px;
    padding: 6px 5px;
    font-size: 14px;
    line-height: 1.42857;
    color: #555;
    background-color: #fff;
    background-image: none;
    border: 1px solid #ddd;
    border-radius: 2px;
    -webkit-box-shadow: inset 0 1px 1px rgba(0, 0, 0, .075);
    box-shadow: inset 0 1px 1px rgba(0, 0, 0, .075);
    -webkit-transition: border-color ease-in-out .15s, box-shadow ease-in-out .15s;
    -o-transition: border-color ease-in-out .15s, box-shadow ease-in-out .15s;
    transition: border-color ease-in-out .15s, box-shadow ease-in-out .15s
}</style>
</head>
<body class="ui-v3 buildflow" id="main">
	<nav class="navbar dao-navbar ng-scope">
		<div class="clearfix dao-container">
			<div class="navbar-header">
				<div class="back-link ng-scope">
					<div class="ng-binding ng-scope">
						<div class="daima">多库导入</div>


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
						<p style="margin-bottom: 2px">适用于复杂的数据库环境，搜素文档的模型字段分别来源于不同数据库的不同表。多库特点如下：</p>
						<p style="margin-bottom: 2px">1.支持多个数据库，支持DBS数据库配置</p>
						<p style="margin-bottom: 2px">2.不支持多表联合查询，需要拆分sql语句</p>
						<p style="margin-bottom: 2px">3.select字段与模型字段对应，可以使用as</p>
					</div>
				</div>
			</div>
			<div class="panel panel-default">
				<div class="panel-body">
					<div class="setting-section">
						<div class="col-md-10 col-lg-10">
							<label class="setting-label">数据库大类 </label>
							<div class="setting-info">
								<select name="type" id="type"
									class="form-control ng-pristine ng-valid ng-touched"
									ng-model="buildflow.package_name" autofocus tabindex="0"
									aria-invalid="false" style="max-width: 450px"
									onchange="changeType()">
									<option value=-1>请选择</option>
									<option value=1>普通数据库</option>
									<option value=2>DBS</option>
								</select>
							</div>

						</div>
						<div id="common">
							<div class="col-md-10 col-lg-10" style="margin-top: 8px">
								<label class="setting-label">数据库类型 </label>
								<div class="setting-info">
									<select name="database" id="database"
										class="form-control ng-pristine ng-valid ng-touched"
										ng-model="buildflow.package_name" autofocus tabindex="0"
										aria-invalid="false" style="max-width: 450px">
										<option value="1">MYSQL</option>
										<option value="2">ORACLE</option>
									</select>
								</div>

							</div>
							<div class="col-md-10 col-lg-10" style="margin-top: 8px">
								<label class="setting-label">数据库主机 </label>
								<div class="setting-info" id="ipDiv">
									<input class="form-control ng-pristine ng-valid ng-touched"
										type="text" placeholder="数据库主机 ip..."
										ng-model="buildflow.package_name" onblur="checkIp()" autofocus
										tabindex="0" aria-invalid="false" name="ip" id="ip">
								</div>
							</div>
							<div class="col-md-10 col-lg-10" style="margin-top: 8px">
								<label class="setting-label">数据库端口 </label>
								<div class="setting-info" id="portDiv">
									<input class="form-control ng-pristine ng-valid ng-touched"
										type="text" placeholder="数据库端口..."
										ng-model="buildflow.package_name" onblur="checkPort()"
										autofocus tabindex="0" aria-invalid="false" name="port"
										id="port">
								</div>
							</div>
							<div class="col-md-10 col-lg-10" style="margin-top: 8px">
								<label class="setting-label">数据库ID </label>
								<div class="setting-info" id="sidDiv">
									<input class="form-control ng-pristine ng-valid ng-touched"
										type="text" placeholder="数据库ID 名称..."
										ng-model="buildflow.package_name" onblur="checkSid()"
										autofocus tabindex="0" aria-invalid="false" name="sid"
										id="sid">
								</div>
							</div>
							<div class="col-md-10 col-lg-10" style="margin-top: 8px">
								<label class="setting-label">用户名 </label>
								<div class="setting-info" id="usernameDiv">
									<input class="form-control ng-pristine ng-valid ng-touched"
										type="text" placeholder="数据库用户名..."
										ng-model="buildflow.package_name" onblur="checkUsername()"
										autofocus tabindex="0" aria-invalid="false" name="username"
										id="username">
								</div>
							</div>
							<div class="col-md-10 col-lg-10" style="margin-top: 8px">
								<label class="setting-label">密码 </label>
								<div class="setting-info" id="pwdDiv">
									<input type="password" style="display: none" /> <input
										class="form-control ng-pristine ng-valid ng-touched"
										type="password" placeholder="密码..."
										ng-model="buildflow.package_name" onblur="checkPwd()"
										autofocus tabindex="0" aria-invalid="false" name="pwd"
										id="pwd">
								</div>
							</div>
						</div>

						<div id="dbs">
							<div class="col-md-10 col-lg-10" style="margin-top: 8px">
								<label class="setting-label">云账号 </label>
								<div class="setting-info" id="userDiv">
									<input class="form-control ng-pristine ng-valid ng-touched"
										type="text" placeholder="云账号 eg. zhangsan@asiainfo.com..."
										ng-model="buildflow.package_name" onblur="checkUser()"
										autofocus tabindex="0" aria-invalid="false" name="user"
										id="user">
								</div>
							</div>

							<div class="col-md-10 col-lg-10" style="margin-top: 8px">
								<label class="setting-label">服务ID </label>
								<div class="setting-info" id="serviceIdDiv">
									<input class="form-control ng-pristine ng-valid ng-touched"
										type="text" placeholder="服务ID eg.DBS001..."
										ng-model="buildflow.package_name" onblur="checkServiceId()"
										autofocus tabindex="0" aria-invalid="false" name="serviceId"
										id="serviceId">
								</div>
							</div>

							<div class="col-md-10 col-lg-10" style="margin-top: 8px">
								<label class="setting-label">服务密码 </label>
								<div class="setting-info" id="servicePwdDiv">
									<input class="form-control ng-pristine ng-valid ng-touched"
										type="password" placeholder="服务密码..."
										ng-model="buildflow.package_name" onblur="checkServicePwd()"
										autofocus tabindex="0" aria-invalid="false" name="servicePwd"
										id="servicePwd">
								</div>
							</div>

							<div class="col-md-10 col-lg-10" style="margin-top: 8px">
								<label class="setting-label">验证sql </label>
								<div class="setting-info" id="vsqlDiv">
									<textarea
										style="width: 450px; height: 80px; border: 1px solid #e5e5e5; color: #555"
										placeholder="验证sql..." ng-model="buildflow.package_name"
										onblur="checkVsql()" autofocus tabindex="0"
										aria-invalid="false" name="vsql" id="vsql">
										</textarea>
								</div>
							</div>


						</div>

						<button type="button" class="btn btn-default" style="float: right;border-radius: 4px"
							id="saveDb" onclick="saveDs()">保存</button>
						<div id="dbinfo"
							style="display: inline-block; text-align: center; width: 100%; margin: 5px 0px 0px 0px;"></div>

						<div style="display: inline-block;display:none; width: 100%;" id="dsDiv">
							<table class="table" id="dsTable">
								<tbody>
									<tr>
										<th width="40%" id="th1">数据源</th>
										<th width="60%" id="th2">操作</th>
									</tr>
								</tbody>
							</table>
						</div>

					</div>




					<!-- sql -->

					<div class="setting-section">
						<div>
							<p class="text-muted" style="color:#AAB2BD;">主体SQL配置，以本结果集中字段为基准去提取其它表中的字段（关联sql的拆分），逐条封装文档。</p>
						</div>
						<div class="col-md-10 col-lg-10" style="margin-top: 8px">
							<label class="setting-label">sql标识 </label>
							<div class="setting-info" id="aliasDiv">
								<input type="text"
									class="form-control ng-pristine ng-untouched ng-valid"
									id="alias" name="alias" onblur="checkAlias()"
									placeholder="eg.aaaa 本次sql结果集的标识，方便其余sql引用">
							</div>
						</div>
						<div class="col-md-10 col-lg-10" style="margin-top: 8px">
							<label class="setting-label">数据源 </label>
							<div class="setting-info" id="drAliasDiv">
								<select class="form-control-select ng-pristine ng-valid ng-touched"
									ng-model="buildflow.package_name" autofocus tabindex="0"
									aria-invalid="false" style="width: 450px" id="drAlias"
									name="drAlias" onblur="checkDrAlias()">
									<option>请选择</option>
								</select>
							</div>
						</div>
						<div class="col-md-10 col-lg-10" style="margin-top: 8px">
							<label class="setting-label">sql </label>
							<div class="setting-info" id="sqlDiv">
								<textarea
									style="width: 450px; height: 100px; border: 1px solid #e5e5e5; color: #555"
									onblur="checkSql()"
									placeholder="eg. select id,name,code from user" id="sql"
									name="sql">
								</textarea>
							</div>
						</div>



						<div style="float: right; display: inline-block; margin-top:75px">
							<button type="button" class="btn btn-default"
								style="border-radius: 4px" id="testPriSqlBut"
								onclick="testPriSql()">测试</button>
							<button type="button" class="btn btn-default"
								style="border-radius: 4px" id="savePriSqlBut"
								onclick="savePriSql()">保存</button>
							<button type="button" class="btn btn-default"
								style="border-radius: 4px" id="deletePriSqlBut"
								onclick="deletePriSql()">删除</button>
						</div>
						<div id="sqlinfo"
							style="display: inline-block; text-align: center; width: 100%; margin: 5px 0px 0px 0px;"></div>

						<!-- fsql -->
						<div>
							<p class="text-muted" style="color:#AAB2BD;">辅助SQL配置，以主体SQl结果集中的值为条件去提取其它表的字段。
								辅助SQL如果对应mapping中的Object对象，请选择true，sql标识，必须填写mapping中的Object的名称，如果该Object的父节点也是Object，
								那么sql标识填写Object对象父节点的名称.Object对象的名称；辅助SQL如果只对应mapping中的普通类型，sql标识 可以不使用mapping
								辅助SQL的结果集也可以作为条件。如果使用到DBS数据库时，索引sql主要为了提取字段时使用分区字段作为查询条件，索引sql可以不填写。</p>
						</div>
						<div class="col-md-10 col-lg-10" style="margin-top: 8px">
							<label class="setting-label">对应mapping中的Object </label>
							<div class="setting-info" id="mapObjDiv">
								<select class="form-control-select ng-pristine ng-valid ng-touched"
									ng-model="buildflow.package_name" autofocus tabindex="0"
									aria-invalid="false" style="width: 450px" id="mapObj"
									name="mapObj" onchange="checkMapObj()">
									<option value="false">否</option>
									<option value="true">是</option>
								</select>
							</div>
						</div>
						<div class="col-md-10 col-lg-10" style="margin-top: 8px">
							<label class="setting-label">sql标识 </label>
							<div class="setting-info" id="faliasDiv">
								<input type="text"
									class="form-control ng-pristine ng-untouched ng-valid"
									id="falias" name="falias" onblur="checkFalias()"
									placeholder="eg.bbbb 本次sql结果集的标识，方便其余sql引用">
							</div>
						</div>
						<div class="col-md-10 col-lg-10" style="margin-top: 8px">
							<label class="setting-label">数据源 </label>
							<div class="setting-info" id="fdrAliasDiv">
								<select class="form-control-select ng-pristine ng-valid ng-touched"
									ng-model="buildflow.package_name" autofocus tabindex="0"
									aria-invalid="false" style="width: 450px" id="fdrAlias"
									name="fdrAlias" onchange="checkFdralias()">
									<option>请选择</option>
								</select>
							</div>
						</div>
						<div class="col-md-10 col-lg-10" style="margin-top: 8px">
							<label class="setting-label">对应关系 </label>
							<div class="setting-info" id="relationDiv">
								<select class="form-control-select ng-pristine ng-valid ng-touched"
									ng-model="buildflow.package_name" autofocus tabindex="0"
									aria-invalid="false" style="width: 450px" id="relation"
									name="relation" onchange="checkRelation()">
									<option value=-1>请选择</option>
									<option value=1>一对一</option>
									<option value=2>一对多</option>
								</select>
							</div>
						</div>
						<div class="col-md-10 col-lg-10" style="margin-top: 8px;display:none;" id="indexDiv">
							<label class="setting-label">索引标识 </label>
							<div class="setting-info" id="indexAliasDiv">
								<input type="text"
									class="form-control ng-pristine ng-untouched ng-valid"
									id="indexAlias" name="indexAlias" onblur="checkIndexAlias()"
									placeholder="eg.bbbbIndex 本辅助sql需要使用索引表时，再填写;sql查询条件，使用分区字段">
							</div>
						</div>
						<div class="col-md-10 col-lg-10" style="margin-top: 8px;display:none;" id="index2Div">
							<label class="setting-label">索引sql </label>
							<div class="setting-info" id="indexSqlDiv">
								<textarea
									style="width: 450px; height: 80px; border: 1px solid #e5e5e5; color: #555"
									onblur="checkIndexSql()"
									placeholder="eg.select service_code from service_index a where a.user_code =  aaaa.code"
									id="indexSql" name="indexSql">
												</textarea>
							</div>
						</div>
						<div class="col-md-10 col-lg-10" style="margin-top: 8px">
							<label class="setting-label">sql </label>
							<div class="setting-info" id="fSqlDiv">
								<textarea
									style="width: 450px; height: 100px; border: 1px solid #e5e5e5; color: #555"
									onblur="checkFSql()"
									placeholder="eg.select service_code,service_name from service a where a"
									id="fsql" name="fsql">
									</textarea>
							</div>
						</div>

						<div style="float: right; display: inline-block; margin-top:80px">
							<button type="button" class="btn btn-default" id="saveFSqlBut"
								style="border-radius: 4px" onclick="saveFSql()">保存</button>
						</div>
						<div id="fsqlinfo"
							style="display: inline-block; text-align: center; width: 100%; margin: 5px 0px 0px 0px;"></div>
						<div style="display: inline-block;display:none; width: 100%;" id="fsqlDivs">
							<table class="table" id="sqlTable">
								<tbody>
									<tr>
										<th width="15%" id="sqlth1">sql标识</th>
										<th width="70%" id="sqlth2">sql语句</th>
										<th width="15%" id="sqlth3">操作</th>
									</tr>
								</tbody>
							</table>
						</div>
					</div>


					<div class="setting-section">
						<p class="info-block open">
							<i class="fa fa-info-circle"></i>依据配置，分别提取文档字段，逐条封装文档，批量提交到搜索服务器；搜索服务器中如果已有文档，会被更新。
						</p>
						<input type="hidden" name="groupId" value="2" /> <input
							type="hidden" name="uId" value="${manyds[0].uId }" id="uId" /> <input
							type="hidden" id="primaryKey" name="primaryKey" value="id" />
						<button type="button" data-loading-text="正在导入......"
							class="btn btn-lg btn-block project-creation-btn ng-binding"
							id="imp" onclick="startImport()">开始导入</button>
						<div id="info" class="biianx" style="overflow: scroll;"></div>
					</div>
				</div>
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
				<c:forEach items="${manyds}" var="item">
					initDisplay("${item.type}","${item.ip}", "${item.port}", "${item.sid}",
							"${item.username}", "${item.pwd}", "${item.database}","${item.user}",
							"${item.serviceId}","${item.servicePwd}","${item.vsql}"); 

				</c:forEach>
				
				dislpaySqlDs("${manysql.primarySql.drAlias}");
				dislpayPriSql("${manysql.primarySql.alias}","${manysql.primarySql.drAlias}",
						"${manysql.primarySql.primaryKey}","${manysql.primarySql.sql}");
				
				
				<c:forEach items="${manysql.filedSqls}" var="item">
					initFSql("${item.alias}","${item.drAlias}", "${item.relation}", "${item.sql}",
							"${item.indexAlias}", "${item.indexSql}","${item.mapObj}"); 
				</c:forEach>
				initNullFSql();
				initCommit(${fn:length(manyds)},${fn:length(manysql.filedSqls)});
		});
	
	
	
</script>
</html>
