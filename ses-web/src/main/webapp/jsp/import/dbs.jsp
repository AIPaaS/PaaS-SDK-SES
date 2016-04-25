<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>

<!DOCTYPE html>
<html lang="zh-cn">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport"
	content="width=device-width; initial-scale=0.8;  user-scalable=0;" />
<title>DBS数据导入</title>
<link href="${ctx}/resources/res/css/import.css" rel="stylesheet"
	type="text/css">
<%@include file="/jsp/common/header.jsp"%>
<script type="text/javascript"
	src="${ctx}/resources/js/jquery-1.10.2.min.js"></script>
<script type="text/javascript"
	src="${ctx}/resources/js/jquery.form.min.js"></script>


<script type="text/javascript">
	$(document).ready(function() {
		$("#user").blur(function() {
			var userVal = $.trim($("#user").val());
			if (userVal == '') {
				$("#userChk").css('visibility', 'visible');
			} else {
				$("#userChk").css('visibility', 'hidden');
			}
		});
		$("#serviceId").blur(function() {
			var serviceIdVal = $.trim($("#serviceId").val());
			if (serviceIdVal == '') {
				$("#serviceIdChk").css('visibility', 'visible');
			} else {
				$("#serviceIdChk").css('visibility', 'hidden');
			}
		});
		$("#servicePwd").blur(function() {
			var servicePwdVal = $.trim($("#servicePwd").val());
			if (servicePwdVal == '') {
				$("#servicePwdChk").css('visibility', 'visible');
			} else {
				$("#servicePwdChk").css('visibility', 'hidden');
			}
		});
		$("#Palias").blur(function() {
			var servicePwdVal = $.trim($("#Palias").val());
			if (servicePwdVal == '') {
				$("#chkPAlias").css('visibility', 'visible');
			} else {
				$("#chkPAlias").css('visibility', 'hidden');
			}
		});
		$("#primarySqlSelect").blur(function() {
			var servicePwdVal = $.trim($("#Palias").val());
			if (servicePwdVal == '') {
				$("#chkPrimarySqlSelect").css('visibility', 'visible');
			} else {
				$("#chkPrimarySqlSelect").css('visibility', 'hidden');
			}
		});
		$("#primaryKey").blur(function() {
			var servicePwdVal = $.trim($("#Palias").val());
			if (servicePwdVal == '') {
				$("#chkPrimaryKey").css('visibility', 'visible');
			} else {
				$("#chkPrimaryKey").css('visibility', 'hidden');
			}
		});
	});

	function dbsCfgClear() {
		$("#user").val("");
		$("#serviceId").val("");
		$("#servicePwd").val("");
	}

	function svDbsCfg() {
		if (dbsCfgOk()) {
			var userVal = $.trim($("#user").val());
			var dataSource;
			if (userVal.indexOf('@') > 0) {
				dataSource = userVal.split('@')[0];
			} else {
				dataSource = userVal;
			}
			var svsId = $.trim($("#serviceId").val());
			dataSource = dataSource + "_" + svsId;
			if (chkDbsTableTr(dataSource)) {
				var dbsTableTrCommon = "<td><span class='ng-binding'><button class='btn' "
						+ "type='button' onclick='testConnection(this);'>测试链接</button></span> <span class='ng-binding'>"
						+ "<button class='btn' type='button' onclick='editDbsInfoLine(this);'>编辑</button></span> <span "
			+"class='ng-binding'><button class='btn' type='button' id='123' onclick='delDbsInfoLine(this);'>删除</button>"
						+ "</span></td></tr>";
				var dataSourceInfo = dataSource + ";" + userVal + ";" + svsId
						+ ";" + $.trim($("#servicePwd").val());
				addDbsInfoLine(dataSourceInfo, dbsTableTrCommon);
			} else {
				if (confirm("数据源已存在，是否覆盖？")) {
					var dataSourceInfo = userVal + ";" + svsId + ";"
							+ $.trim($("#servicePwd").val());
					changeDbsInfo(dataSource, dataSourceInfo);
				}

			}
			dbsCfgClear();
		} else {

		}

	}

	function dbsCfgOk() {
		if ($.trim($("#user").val()) && $.trim($("#serviceId").val())
				&& $.trim($("#servicePwd").val())) {
			return true;
		}
		return false;
	}

	function addDbsInfoLine(dataSourceInfo, dbsTableTrCommon) {
		var dataSourceInfoArray = dataSourceInfo.split(";");
		var dataSourceName = dataSourceInfoArray[0];
		var userName = dataSourceInfoArray[1];
		var svsId = dataSourceInfoArray[2];
		var svsPwd = dataSourceInfoArray[3];
		var addContent = "<tr ng-repeat='item in ctrl.buildflows' class='ng-scope' id='"+dataSourceName+"'name='"+dataSourceName+"'>"
				+ "<td>"
				+ dataSourceName
				+ "<input type='text' id='V"+dataSourceName+"' value='"+userName+
					";"+svsId+";"+svsPwd+"'  />"
				+ "</td>" + dbsTableTrCommon;

		$('#dbsInfoTable').find('tbody').append(addContent);

	}

	function delDbsInfoLine(obj) {
		$(obj).parent("span").parent("td").parent("tr").remove();
	}

	function testConnection(obj) {
		var uss = $(obj).parent("span").parent("td").prev().find("input").val()
				.split(";");
		var userVal = uss[0];
		var svsId = uss[1];
		var svsPwd = uss[2];

	}

	function chkDbsTableTr(dsName) {
		var dsName = dsName;
		if ($('#' + dsName).attr('name')) {
			return false;
		}
		return true;
	}

	function changeDbsInfo(dsn, dsi) {
		$('#V' + dsn).val(dsi);
	}

	function editDbsInfoLine(obj) {
		var Vid = "#V"
				+ $(obj).parent("span").parent("td").parent("tr").attr('name');
		var dsiArr = $(Vid).val().split(";");
		$("#user").val(dsiArr[0]);
		$("#serviceId").val(dsiArr[1]);
		$("#servicePwd").val(dsiArr[2]);
	}

	function loadPrimarySelect() {
		var trs = $('#dbsInfoTable').find('tbody').children();
		$("#primarySqlSelect").empty();
		$("#primarySqlSelect").append("<option></option>");
		$.each(trs, function() {
			$("#primarySqlSelect").append(
					"<option id='" + $(this).attr('name') + "' value='"
							+ $(this).attr('name') + "'>"
							+ $(this).attr('name') + "</option>");
		});
	}
</script>

</head>
<body class="ui-v3 buildflow">
	<%@include file="/jsp/common/uselogin.jsp"%>

	<div ng-if="pageClass !== 'dashboard'" class="ng-scope">
		<div class="ui navbar ng-scope" ng-controller="NavbarCtrl">
			<ul class="nav nav-pills nav-stacked">
				<li class="logo"><a href="#"><i class="icon-qrcode"></i></a></li>
				<li class="logo logoa"><a href="#"><i class="icon-wrench"></i></a>
				</li>
				<li class="logo logoa"><a href="#"><i class="icon-home"></i></a>
				</li>
				<li class="logo logoa"><a href="#"><i class="icon-sitemap"></i></a>
				</li>
				<li class="logo logoa"><a href="#"><i class="icon-tasks"></i></a>
				</li>
				<li class="logo logoa"><a href="#"><i class="icon-cogs"></i></a>
				</li>
				<li class="logo logoa"><a href="#"><i class="icon-reorder"></i></a>
				</li>
			</ul>

			<ul class="nav nav-bottom nav-pills nav-stacked">
				<li class="logo logoa"><a href="#"><i
						class="icon-fighter-jet"></i></a></li>
				<li class="logo logoa"><a href="#"><i class="icon-user"></i></a>

				</li>
			</ul>
		</div>
	</div>


	<!-- 主体功能区 -->
	<div class="main ng-scope">
		<div class="dao-container ng-scope">
			<div class="panel panel-default panel-page-header">
				<div class="panel-body">
					<div class="primary-section"
						style="border: 0; margin: 0; padding-bottom: 0;">
						<h2>DBS数据导入</h2>
						<p></p>
					</div>
					<!-- 主体功能区 -->
					<div class="main ng-scope">
						<div class="dao-container ng-scope">
							<div class="panel panel-default">
								<div class="panel-body">
									<form id="one">
										<div class="setting-section">
											<h4>DBS数据库配置</h4>
											<div class="col-md-10 col-lg-10" style="margin-top: 15px">
												<label class="setting-label">云账号 </label>
												<div class="setting-info" id="ipDiv">
													<span><input
														class="form-control ng-pristine ng-valid ng-touched"
														type="text" placeholder="用户账号，如：majh5@asiainfo.com"
														ng-model="buildflow.package_name" onblur="checkIp()"
														autofocus tabindex="0" aria-invalid="false" name="user"
														id="user"></span>&nbsp;&nbsp;&nbsp; <span
														style="color: #FF0000; visibility: hidden;" id="userChk">云账号非法</span>
												</div>
											</div>
											<div class="col-md-10 col-lg-10" style="margin-top: 15px">
												<label class="setting-label">服务ID </label>
												<div class="setting-info" id="ipDiv">
													<span><input
														class="form-control ng-pristine ng-valid ng-touched"
														type="text" placeholder="服务ID..."
														ng-model="buildflow.package_name" onblur="checkIp()"
														autofocus tabindex="0" aria-invalid="false"
														name="serviceId" id="serviceId"></span>
													&nbsp;&nbsp;&nbsp; <span
														style="color: #FF0000; visibility: hidden;"
														id="serviceIdChk">服务ID非法</span>
												</div>
											</div>
											<div class="col-md-10 col-lg-10" style="margin-top: 15px">
												<label class="setting-label">服务密码 </label>
												<div class="setting-info" id="portDiv">
													<span><input
														class="form-control ng-pristine ng-valid ng-touched"
														type="password" placeholder="服务密码..."
														ng-model="buildflow.package_name" onblur="checkPort()"
														autofocus tabindex="0" aria-invalid="servicePwd"
														name="servicePwd" id="servicePwd"></span>&nbsp;&nbsp;&nbsp;
													<span style="color: #FF0000; visibility: hidden;"
														id="servicePwdChk">服务密码非法</span>

												</div>
											</div>

											<div class="col-md-10 col-lg-10" style="margin-top: 15px">
												<div class="setting-info" id="portDiv">
													<tr>
														<th><button class="btn" type="button"
																onclick="svDbsCfg();">保存</button></th>
														<th><button class="btn" type="button"
																onclick="dbsCfgClear();">取消</button></th>
													</tr>
												</div>
											</div>

											<div class="panel panel-default ng-scope"
												ng-if="ctrl.buildflows.length">
												<div class="panel-body">
													<table class="table" id="dbsInfoTable">
														<thead>
															<tr>
																<th>数据源</th>
																<th>操作</th>
															</tr>
														</thead>
														<tbody>

														</tbody>
													</table>
												</div>
											</div>
										</div>
									</form>
								</div>
							</div>
						</div>
					</div>

					<div class="main ng-scope">
						<div class="dao-container ng-scope">
							<div class="panel panel-default">
								<div class="panel-body">
									<form id="one">
										<div class="setting-section">
											<h4>SQL配置</h4>
											<div class="col-md-10 col-lg-10" style="margin-top: 15px">
												<label class="setting-label">主体SQL配置： </label>
											</div>
											<div class="col-md-10 col-lg-10" style="margin-top: 15px">
												<label class="setting-label">SQL别名 </label>
												<div class="setting-info" id="ipDiv">
													<span> <input
														class="form-control ng-pristine ng-valid ng-touched"
														type="text" placeholder="SQL别名"
														ng-model="buildflow.package_name" onblur="checkIp()"
														autofocus tabindex="0" aria-invalid="false" name="Palias"
														id="Palias">
													</span> <span> 其余SQL配置时可能会使用到 </span> <br /> <span
														style="color: #FF0000; visibility: hidden;" id="chkPAlias">SQL别名非法</span>
												</div>
											</div>
											<div class="col-md-10 col-lg-10" style="margin-top: 15px">
												<label class="setting-label">数据源 </label>
												<div class="setting-info" id="ipDiv">
													<span><select name="primarySqlSelect"
														id="primarySqlSelect"
														class="form-control ng-pristine ng-valid ng-touched"
														ng-model="buildflow.package_name" autofocus tabindex="0"
														aria-invalid="false" style="max-width: 450px"
														onFocus="loadPrimarySelect();">
															<option></option>
													</select> <br /> <span style="color: #FF0000; visibility: hidden;"
														id="chkPrimarySqlSelect">数据源非法</span>
												</div>
											</div>
											<div class="col-md-10 col-lg-10" style="margin-top: 15px">
												<label class="setting-label">主键 </label>
												<div class="setting-info" id="portDiv">
													<span> <input
														class="form-control ng-pristine ng-valid ng-touched"
														type="text" placeholder="主键字段..."
														ng-model="buildflow.package_name" onblur="checkPort()"
														autofocus tabindex="0" aria-invalid="servicePwd"
														name="primaryKey" id="primaryKey">
													</span> <span> 主表主键便于异常提示 </span> <br /> <span
														style="color: #FF0000; visibility: hidden;"
														id="chkPrimaryKey">SQL主键非法</span>
												</div>
											</div>


											<div class="col-md-10 col-lg-10" style="margin-top: 15px">
												<div class="setting-info" id="portDiv">
													<tr>
														<td><textarea class="form-control" rows="5"
																placeholder="select ... from ..."></textarea></td>
													</tr>
												</div>
											</div>

											<div class="col-md-10 col-lg-10" style="margin-top: 15px">
												<div class="setting-info" id="portDiv">
													<tr>
														<th><button class="btn" type="button">保存</button></th>
														<th><button class="btn" type="button">删除</button></th>
													</tr>
												</div>
											</div>
									</form>

								</div>
								<form id="one">
									<div class="setting-section">
										<div class="col-md-10 col-lg-10" style="margin-top: 15px">
											<label class="setting-label">辅助SQL配置： </label>
										</div>
										<div class="col-md-10 col-lg-10" style="margin-top: 15px">
											<label class="setting-label">SQL别名 </label>
											<div class="setting-info" id="ipDiv">
												<span> <input
													class="form-control ng-pristine ng-valid ng-touched"
													type="text" placeholder="SQL别名"
													ng-model="buildflow.package_name" onblur="checkIp()"
													autofocus tabindex="0" aria-invalid="false" name="user"
													id="user">
												</span> <span> 其余SQL配置时可能会使用到 </span>
											</div>
										</div>
										<div class="col-md-10 col-lg-10" style="margin-top: 15px">
											<label class="setting-label">数据源 </label>
											<div class="setting-info" id="ipDiv">
												<select name="database" id="database"
													class="form-control ng-pristine ng-valid ng-touched"
													ng-model="buildflow.package_name" autofocus tabindex="0"
													aria-invalid="false" style="max-width: 450px">
													<option value="1">MYSQL</option>
													<option value="2">ORACLE</option>
												</select>
											</div>
										</div>
										<div class="col-md-10 col-lg-10" style="margin-top: 15px">
											<label class="setting-label">主键 </label>
											<div class="setting-info" id="portDiv">
												<span> <input
													class="form-control ng-pristine ng-valid ng-touched"
													type="text" placeholder="主键字段..."
													ng-model="buildflow.package_name" onblur="checkPort()"
													autofocus tabindex="0" aria-invalid="servicePwd"
													name="servicePwd" id="port">
												</span> <span> 主表主键便于异常提示 </span>

											</div>
										</div>


										<div class="col-md-10 col-lg-10" style="margin-top: 15px">
											<div class="setting-info" id="portDiv">
												<tr>
													<td><textarea class="form-control" rows="5"
															placeholder="select ... from ..."></textarea></td>
												</tr>
											</div>
										</div>

										<div class="col-md-10 col-lg-10" style="margin-top: 15px">
											<div class="setting-info" id="portDiv">
												<tr>
													<th><button class="btn" type="button">保存</button></th>
													<th><button class="btn" type="button">删除</button></th>
												</tr>
											</div>
										</div>
								</form>
							</div>
						</div>
					</div>
				</div>
			</div>
		</div>
	</div>
	<div class="help">
		<a href="#"><i class="icon-comment"></i><span>?</span></a>
	</div>


</body>
</html>
