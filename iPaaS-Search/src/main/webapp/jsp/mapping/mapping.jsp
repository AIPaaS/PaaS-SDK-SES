<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>模型配置</title>

<%@include file="/jsp/common/header.jsp"%>

<style type="text/css">
.buildflow h4{color:#aab2bd;}
.primary-section p{margin-left:4%;}
</style>
<script type="text/javascript">
	//var mapping = ${mapping};
</script>
</head>
<body class="ui-v3 buildflow">
	<nav class="navbar dao-navbar ng-scope" >
	<div class="clearfix dao-container">
		<div class="navbar-header">

			<div class="back-link ng-scope">

				<div class="ng-binding ng-scope">
					<div class="daima">模型构建</div>
					<ul class="nav navbar-nav navbar-right">
						<li><a href="${ctx}/doc/"><i class="icon-file-alt"></i> 文档</a></li>
						<li class="dropdown"><a href="#" class="dropdown-toggle"
							data-toggle="dropdown" role="button" aria-haspopup="true"
							aria-expanded="false"><span class="icon-user"></span>
								${ SES_USER["userName"]} <span class="icon-angle-down"></span></a>
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
	<div class="main ng-scope">
		<div class="ui dao-container ng-scope">

			<div class="panel panel-default panel-page-header">
				<div class="panel-body">
					<div class="primary-section">
						<h2>欢迎进入模型构建</h2>
						<div>
							什么是 模型构建 ?
						</div>
						<br>
						<p>模型构建就是对索引库中索引的字段名及其数据类型进行定义，类似关系数据库中表建立时定义字段名及其数据类型</p>
						<h4>1.数据类型(type)</h4>
						<p>string, long, integer, short, byte, double, float, date, boolean, object</p>
						<h4>2.是否索引(index)</h4>
						<p>可选值:true/false   false为不对该字段进行索引（无法搜索）,反之亦然</p>
						<h4>3.是否分词(analyze)</h4>
						<p>可选值:true/false</p>
						<h4>4.是否存储(store)</h4>
						<p>可选值:true/false   是否存储索引的字段，默认为false(不存储),一般我们不用设置{"store":true}，除非，我们需要对某个域（就是字段）进行高亮显示，或者，索引数据非常大，为了提高检索性能，单独对此索引字段进行检索</p>
						<a id="creatMapping" class="btn btn-lg btn-success" href="${ctx}/ses/assembleMapping">创建新模型</a>
					</div>
					<div class="secondary-section">
						<div class="row">
							<div class="col-xs-12 col-sm-12">
								<h4>模型字段包含object?</h4>
								<p>
									如果您不清楚如何配置嵌套一层或多层object的复杂数据模型,请参考我们的用户手册: <a href="${ctx}/doc/"
										style="font-weight: 600;">配置复杂模型</a>
								</p>
							</div>
						</div>
					</div>
				</div>
			</div>

			<div class="panel panel-default ng-scope">
				<div class="panel-body">
					<table class="table">
						<tbody>
							<tr>
								<th>索引名称</th>
								<th>服务ID</th>
								<th>最近更新</th>
								<th>状态</th>
								<th>操作</th>
							</tr>
							<tr  class="ng-scope">
								<td>${indexDisplay}</td>
								<td>${serviceId}</td>
								<td>${updateTime}</td>
								<td style="color:#5cb85c;">启用</td>
								<td><a href="${ctx}/ses/assembleMapping">查看模型</a></td>
							</tr>
						</tbody>
					</table>
				</div>
			</div>

		</div>
	</div>
	<script type="text/javascript">
		$(window).load(function() {
			NProgress.done();
		});
		$(document).ready(function() {
			NProgress.start();
			$("#creatMapping").on("click",function(){
				
				
			});
		});
	</script>
</body>


</html>