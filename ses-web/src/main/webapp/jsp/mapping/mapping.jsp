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
						<h3>欢迎进入模型构建</h3>
						<div>
							什么是 模型构建——是对搜索引擎中索引的字段及其数据类型进行定义，类似关系数据库中表建立时定义字段名及其数据类型，为避免脏数据，SES不支持动态索引定义
						</div>
						<h5>1.数据类型(type)</h5>
						<p>支持string, long, integer, short, byte, double, float, date, boolean, object(对象，实现嵌套复杂对象)</p>
						<h5>2.是否索引(indexed)</h5>
						<p>可选值:true/false   false为不对该字段进行索引（无法搜索，仅用于显示）,反之进行索引</p>
						<h5>3.是否分词(analyzed)</h5>
						<p>可选值:true/false	  是否要对该字段进行分词，如：中华人民共和国，将切分成中华、人民、共和国等，在搜索时输入上述词都将匹配</p>
						<h5>4.是否存储(stored)</h5>
						<p>可选值:true/false  是否存储，默认为false(不存储),一般不用设置，除非需对某个字段进行高亮显示，或索引数据非常大，单独对此字段进行检索时设置为True</p>
						<h5>5.是否聚合(agged)</h5>
						<p>可选值:true/false  是否需要聚合，默认为false(不聚合)。如果需要对全文搜索时，搜索条件根据查询内容进行聚合时设置为True,参照电商的全文索引。</p>
						<a id="creatMapping" class="btn btn-lg btn-success" href="${ctx}/ses/assembleMapping">管理模型</a>
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