<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>数据查看</title>
<!-- <style>
.hidden {
	display: none;
}
</style> -->
<%@include file="/jsp/common/header.jsp"%>
<link rel="stylesheet" type="text/css"
	href="${ctx}/resources/css/ses.css" />
<link href="${ctx}/resources/bootstrap/dist/css/bootstrap.css"
	rel="stylesheet" />
<style>
</style>

</head>
	<body class="ui-v3 buildflow">
	<!-- header begin-->
	
	
	<nav class="navbar dao-navbar ng-scope" >
	<div class="clearfix dao-container">
		<div class="navbar-header">
			<div class="back-link ng-scope">
				<div class="ng-binding ng-scope">
					<div class="daima">数据查看</div>
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
	
	<!-- header end-->
		<div class="ui dao-container ng-scope">
			<div class="panel panel-default panel-page-header">
				<iframe src="${addr}"
					frameborder="0" marginheight="0" marginwidth="0" frameborder="0"
					scrolling="auto" id="ifm" name="ifm" width="100%" height="700px;"> </iframe>
			</div>
		</div>

	<!--页脚-->


	<script src="${ctx}/resources/js/jquery-1.10.2.min.js"></script>
	<script src="${ctx}/resources/jsonmate/json2.js"></script>
	<script src="${ctx}/resources/bootstrap/dist/js/bootstrap.js"></script>
	<script type="text/javascript">
		
	</script>
</body>
</html>