<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%
	String ctx = request.getContextPath();
	request.setAttribute("ctx", ctx);
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>模型配置</title>
<!-- <style>
.hidden {
	display: none;
}
</style> -->
<link href="${ctx}/resources/jsonmate/jsoneditor.css" rel="stylesheet" />
<link rel="stylesheet" type="text/css"
	href="${ctx}/resources/css/ses.css" />
<link href="${ctx}/resources/bootstrap/dist/css/bootstrap.css"
	rel="stylesheet" />
</head>
<body>
	<!-- header begin-->
	<div class="head">
		<div class="head-left" style="">
			<a href="${ctx}" style="color: white">主页</a> <span class="divider">/</span>创建数据模型
		</div>
		<div class="head-right" onclick="LogOut()" style="cursor: pointer">${ SES_USER["userName"]}</div>
	</div>

	<!--页脚-->

	<div class="intro-header">
		<div class="container">

			<div class="row">
				<div class="col-lg-12">
					<div class="intro-message">
						<h1>Search Engine Service</h1>
						<hr class="intro-divider">
						<div class="spacer"></div>
						<ul class="list-inline intro-social-buttons">
							<li><span class="network-name">创建数据模型成功！</span>
							</li>
						</ul>

					</div>
				</div>
			</div>

		</div>
		<!-- /.container -->

	</div>
	<script src="${ctx}/resources/js/jquery-1.10.2.min.js"></script>
	<script src="${ctx}/resources/bootstrap/dist/js/bootstrap.js"></script>
	<script type="text/javascript">
		
	</script>
</body>
</html>