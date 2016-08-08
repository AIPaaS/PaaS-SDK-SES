<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>

<!DOCTYPE html>
<html lang="zh-cn">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<title>SES</title>
<%@include file="/jsp/common/header.jsp"%>
<link href="${ctx}/resources/res/css/css.css" rel="stylesheet" type="text/css">

<script type="text/javascript"
	src="${ctx}/resources/res/js/index.js"></script>
</head>
<body class="body-blue theme-clean global-color_sun page-signin">
	<div class="navbar navbar-inverse navbar-fixed-top" role="navigation">
		<div class="container">
			<div class="navbar-header">
				<a class="navbar-brand" href="${ctx}/" style="height: 36px;"> <img
					style="height: 55px; margin-top: -16px" src="${ctx}/resources/res/images/logo_login.png">
				</a>
			</div>
		</div>
	</div>




	<div class="wrapper body-inverse text-center" style="padding-top:15%;">
			
			<a id="wx" href="javascript:void(0);"> 
				<img style="height: 150px; border-radius: 100%;padding-top:10px;" src="${ctx}/resources/res/images/logo_login.png">
			</a>
			<h1>comming soon......</h1>
	</div>




	<div class="footer-wrapper">
		<!-- footer wrapper -->
		<hr>
		<div class="container">
			<footer>
				<ul class="list-inline pull-left">
					<li><a href="#">京ICP备140434342号-1</a></li>
				</ul>
				<span class="pull-right-xs text-muted">© SES</span>
				<div class="clearfix"></div>
			</footer>
		</div>
		<!-- /container -->
	</div>
</body>
<script type="text/javascript">
	
	$(document).ready(function(){
		$("#wx").on("click", function(){
			alert("干什么啊？不知道！");
		});
	});
        
	
	
</script>
</html>
