<%@ page language="java" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>首页</title>
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<%@include file="/jsp/common/header.jsp"%>
<link href="${ctx}/resources/nprogress/nprogress.css" rel="stylesheet" />
<%-- <link href="${ctx}/resources/bootstrap/dist/css/bootstrap.css"
	rel="stylesheet" />
<!--[if lte IE 8]><link rel="stylesheet" href="../../responsive-nav.css"><![endif]-->
<!--[if gt IE 8]><!--><link rel="stylesheet" href="${ctx}/resources/responsive-nav/demos/advanced-left-navigation/styles.css"><!--<![endif]-->
<script src="${ctx}/resources/responsive-nav/responsive-nav.js"></script> --%>
<script src="${ctx}/resources/nprogress/nprogress.js"></script>
<style type="text/css">
.intro-header {
	text-align: center;
	margin-top: 10%;
}

.list-inline {
	margin-top: 4%;
}
.wallpaper {
    height: 100%;
    width: 100%;
    position: fixed;
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
    z-index: -1;
    overflow: hidden
}

.wallpaper:after {
    content: "";
    display: block;
    height: 110%;
    width: 110%;
    position: absolute;
    top: -10px;
    right: -10px;
    bottom: -10px;
    left: -10px;
    background-image: url(resources/res/images/wallpaper.jpg);
    background-repeat: no-repeat;
    background-size: cover;
    -webkit-filter: blur(4px) grayscale(70%);
    filter: blur(4px) grayscale(70%)
}
</style>
</head>
<body>
	<!-- <div role="navigation" id="foo" class="nav-collapse">
      <ul>
        <li class="active"><a href="#">Home</a></li>
        <li><a href="#">About</a></li>
        <li><a href="#">Projects</a></li>
        <li><a href="#">Blog</a></li>
      </ul>
    </div>
    <a href="#nav" class="nav-toggle">Menu</a> -->
	<%--<div class="intro-header">
	 	<div class="container">

			<div class="row">
				<div class="col-lg-12">
					<div class="intro-message">
						<h1>Search Engine Service</h1>
						<hr class="intro-divider">
						<div class="spacer"></div>
						<ul class="list-inline intro-social-buttons">
							<li><a href="${ctx}/ses/mapping" id="mapping"
								class="btn btn-primary btn-lg"><i
									class="fa fa-download fa-fw"></i> <span class="network-name">创建数据模型&nbsp;</span></a>
							</li>
							<li><a href="${ctx}/dataimport/toDs" id="ds"
								class="btn btn-primary btn-lg"><i
									class="fa fa-download fa-fw"></i> <span class="network-name">数据倒入&nbsp;</span></a>
							</li>
							<li><a href="${ctx}/overview/" id="overview"
								class="btn btn-primary btn-lg"><i
									class="fa fa-download fa-fw"></i> <span class="network-name">数据查看&nbsp;</span></a>
							</li>
						</ul>

					</div>
				</div>
			</div>

		</div>

	</div> --%>
	<div class="wallpaper ng-scope"></div>
	<div class="container">
		<div class="index_top">
			<div class="logo">
				<img src="${ctx}/resources/res/images/logo.png" width="175px;">
			</div>
			<div class="index_run">
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

		<div class="container-fluid">
			<!-- <div class="cont_p">服务集成:
				创建和管理数据/SaaS服务，比如MySQL，Redis，NewRelic等</div> -->
			<div class="cont_p"></div>
			<div class="row">
				<div class="col-md-3">
					<a href="${ctx}/ses/mapping">
						<ul>
							<li class="cr_t"><i class="icon-wrench"></i></li>
							<li class="cr_b">模型构建</li>
						</ul>
					</a>
				</div>
				<div class="col-md-3">
					<a href="${ctx}/dataimport/toOne">
						<ul>
							<li class="cr_t"><i class="icon-home"></i></li>
							<li class="cr_b">简单模型数据导入</li>
						</ul>
					</a>
				</div>
				<div class="col-md-3">
					<a href="${ctx}/dataimport/toMany">
						<ul>
							<li class="cr_t"><i class="icon-sitemap"></i></li>
							<li class="cr_b">复杂模型数据导入</li>
						</ul>
					</a>
				</div>
				<div class="col-md-3">
					<a href="${ctx}/dic/index">
						<ul>
							<li class="cr_t"><i class="icon-tasks"></i></li>
							<li class="cr_b">词库管理</li>
						</ul>
					</a>
				</div>
				<div class="col-md-3">
					<a href="${ctx}/overview/overview">
						<ul>
							<li class="cr_t"><i class="icon-tasks"></i></li>
							<li class="cr_b">数据查看</li>
						</ul>
					</a>
				</div>

			</div>
		</div>
	</div>



</body>

<script type="text/javascript">
$(window).load(function() { 
	NProgress.done(); 
}); 
$(document).ready(function() { 
	NProgress.start(); 
}); 
	$(document).ready(function() {
		/* var navigation = responsiveNav("foo", {customToggle: ".nav-toggle"}); */
		/* var navigation = responsiveNav("#nav", { // Selector: The ID of the wrapper
			  animate: true, // Boolean: 是否启动CSS过渡效果（transitions）， true 或 false
			  transition: 400, // Integer: 过渡效果的执行速度，以毫秒（millisecond）为单位
			  label: "Menu", // String: Label for the navigation toggle
			  insert: "after", // String: Insert the toggle before or after the navigation
			  customToggle: "", // Selector: Specify the ID of a custom toggle
			  openPos: "relative", // String: Position of the opened nav, relative or static
			  jsClass: "js", // String: 'JS enabled' class which is added to <html> el
			  debug: false, // Boolean: Log debug messages to console, true 或 false
			  init: function(){}, // Function: Init callback
			  open: function(){}, // Function: Open callback
			  close: function(){} // Function: Close callback
			}); */
	});
</script>
</html>

