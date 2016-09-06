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
<%
		response.setHeader("Access-Control-Allow-Origin", "*");		
		response.setHeader("Access-Control-Allow-Credentials", "true");	
		response.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
		response.setHeader("Access-Control-Allow-Headers", "Origin, No-Cache, X-Requested-With, If-Modified-Since, Pragma, Last-Modified, Cache-Control, Expires, Content-Type, X-E4M-With");
		response.setHeader("Content-Type", "text/html; charset=utf-8");
		
%>
<script type="text/javascript"
	src="${ctx}/resources/res/js/index.js"></script>
</head>
<body class="body-blue theme-clean global-color_sun page-signin">
	<div class="navbar navbar-inverse navbar-fixed-top" role="navigation">
		<div class="container">
			<div class="navbar-header">
				<a class="navbar-brand" href="#" style="height: 36px;"> <img
					style="height: 55px; margin-top: -16px" src="${ctx}/resources/res/images/logo_login.png">
				</a>
			</div>
			<div class="navbar-collapse collapse">
				<ul class="nav navbar-nav">
					<li class="dropdown"><a href="${ctx}/doc/" style="font-weight:700;font-size:16px;">文档</a></li>
				</ul>
			</div>
		</div>
	</div>




	<div class="wrapper body-inverse">
		<ng-view class="ng-scope">
		<div class="signin-container ng-scope">
			<h2 class="signin-head">登录</h2>

				<form id="signin-form_id" class="ng-pristine ng-valid">
		
					<div class="signin-form">
					<div class="errorMsg ng-scope" ng-if="errorMsg.show" style="display:none;">
						<!-- <alert type="danger">用户名或密码错误</alert> -->
						<div class="alert ng-isolate-scope alert-danger"
							ng-class="['alert-' + (type || 'warning'), closeable ? 'alert-dismissable' : null]"
							role="alert" type="danger">
							<div ng-transclude="">
								<span class="ng-binding ng-scopxe">用户名或密码错误</span>
							</div>
						</div>
		
					</div>

					<div class="form-group w-icon">
						<input type="text" id="userName" name="userName"
							class="form-control input-md ng-pristine ng-untouched ng-valid"
							placeholder="注册账号" value=""> <span
							class="fa fa-user signin-form-icon"></span>
					</div>

					<div class="form-group w-icon">
						<input type="text" id="serviceId" name="serviceId"
							class="form-control input-md ng-pristine ng-untouched ng-valid"
							placeholder="服务ID" value=""> <span
							class="fa fa-user signin-form-icon"></span>
					</div>


					<div class="form-group w-icon">
						<input type="password" id="servicePwd" name="servicePwd"
							class="form-control input-md ng-pristine ng-untouched ng-valid"
							placeholder="密码" value=""> <span
							class="fa fa-lock signin-form-icon"></span>
					</div>

					<div class="form-actions">
						<input type="button" value="登录" class="signin-btn bg-primary"
							 id="login"> 
					</div>

				</form>
			</div>
		</div>
		<!-- / Container --> </ng-view>
	</div>




	<div class="footer-wrapper">
		<!-- footer wrapper -->
		<hr>
		<div class="container">
			<footer>
				<ul class="list-inline pull-left">
					<li>版权所有</li>
				</ul>
				<span class="pull-right-xs text-muted">© SES..</span>
				<div class="clearfix"></div>
			</footer>
		</div>
		<!-- /container -->
	</div>
</body>
<script type="text/javascript">
	var urlInfo = "${urlInfo}";
	
	$(document).ready(function(){
		$("#login").click(function(){
			login();
		});
		
		$("#userName").keyup(function(){
			var userName = $("#userName").val();
			var serviceId = $("#serviceId").val();
			var password = $("#servicePwd").val();
			if(userName!="" && serviceId!="" && password!=""){
				$("#login").removeAttr("disabled");
			}else{
				$("#login").attr("disabled","disabled");
			}
		});
		$("#serviceId").keyup(function(){
			var userName = $("#userName").val();
			var serviceId = $("#serviceId").val();
			var password = $("#servicePwd").val();
			if(userName!="" && serviceId!="" && password!=""){
				$("#login").removeAttr("disabled");
			}else{
				$("#login").attr("disabled","disabled");
			}
		});
		$("#servicePwd").keyup(function(){
			var userName = $("#userName").val();
			var serviceId = $("#serviceId").val();
			var password = $("#servicePwd").val();
			if(userName!="" && serviceId!="" && password!=""){
				$("#login").removeAttr("disabled");
			}else{
				$("#login").attr("disabled","disabled");
			}
		});
		
		
		document.onkeydown=function(event){
            var e = event || window.event || arguments.callee.caller.arguments[0];
            if(e && e.keyCode==13){ // enter 键
            	 login();
            }
        }; 
        
        
        
        
        function login(){
    		var userName =$.trim($("#userName").val());
    		var serviceId = $.trim($("#serviceId").val());
    		var servicePwd =$.trim($("#servicePwd").val());
    		var url = "${ctx}/login/login";
    		$.ajax({
    			type : "post",
    			url : url,
    			data : {
    				"userName" : userName,
    				"serviceId" : serviceId,
    				"servicePwd"    : servicePwd
    			},
    			success : function(data) {
    				if(data.CODE =="1"){
    					var redirectUrl = urlInfo;
    					if(redirectUrl =="" || redirectUrl=="undefined"){
    						redirectUrl ="${ctx}";
    					}else{
    						redirectUrl = decodeURI(urlInfo);
    					}
    					window.location.href=redirectUrl;
    				}else{
    					if(data.CODE=="0"){
    						$(".errorMsg").fadeIn();
    					}
    				}
    			}
    		});
    	}
			
	});
	
	function gotoFocus(){
		$("#userName").focus();
	}
	
	
</script>
</html>
