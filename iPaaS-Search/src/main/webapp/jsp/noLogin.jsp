<!DOCTYPE html>
<%@ page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%
	String ctx = request.getContextPath();
	request.setAttribute("ctx", ctx);
%>
<!--[if lt IE 7 ]> <html lang="en" class="ie6 ielt8"> <![endif]-->
<!--[if IE 7 ]>    <html lang="en" class="ie7 ielt8"> <![endif]-->
<!--[if IE 8 ]>    <html lang="en" class="ie8"> <![endif]-->
<!--[if (gte IE 9)|!(IE)]><!--> <html lang="en"> <!--<![endif]-->
<html>
	<link rel="stylesheet" type="text/css" href="${ctx}/resources/css/login.css"/>
	<script type="text/javascript" src="${ctx}/resources/js/jquery-1.10.2.min.js"></script>
	<script type="text/javascript" src="${ctx}/resources/js/jquery.form.min.js"></script>
	<script type="text/javascript" src="${ctx}/resources/js/jquery.validate.min.js"></script>
<body style="background :none">


<div style="display: none">
	<section id="content">
		<form id="loginF">
			<h1>Login</h1>
			<div>
				<input id="userName" name="userName" type="text"
					class="yon_input" placeholder="用户名" value="${userName }"/>
				<label id="userNameInvlid"></label>
			</div>
			<div>
				<input id="serviceId" name="serviceId" type="text" value="${sid }" class="yon_input" placeholder="服务id"/>
				<label id="serviceIdInvlid"></label>
			</div>
			<div>
				<input id="servicePwd" name="servicePwd" type="text" value="${pwd }" class="yon_input" placeholder="服务密码"/>
				<label id="servicePwdInvlid"></label>
			</div>
			<div>
				<input type="button"  id="login" value="Log in" />
				<label id="loginInvlid"></label>
			</div>
		</form>
	</section>
</div>

</body>

<script type="text/javascript">
	var urlInfo = "${urlInfo}";
	
	$(document).ready(function(){
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
							alert("用户名或密码错误");
						}
					}
				}
			});
		});
		
			
</script>





</html>