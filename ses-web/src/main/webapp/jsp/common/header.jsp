<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%
	String ctx = request.getContextPath();
	request.setAttribute("ctx", ctx);
%>
<script type="text/javascript">
	var webPath = "${ctx}";
</script>
<link href="${ctx}/resources/res/css/bootstrap.min.css" rel="stylesheet"
	type="text/css">
<link href="${ctx}/resources/res/css/font-awesome.css" rel="stylesheet"
	type="text/css">
<link href="${ctx}/resources/res/css/main.css" rel="stylesheet"
	type="text/css">
<link href="${ctx}/resources/res/css/app.css" rel="stylesheet"
	type="text/css">
<link href="${ctx}/resources/nprogress/nprogress.css" rel="stylesheet" />
<script type="text/javascript"
	src="${ctx}/resources/res/js/jquery-1.11.1.min.js"></script>
<script type="text/javascript"
	src="${ctx}/resources/res/js/bootstrap.js"></script>
<script type="text/javascript"
	src="${ctx}/resources/res/js/tooltip.js"></script>
<script type="text/javascript"
	src="${ctx}/resources/js/ses/login.js"></script>
<script src="${ctx}/resources/nprogress/nprogress.js"></script>
