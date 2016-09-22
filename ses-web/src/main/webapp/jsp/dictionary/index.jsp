<%@ page language="java" pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<html lang="zh-cn">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport"
	content="width=device-width; initial-scale=0.8;  user-scalable=0;" />
<title>词典导入</title>
<%@include file="/jsp/common/header.jsp"%>
<link rel="stylesheet" href="${ctx}/resources/libs/bootstrap/css/bootstrap.min.css">
<link rel="stylesheet" href="${ctx}/resources/bootstrap/css/bootstrap-theme.min.css">
<link rel="stylesheet" type="text/css" href="${ctx}/resources/css/ses.css"/>
<link href="${ctx}/resources/css/bootstrap-modal.css" rel="stylesheet">
<script type="text/javascript" src="${ctx}/resources/js/jquery.form.min.js"></script>
<script type="text/javascript" src="${ctx}/resources/js/dic.js"></script>
</head>
<body class="ui-v3 buildflow">
	<%-- <%
response.sendRedirect("dataimport/toDs");

%> --%>


<nav class="navbar dao-navbar ng-scope" >
	<div class="clearfix dao-container">
		<div class="navbar-header">
			<div class="back-link ng-scope">
				<div class="ng-binding ng-scope">
					<div class="daima">词典维护</div>
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
	
	<div>
		<div class="dao-container ng-scope">
			<div class="panel panel-default panel-page-header">
				<div class="panel-body">
					<div class="primary-section"
						style="border: 0; margin: 0; padding-bottom: 0;">
						<p>导入用户词典及停用词词典，提供词典维护，导入成功后，即可在搜索中是用自定义词语</p>
						<p><strong>上传词库必须为UTF-8编码且无BOM格式</strong></p>
					</div>
				</div>
			</div>
			<div class="panel panel-default panel-page-header" >
				<div class="panel-body">
					<div class="primary-section"
						style="border: 0; margin: 0; padding-bottom: 0;">
						<h4>当前热词词库</h4>
						<c:forEach items="${allIndexWordList }" var="indexWord" begin="0" end="2">
						<p style="margin-bottom: 0px;">${ indexWord.word}</p>
						</c:forEach>
						<p style="margin-bottom: 0px;"><a href="javascript:void(0);" onclick="javascript:moreIndexWord();">[...]</a></p>
					</div>
				</div>
			</div>
			<div id="indexWordArea"  class="modal" aria-hidden="false" style="display:none;z-index: 1041;height: 460px;">
				
				<div class="modal-dialog modal-lg">
			    <div class="modal-content">
			      <div class="modal-header">
			        <button type="button" class="close" data-dismiss="modal" aria-hidden="true">×</button>
			        <h3 class="modal-title">热词词库预览</h3>
			      </div>
			      
			      <div class="modal-body">
			        <textarea id="allIndexWords" class="form-control"  disabled="disabled" style="font-family:Monaco,Consolas,monospace; height: 253.5px;" data-role="tagsinput"><c:forEach items="${allIndexWordList }" var="indexWord" >${ indexWord.word} </c:forEach>
			        </textarea>
					
			        <nav aria-label="Page navigation">
					<nav aria-label="...">
					  <ul class="pager">
					    <li><a id="indexPrePage" href="javascript:void(0);" onclick="javascript:preIndexWord();">上一页</a></li>
					    <li><a id="indexNextPage"href="javascript:void(0);" onclick="javascript:nextIndexWord();">下一页</a></li>
					  </ul>
					 </nav>
					 </nav>
			      </div>
			    </div>
				</div>
			</div>
			<div class="panel panel-default panel-page-header">
				<div class="panel-body">
					<div class="primary-section"
						style="border: 0; margin: 0; padding-bottom: 0;">
						<h5>当前停用词词库</h5>
						<c:forEach items="${allStopWordList }" var="stopWord"  begin="0" end="2">
						<p style="margin-bottom: 0px;">${ stopWord.word}</p>
						</c:forEach>
						<p style="margin-bottom: 0px;"><a href="javascript:void(0);" onclick="javascript:moreStopWord();">[...]</a></p>
					</div>
				</div>
			</div>
			<div id="stopWordArea"  class="modal" aria-hidden="false" style="display:none;z-index: 1041">
			  <div class="modal-dialog modal-lg">
			    <div class="modal-content">
			      <div class="modal-header">
			        <button type="button" class="close" data-dismiss="modal" aria-hidden="true">×</button>
			        <h3 class="modal-title">停用词词库预览</h3>
			      </div>
			      <div class="modal-body">
			        <textarea id="allStopWords" class="form-control" disabled="disabled" style="font-family:Monaco,Consolas,monospace; height: 253.5px;" readonly=""><c:forEach items="${allStopWordList }" var="stopWord">${ stopWord.word} </c:forEach>
			        </textarea>
			        			        <nav aria-label="Page navigation">
					<nav aria-label="...">
					  <ul class="pager">
					    <li><a id="stopPrePage" href="javascript:void(0);" onclick="javascript:preStopWord();">上一页</a></li>
					    <li><a id="stopNextPage"href="javascript:void(0);" onclick="javascript:nextStopWord();">下一页</a></li>
					  </ul>
					 </nav>
					 </nav>
			      </div>
			    </div>
				</div>
			</div>
			<div class="panel panel-default panel-page-header" >
				<div class="panel-body">
					<div class="primary-section"
						style="border: 0; margin: 0; padding-bottom: 0;">
						<h4>当前分词情况</h4>
						<br/><br/><b>请请输入分词内容：</b><br/>
						<textarea rows="2" cols="20" style="width: 365px; border:solid 1px #000000; " id="inputText" class="textarea input-sm"></textarea>
						
						<br/><br/><b>分词后的展示内容：</b><br/>
						<textarea rows="2" cols="20" style="width: 365px; border:solid 1px #000000; "  id="showText"  class="textarea input-sm"></textarea>
						
						<div	 style="width:300px;text-align:center;">
						<input type="hidden"  id="addr"  value="${ addr}"/>
						<input type="hidden"  id="filds"  value="${ filds}"/>
						<input type="hidden"  id="indexName"  value="${ indexName}"/>
						<br/><input type="button" name="fildBut" id="fildBut" value="确认"  onclick="elasticsearchFilds()" class="btn btn-sm project-creation-btn ng-binding"/>
						</div>
					</div>
				</div>
			</div>
			<div class="panel panel-default">
				<div class="panel-body">
					<form id="indexDicForm" class="project-form ng-pristine ng-valid dic">
						
						<div class="setting-section">
							<div class="col-md-10 col-lg-10">
								<label class="setting-label">热词词库上传</label>
								<div class="setting-info">
				                    <input id="indexWord" type="file" name = "indexWord" class="form-control ng-pristine ng-valid ng-touched" ng-model="buildflow.package_name" 
										autofocus tabindex="0"  multiple=true>
				                </div>
							</div>
						</div>
						
						<div class="setting-section">
							<div class="col-md-10 col-lg-10">
								<label class="setting-label">停用词词库上传</label>
								<div class="setting-info">
				                    <input id="stopWord" type="file" name ="stopWord" class="form-control ng-pristine ng-valid ng-touched" ng-model="buildflow.package_name" 
										autofocus tabindex="0"  multiple=true>
				                </div>
							</div>
						</div>
					</form>
					<div class="setting-section">
						<p class="info-block open">
							<i class="fa fa-info-circle"></i>词典导入文件只能是TXT格式文件，单词之间要采用换行来区分
						</p>
						</br>
						<p align="center">
						<button id="dicSaveBtn"
							class="btn btn-sm project-creation-btn ng-binding">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;保存&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</button>
						<button id="indexClearBtn"
							class="btn btn-sm project-creation-btn ng-binding">清空热词词库</button>
						<button id="stopClearBtn"
							class="btn btn-sm project-creation-btn ng-binding">清空停用词词库</button>	
						<button id="engineUpdateBtn"
							class="btn btn-sm project-creation-btn ng-binding">更新搜索词库</button>	
						</p>		
					</div>
					<div class="setting-section">
						<div id="submitInfo" class="alert alert-success" role="alert" style="display:block;height: 100px;"></div>
					</div
				</div>
			</div>
		</div>
	</div>






<div class="help">
		<a href="#"><i class="icon-comment"></i><span>?</span></a>
	</div>


	<div class="xiaox">
		<div class="p">
			<a class="pull-right text-muted"><i class="icon-remove"></i></a>
			DaoCloud
		</div>
		<div class="box-row">
			<div class="box-cell">
				<div class="box-inner">
					<div class="list-group no-radius no-borders">
						<a class="list-group-item p-h-md p-v-xs"> <i
							class="icon-circle text-success text-xs m-r-xs"></i> <span>DaoCloud1</span>
						</a> <a class="list-group-item p-h-md p-v-xs"> <i
							class="icon-circle text-success text-xs m-r-xs"></i> <span>商品发布信息2</span>
						</a> <a class="list-group-item p-h-md p-v-xs"> <i
							class="icon-circle text-warning text-xs m-r-xs"></i> <span>商品发布信息3</span>
						</a> <a class="list-group-item p-h-md p-v-xs"> <i
							class="icon-circle text-muted-lt text-xs m-r-xs"></i> <span>商品发布信息4</span>
						</a> <a class="list-group-item p-h-md p-v-xs"> <i
							class="icon-circle text-muted-lt text-xs m-r-xs"></i> <span>商品发布信息5</span>
						</a> <a class="list-group-item p-h-md p-v-xs"> <i
							class="icon-circle text-muted-lt text-xs m-r-xs"></i> <span>商品发布信息6</span>
						</a> <a class="list-group-item p-h-md p-v-xs"> <i
							class="icon-circle text-muted-lt text-xs m-r-xs"></i> <span>商品发布信息7</span>
						</a> <a class="list-group-item p-h-md p-v-xs"> <i
							class="icon-circle text-muted-lt text-xs m-r-xs"></i> <span>商品发布信息</span>
						</a> <a class="list-group-item p-h-md p-v-xs"> <i
							class="icon-circle text-muted-lt text-xs m-r-xs"></i> <span>商品发布信息9</span>
						</a> <a class="list-group-item p-h-md p-v-xs"> <i
							class="icon-circle text-muted-lt text-xs m-r-xs"></i> <span>商品发布信息10</span>
						</a>
					</div>
				</div>
			</div>
		</div>
	</div>
	 <div id="big" style="z-index: 10000;" name="big">
    </div>
</body>

<script type="text/javascript">
$(function(){
	
	$("#indexClearBtn").css("background","#00ADEF");
	$("#indexClearBtn").attr("onclick","clearIndexWords()");
	$("#stopClearBtn").css("background","#00ADEF");
	$("#stopClearBtn").attr("onclick","clearStopWords()");
	$("#engineUpdateBtn").css("background","#00ADEF");
	$("#engineUpdateBtn").attr("onclick","updateEngine()");
	
	
	$("#indexWord").change(function(){
		var indexWord = $("#indexWord").val();
		var stopWord = $("#stopWord").val();
		if(indexWord!=""){
			$("#dicSaveBtn").css("background","#00ADEF");
			$("#dicSaveBtn").attr("onclick","saveDic()");
		}else if(stopWord==""){
			$("#dicSaveBtn").css("background","#DDD");
			$("#dicSaveBtn").removeAttr("onclick");
		}
		
	})
	
	$("#stopWord").change(function(){
		var stopWord = $("#stopWord").val();
		var indexWord = $("#indexWord").val();
		if(stopWord !=""){
			$("#dicSaveBtn").css("background","#00ADEF");
			$("#dicSaveBtn").attr("onclick","saveDic()");
		}else if(indexWord==""){
			$("#dicSaveBtn").css("background","#DDD");
			$("#dicSaveBtn").removeAttr("onclick");
		}
		
	})
	
});


function moreStopWord(){
	
	$("#stopWordArea").modal("show");
	
}

function moreIndexWord(){
	$('#indexWordArea').modal('show');
}
function saveAllIndexWords(){
var param = $("#allIndexWords").val();

	var url = "${ctx}/dic/saveAllIndex";
		$("#indexSaveBtn").css("background","#DDD");
		$("#indexSaveBtn").removeAttr("onclick").html("正在保存中……");
		
		$.ajax({
	    	   type:"POST",
	    	   url:"${ctx}/dic/saveAllIndexWords",	
			   dataType : "json",
			   data : {words:param},
			   success:function(msg){
				   $("#big").hide();
				   $("#indexSaveBtn").html("保存");
				   $("#indexSaveBtn").css("background","#00ADEF");
				   $("#indexSaveBtn").attr("onclick","saveAllIndexWords()");
				   if (msg == '1') {
					   $("#submitInfo").removeClass("alert-fail-info").addClass("alert-success-info").html("保存成功").fadeIn();
				   }else{
					   $("#submitInfo").removeClass("alert-success-info").addClass("alert-fail-info").html("保存失败").fadeIn();
				   }
			   }
	       }); 
		
}
var indexStart=1;
var stopStart=1;
function preIndexWord(){
	var url = "${ctx}/dic/getIndexWords";
	indexStart = indexStart - 1;
	if(indexStart <= 0)
		indexStart = 1;
	
	$.ajax({
    	   type:"POST",
    	   url:url,	
		   dataType : "text",
		   data : {start:indexStart,rows:500},
		   success:function(msg){
			   $("#allIndexWords").val(msg);
		   }
       }); 
}

function nextIndexWord(){
	var url = "${ctx}/dic/getIndexWords";
	indexStart = indexStart +1;
	$.ajax({
    	   type: "POST",
    	   url: url,	
		   dataType : "text",
		   data : {start:indexStart,rows:500},
		   success:function(msg){
			   if(msg ==null || msg =="" || msg == "null"){
				   indexStart = indexStart -1;
			   }
			   else
			   	 $("#allIndexWords").val(msg);
		   }
       }); 
}

function preStopWord(){
	var url = "${ctx}/dic/getStopWords";
	stopStart = stopStart - 1;
	if(stopStart <= 0)
		stopStart = 1;
	
	$.ajax({
    	   type:"POST",
    	   url:url,	
		   dataType : "text",
		   data : {start:stopStart,rows:500},
		   success:function(msg){
			   $("#allStopWords").val(msg);
		   }
       }); 
}

function nextStopWord(){
	var url = "${ctx}/dic/getStopWords";
	stopStart = stopStart +1;
	$.ajax({
    	   type: "POST",
    	   url: url,	
		   dataType : "text",
		   data : {start:stopStart,rows:500},
		   success:function(msg){
			   if(msg ==null || msg =="" || msg == "null"){
				   stopStart = stopStart -1;
			   }
			   else
			   	 $("#allStopWords").val(msg);
		   }
       }); 
}

function clearIndexWords(){
	var url = "${ctx}/dic/clearIndexWords";
	$("#indexClearBtn").css("background","#DDD");
	$("#indexClearBtn").removeAttr("onclick").html("正在清除中……");
	$.ajax({
    	   type: "POST",
    	   url: url,	
		   dataType : "json",
		   data : {},
		   success:function(msg){
			   $("#big").hide();
			   $("#indexClearBtn").html("清除热词词库");
			   $("#indexClearBtn").css("background","#00ADEF");
			   $("#indexClearBtn").attr("onclick","clearIndexWords()");
			   if (msg == '1') {
				   $("#submitInfo").removeClass("alert-fail-info").addClass("alert-success-info").html("清除热词词库成功").fadeIn();
			   }else{
				   $("#submitInfo").removeClass("alert-success-info").addClass("alert-fail-info").html("清除热词词库成功失败").fadeIn();
			   }			   
		   }
       }); 
}

function clearStopWords(){
	var url = "${ctx}/dic/clearStopWords";
	$("#stopClearBtn").css("background","#DDD");
	$("#stopClearBtn").removeAttr("onclick").html("正在清除中……");
	$.ajax({
    	   type: "POST",
    	   url: url,	
		   dataType : "json",
		   data : {},
		   success:function(msg){
			   $("#big").hide();
			   $("#stopClearBtn").html("清除停用词词库");
			   $("#stopClearBtn").css("background","#00ADEF");
			   $("#stopClearBtn").attr("onclick","clearIndexWords()");
			   if (msg == '1') {
				   $("#submitInfo").removeClass("alert-fail-info").addClass("alert-success-info").html("清除停用词词库成功").fadeIn();
			   }else{
				   $("#submitInfo").removeClass("alert-success-info").addClass("alert-fail-info").html("清除停用词词库失败").fadeIn();
			   }		
		   }
       }); 
}

function updateEngine(){
	var url = "${ctx}/dic/updateEngineWords";
	$("#engineUpdateBtn").css("background","#DDD");
	$("#engineUpdateBtn").removeAttr("onclick").html("正在清除中……");
	$.ajax({
    	   type: "POST",
    	   url: url,	
		   dataType : "json",
		   data : {},
		   success:function(msg){
			   $("#big").hide();
			   $("#engineUpdateBtn").html("更新搜索引擎词库");
			   $("#engineUpdateBtn").css("background","#00ADEF");
			   $("#engineUpdateBtn").attr("onclick","clearIndexWords()");
			   if (msg == '1') {
				   $("#submitInfo").removeClass("alert-fail-info").addClass("alert-success-info").html("更新搜索引擎词库成功").fadeIn();
			   }else{
				   $("#submitInfo").removeClass("alert-success-info").addClass("alert-fail-info").html("更新搜索引擎词库失败").fadeIn();
			   }		
		   }
       }); 
}

var clearFileInput = function (input) {
	  if (!input) {
	    return;
	  }

	  // standard way - works for IE 11+, Chrome, Firefox, webkit Opera
	  input.value = null;

	  if (input.files && input.files.length && input.parentNode) {
	    // workaround for IE 10 and lower, pre-webkit Opera

	    var form = document.createElement('form');
	    input.parentNode.insertBefore(form, input);

	    form.appendChild(input);
	    form.reset();

	    form.parentNode.insertBefore(input, form);
	    input.parentNode.removeChild(form);
	  }

	}

function saveDic(){
	var param = $("#indexDicForm").serialize();
	
	var url = "${ctx}/dic/save";
	if(validateDic()){
		$("#dicSaveBtn").css("background","#DDD");
		$("#dicSaveBtn").removeAttr("onclick").html("正在保存中……");
		
		$("#indexDicForm").ajaxSubmit({
	    	   type:"POST",
	    	   url:"${ctx}/dic/save",	
			   dataType : "json",
			   success:function(msg){
				   $("#big").hide();
				   $("#dicSaveBtn").html("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;保存&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;");
				   $("#dicSaveBtn").css("background","#00ADEF");
					$("#dicSaveBtn").attr("onclick","saveDic()");
				   if (msg == '1') {
					   $("#submitInfo").removeClass("alert-fail-info").addClass("alert-success-info").html("保存成功").fadeIn();
					   $("#indexDicForm")[0].reset();
				   }else{
					   $("#submitInfo").removeClass("alert-success-info").addClass("alert-fail-info").html("保存失败").fadeIn();
				   }
			   }
	       }); 
		
	}
}

</script>


</html>
