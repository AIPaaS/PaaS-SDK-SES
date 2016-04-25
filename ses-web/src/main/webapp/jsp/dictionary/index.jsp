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
						<h2>用户词典维护</h2>
						<p>导入用户词典及停用词词典，提供词典维护，导入成功后，即可在搜索中是用自定义词语</p>
					</div>
				</div>
			</div>
			<div class="panel panel-default panel-page-header" >
				<div class="panel-body">
					<div class="primary-section"
						style="border: 0; margin: 0; padding-bottom: 0;">
						<h2>当前热词词库</h2>
						<c:forEach items="${allIndexWordList }" var="indexWord" begin="0" end="2">
						<p style="margin-bottom: 0px;">${ indexWord.word}</p>
						</c:forEach>
						<p style="margin-bottom: 0px;"><a href="javascript:void(0);" onclick="javascript:moreIndexWord();">[...]</a></p>
					</div>
				</div>
			</div>
			<div id="indexWordArea"  class="modal" aria-hidden="false" style="display:none;z-index: 1041;height: 350px;">
				<div class="modal-dialog modal-lg">
			    <div class="modal-content">
			      <div class="modal-header">
			        <button type="button" class="close" data-dismiss="modal" aria-hidden="true">×</button>
			        <h3 class="modal-title">热词词库预览</h3>
			      </div>
			      <div class="modal-body">
			        <textarea class="form-control" style="font-family:Monaco,Consolas,monospace; height: 253.5px;" readonly=""><c:forEach items="${allIndexWordList }" var="indexWord" >${ indexWord.word} </c:forEach>
			        </textarea>
			      </div>
			    </div>
				</div>
			</div>
			<div class="panel panel-default panel-page-header">
				<div class="panel-body">
					<div class="primary-section"
						style="border: 0; margin: 0; padding-bottom: 0;">
						<h2>当前停用词词库</h2>
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
			        <textarea class="form-control" style="font-family:Monaco,Consolas,monospace; height: 253.5px;" readonly=""><c:forEach items="${allStopWordList }" var="stopWord">${ stopWord.word} </c:forEach>
			        </textarea>
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
						<button id="dicSaveBtn"
							class="btn btn-lg btn-block project-creation-btn ng-binding">保存</button>
						<div id="submitInfo" class="alert alert-success" role="alert" style="display:block;height: 100px;"></div>
					</div>

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
				   $("#dicSaveBtn").html("保存");
				   $("#dicSaveBtn").css("background","#00ADEF");
					$("#dicSaveBtn").attr("onclick","saveDic()");
				   if (msg == '1') {
					   $("#submitInfo").removeClass("alert-fail-info").addClass("alert-success-info").html("保存成功").fadeIn();
				   }else{
					   $("#submitInfo").removeClass("alert-success-info").addClass("alert-fail-info").html("保存失败").fadeIn();
				   }
			   }
	       }); 
		
	}
}



</script>


</html>
