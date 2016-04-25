<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>模型配置</title>

<%@include file="/jsp/common/header.jsp"%>
<link href="${ctx}/resources/jsonmate/jsoneditor.css" rel="stylesheet" />
<link href="${ctx}/resources/res/css/bootstrap-select.min.css"
	rel="stylesheet" />
<link href="${ctx}/resources/res/css/animate.min.css" rel="stylesheet" />
<style type="text/css">
.buildflow h4 {
	color: #aab2bd;
}

.primary-section p {
	margin-left: 4%;
}

.alert-warning {
	background-color: #F5F7FA;
	border-color: #F5F7FA;
	color: #f86631;
	border-radius: 4px;
}
</style>
<script type="text/javascript">
	var mapping = ${mapping};
</script>
</head>
<body class="ui-v3 buildflow">
	<nav class="navbar dao-navbar ng-scope">
	<div class="clearfix dao-container">
		<div class="navbar-header">

			<div class="back-link ng-scope">

				<div class="ng-binding ng-scope">
					<div class="daima">
						<a class="ng-binding" href="${ctx}/ses/mapping"> <i
							class="icon-arrow-left"></i>代码构建
						</a>
					</div>
					<ul class="nav navbar-nav navbar-right">
						<li><a href="${ctx}/doc/"><i class="icon-file-alt"></i> 文档</a></li>
						<li class="dropdown"><a href="#" class="dropdown-toggle"
							data-toggle="dropdown" role="button" aria-haspopup="true"
							aria-expanded="false"><span class="icon-user"></span> ${ SES_USER["userName"]}
								<span class="icon-angle-down"></span></a>
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
		<div class="dao-container ng-scope">
			<div class="panel panel-default panel-page-header">
				<div class="panel-body">
					<div class="primary-section"
						style="border: 0; margin: 0; padding-bottom: 0;">
						<h2>模型构建</h2>
						<br>
						<p>数据模型构建非常重要，只有创建了数据模型，才能进行后续的数据导入操作!</p>
					</div>
				</div>
			</div>
			<div class="panel panel-default">
				<div class="panel-body">

					<div class="setting-section">
						<div class="col-md-9 col-lg-9">
							<label class="setting-label">服务ID </label>
							<div class="setting-info">
								<div>
									<p class="text-muted">${serviceId}</p>
								</div>
							</div>
						</div>
						<div class="col-md-9 col-lg-9">
							<label class="setting-label">索引名称 </label>
							<div class="setting-info">
								<div>
									<input id="createIndex"
										class="form-control ng-pristine ng-valid ng-touched"
										type="text" placeholder="给索引起一个名字..." autofocus tabindex="0">
									<p id="showIndex" class="text-muted">${indexDisplay}</p>
								</div>
							</div>
						</div>
					</div>

					<div class="setting-section">
						<div class="col-md-9 col-lg-9">
							<label class="setting-label">构建mapping</label>
							<div class="setting-info">
								<div>
									<p class="text-muted">
										构建后的数据模型为json格式,我们称它为mapping,你可以按照<a href="${ctx}/doc/"
											style="font-weight: 600;">规则</a>自己生成mapping,也可以使用我们提供的构建工具制作mapping，just
										enjoy！
									</p>
								</div>
								<div class="form-horizontal">
									<div class="row" style="margin-bottom: 0; margin-left: 0;">

										<div class="btn-group" role="group" aria-label="..." style="margin-bottom:5px;">
											<button id="beautify" type="button" class="btn btn-default">展开</button>
											<button id="uglify" type="button" class="btn btn-default">收起</button>
										</div>
										<textarea id="json"
											style="min-width: 100%; border: 1px solid #e5e5e5; color: #555;">${mapping}</textarea>

										<div id="editor" class="json-editor" style="margin-left: -19px;">
											<button id="expander" type="button" class="btn btn-default">展开</button>
										</div>
									</div>
								</div>
							</div>
						</div>
					</div>
					<div class="setting-section">
						<div class="col-md-10 col-lg-10">
							<label class="setting-label">主键字段 </label>
							<div class="setting-info">
								<select id="pk" class="form-control"
									style="margin-top: -10px; width: 345px;">
									<option>请选择主键字段</option>
								</select>
								<!-- ngIf: available.buildflowName -->
								<div class="ng-binding ng-hide" aria-hidden="true">如果不需要指定主键字段，请选择"_id"</div>
							</div>
						</div>
					</div>
					<div class="setting-section">
						<div class="col-md-9 col-lg-9">
							<label class="setting-label">全文检索配置</label>
							<div id="fulltextSection" class="setting-info">
								<div>
									<p class="text-muted">
										你可以根据业务需求将字段组合后配置成全文检索字段。 <br>
									</p>
								</div>
								<a id="addFulltextRetrieval" class="btn btn-sm btn-success"
									href="javascript:void(0);" style="margin-bottom: 20px;">添加配置</a>
							</div>
						</div>
					</div>
					<div class="setting-section">
						<div id="submitInfo"></div>
						<button id="mapping_save_btn"
							class="btn btn-lg btn-block project-creation-btn ng-binding"
							data-loading-text="正在创建......">开始创建</button>


					</div>


				</div>

			</div>
		</div>
	</div>






	<div class="help">
		<a href="${ctx}/doc/"><i class="icon-comment"></i><span>?</span></a>
	</div>


	<div class="xiaox">
		<div class="p">
			<a class="pull-right text-muted"><i class="icon-remove"></i></a>
			xxxxxx
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
	<script src="${ctx}/resources/jsonmate/json2.js"></script>
	<script src="${ctx}/resources/jsonmate/jquery.jsoneditor.js"></script>
	<script src="${ctx}/resources/jsonmate/jsoneditor.js"></script>
	<script src="${ctx}/resources/res/js/bootstrap-select.js"></script>
	<script type="text/javascript">
		$(window).load(function() {
			valid();
			NProgress.done();
		});
		$(document).ready(function() {
			NProgress.start();
			$("#pk").on("focus",function(){
				var json = parse($("#json").val());
				var selector = $("#pk");
				selector.empty();
				selector.append('<option value="_id">_id</option>'); 
                for (var key in json) {
        		    selector.append('<option value="'+key+'">'+key+'</option>');   
        		}
				
			});
			initPk();
			var copyto = '${copyto}';
			initCopyto(copyto);
		});
		$(function(){
			  $("#json").bind("keydown keyup",function(){
			   $(this).autosize();
			  }).show().autosize();
		});
		$.fn.autosize = function(){
		 	$(this).height('0px');
		    var setheight = $(this).get(0).scrollHeight;
		    if($(this).attr("_height") != setheight)
		    	$(this).height(setheight+"px").attr("_height",setheight);
		  	else
		   		$(this).height($(this).attr("_height")+"px");
		}
		String.prototype.replaceAll = function(s1,s2){ 
			return this.replace(new RegExp(s1,"gm"),s2); 
		}
		var SESMappingController;
		function initPk(){
			var pk = "${pk}";
			if(pk){
				$("#pk").append('<option value="'+pk+'" selected>'+pk+'</option>'); 
			}
		}
		function initCopyto(copyto){
			if(copyto){
				var json = parse(copyto);
				for (var key in json) {
					var group = addCopyto(); 
					group.find("input").val(key);
					group.find(".filter-option").text(json[key]);
					group.find("select").val(json[key]);
        		}
			}
		}
		function addCopyto(){
			var selector=$('<select class=selectpicker show-menu-arrow form-control"" multiple></select>'); 
			var json = parse($("#json").val());
			for (var key in json) {
			    selector.append('<option value="'+key+'">'+key+'</option>');   
			}
			var group = $('<ul class="radio-group"></ul>');
			group.append('<li><input class="fulltext form-control ng-pristine ng-valid ng-touched" type="text" placeholder="请输入全文检索字段名称" autofocus tabindex="0"></li>');
			group.append($('<li></li>').append(selector));
			var del = $('<a  class="btn btn-sm btn-danger" href="javascript:void(0);">删除</a>');
			
			group.append($('<li></li>').append(del));
			group.addClass('animated fadeInLeft');
			group.one('webkitAnimationEnd mozAnimationEnd MSAnimationEnd oanimationend animationend', function(){
				group.removeClass('animated fadeInLeft');
			});
			$("#fulltextSection").append(group);
			
			selector.selectpicker({
	            noneSelectedText: "请选择字段",
	        });
			selector.parent().css("width", "360px");
			del.on("click", function(){
				var ul = $(this).closest("ul");
				ul.removeClass('animated fadeInLeft');
				ul.addClass('animated bounceOutRight');
				ul.one('webkitAnimationEnd mozAnimationEnd MSAnimationEnd oanimationend animationend', function(){
					ul.remove();
				});
			});
			group.find("input").on("blur", function(){
				if(!$(this).val()){
					del.trigger("click");
				}
				
			});
			return group;
		}
		function LogOut(){
			if(confirm("是否需要切换用户？")){
				window.location="${ctx}/login/doLogout";
			}
		}
		function parse(str) {
	        var res;
	        try { res = JSON.parse(str); }
	        catch (e) { res = null; error('JSON parse failed.'); }
	        return res;
	    }

	    function stringify(obj) {
	        var res;
	        try { res = JSON.stringify(obj); }
	        catch (e) { res = 'null'; error('JSON stringify failed.'); }
	        return res;
	    }
		function assembleJson(json){
			var mappingJson = parse(json);
			$.each($(".fulltext"), function(i,val){  
				mappingJson[val.value]={"type": "string" };
				var copytoArr = $(this).parent().next().find("select").val();
				$.each(copytoArr, function(i, item){      
				      var copytoFieldArr = mappingJson[item]["copy_to"] ; 
				      if(copytoFieldArr === undefined){
				    	  var arrayObj = new Array();
					      arrayObj.push(val.value);
					      mappingJson[item]["copy_to"] = arrayObj;
				      }else{
				    	  mappingJson[item]["copy_to"].push(val.value);
				      }
			　　 }); 
			 }); 
			return stringify(mappingJson);
		}
		function assembleCopytoJson(){
			var copytoJson = {};
			$.each($(".fulltext"), function(i,val){  
				var copytoFieldName = val.value;
				var copytoArr = $(this).parent().next().find("select").val();
				
				copytoJson[copytoFieldName] = copytoArr;
			 }); 
			return stringify(copytoJson);
		} 
		$(function() {
			SESMappingController = new $.SESMappingController();
			$(".head").wrap(document.createElement("div")).closest("div")
					.addClass("navigation");
			if(JSON.stringify(mapping) == "{}"){
				$("#createIndex").show();
				$("#showIndex").hide();
			}else{
				$("#createIndex").hide();
				$("#showIndex").show();
			}
			$("#submitInfo").hide();
			
		});

		/*定义页面管理类*/
		(function() {
			$.SESMappingController = function() {
				this.settings = $.extend(true, {},
						$.SESMappingController.defaults);
				this.init();

			};
			$.extend($.SESMappingController, {
				defaults : {
					MAPPING_SAVE_BTN : "#mapping_save_btn",
					ADD_FULLTEXT_RETRIEVAL_BTN : "#addFulltextRetrieval",
					CREATE_INDEX_NAME : "#createIndex",
					JSON : "#json",
					PK : "#pk"
				},
				prototype : {
					init : function() {
						var _this = this;
						_this.addRults();
						_this.bindEvents();
					},
					bindEvents : function() {
						var _this = this;
						$(_this.settings.MAPPING_SAVE_BTN).bind(
								"click",
								function() {
									var mappingJson = $("#json").val();
									var pk = $("#pk").val();
									var indexDis = $("#showIndex").text().trim();
									var indexDisplay = $("#createIndex").val();
									if(mappingJson == "{}"){
										alert("数据模型不能为空!");
										return;
									}else if(pk==""){
										alert("指定主键不能为空!");
										return;
									}else if(indexDisplay=="" && indexDis==""){
										alert("索引名称不能为空!");
										return;
									}
									//var json = assembleJson(mappingJson);
									//return;
								    $(this).button('loading');
								    $("#submitInfo").hide();
									$.ajax({
										type : 'POST',
										url : '${ctx}/ses/saveMapping',
										dataType : 'json',
										data : {
											serviceId : function() {
												return $("#serviceId").val();
											},
											mapping : function() {
												return mappingJson;
											},
											indexDisplay : function() {
												return indexDis == "" ? indexDisplay : indexDis;;
											},
											pk : function() {
												return $("#pk").val();
											},
											copyto : function() {
												return assembleCopytoJson();
											},
											assembledJson : function(){
												return assembleJson(mappingJson);
											}
											
										},
										success : function(data) {
											var code = data.resultCode;
											setTimeout(function () { $("#mapping_save_btn").button('reset'); },1000);
											if ("000000" == code) {
												$("#submitInfo").show("slow").removeClass("alert-fail-info").addClass("alert-success-info").html("").append("数据模型创建成功！您可以进行以下操作:   "
												+ "<a href=\"${ctx}/dataimport/toOne\" class=\"alert-link\"><strong style=\"color: #4a89dc;\">数据导入-单库</strong></a><span style=\"margin:5px;\">/</span>"
												+ "<a href=\"${ctx}/dataimport/toMany\" class=\"alert-link\"><strong style=\"color: #4a89dc;\">数据导入-多库</strong></a>");
											} else {
												$("#submitInfo").show("slow").removeClass("alert-success-info").addClass("alert-fail-info").html("").append("数据模型创建失败！错误信息：");
												$("#submitInfo").append("<br>" + data.resultMsg);
											}
										}
									}); 
								});
						$(_this.settings.ADD_FULLTEXT_RETRIEVAL_BTN).bind("click",function() {
							addCopyto();
						});
						$(_this.settings.CREATE_INDEX_NAME).bind("blur",function() {
							valid();
						});
						$(_this.settings.JSON).bind("blur",function() {
							valid();
						});
						$(_this.settings.PK).bind("blur",function() {
							valid();
						});
					},
					addRults : function() {

					}
				}
			});
		})(jQuery);
		function valid(){
			var createIndex = $("#createIndex").val();
			var json = $("#json").val();
			var pk = $("#pk").val();
			if($("#createIndex").is(":hidden")){
				if(json && pk){
					$("#mapping_save_btn").addClass("btnAval").removeClass("disabled").css("color", "#fff");
				}else{
					$("#mapping_save_btn").removeClass("btnAval").addClass("disabled").css("color", "#666");
				}
			}else{
				if(createIndex && json && pk){
					$("#mapping_save_btn").addClass("btnAval").removeClass("disabled").css("color", "#fff");
				}else{
					$("#mapping_save_btn").addClass("btnAval").addClass("disabled").css("color", "#666");
				}
			}
		} 
		
	</script>
</body>


</html>