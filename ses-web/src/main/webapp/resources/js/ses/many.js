var oneCommit = false;
var dbCommit = false;
var sqlCommit = false;
var prisqlCommit = false;
var runningIt;

function changeType(){

	var dbType = $("#type").val();
	if(dbType==1){
		$("#common").show();
		$("#dbs").hide();
		$("#vsql").val('');
	}else if(dbType==2){
		$("#common").hide();
		$("#dbs").show();
	}
	canDbCommit();
}


function checkIp(){
	$("#ipDiv").find(".text-danger").remove();
	if($("#ip").val()==''){
		$("#ipDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">数据库主机不能为空</small>');
	}
	canDbCommit();
}

function checkPort(){
	$("#portDiv").find(".text-danger").remove();
	if($("#port").val()==''){
		$("#portDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">数据库端口不能为空</small>');
	}
	canDbCommit();
}

function checkSid(){
	$("#sidDiv").find(".text-danger").remove();
	if($("#sid").val()==''){
		$("#sidDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">数据库ID不能为空</small>');
	}
	canDbCommit();
}

function checkUsername(){
	$("#usernameDiv").find(".text-danger").remove();
	if($("#username").val()==''){
		$("#usernameDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">用户名不能为空</small>');
	}
	canDbCommit();
}

function checkPwd(){
	$("#pwdDiv").find(".text-danger").remove();
	if($("#pwd").val()==''){
		$("#pwdDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">密码不能为空</small>');
	}
	canDbCommit();
}





function checkUser(){
	$("#userDiv").find(".text-danger").remove();
	if($("#user").val()==''){
		$("#userDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">云账号不能为空</small>');
	}
	canDbCommit();
}
function checkServiceId(){
	$("#serviceIdDiv").find(".text-danger").remove();
	if($("#serviceId").val()==''){
		$("#serviceIdDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">服务ID不能为空</small>');
	}
	canDbCommit();
}
function checkServicePwd(){
	$("#servicePwdDiv").find(".text-danger").remove();
	if($("#servicePwd").val()==''){
		$("#servicePwdDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">服务密码不能为空</small>');
	}
	canDbCommit();

}
function checkVsql(){
	$("#vsqlDiv").find(".text-danger").remove();
	if($("#vsql").val()==''){
		$("#vsqlDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">验证sql不能为空</small>');
	}else{
		var rs = stripscript($("#vsql").val());
		$("#vsql").val(rs);
	}
	canDbCommit();
}



function canDbCommit(){
	if($("#type").val()==1){
		if($("#ip").val()==''||$("#port").val()==''||$("#sid").val()==''||$("#username").val()==''||$("#pwd").val()==''){
			dbCommit = false;
			$("#saveDb").removeClass("btn-success");
		}else{
			dbCommit = true;
			$("#saveDb").addClass("btn-success");
		}
	}else if($("#type").val()==2){
		if($("#user").val()==''||$("#serviceId").val()==''||$("#servicePwd").val()==''||$("#vsql").val()==''){
			dbCommit = false;
			$("#saveDb").removeClass("btn-success");
		}else{
			dbCommit = true;
			$("#saveDb").addClass("btn-success");
		}
	}
	canCommit();
}

function checkDb(db){
	var dbStr = db.split("___");
	var port = dbStr[1];
	var sid = dbStr[2];
	var up = $("#"+db).val().split(",,");
	var ip;
	var username;
	var password;
	var database;
	var user;
	var servicePwd;
	var serviceId = dbStr[1];
	var type;
	if(up.length==5){
		ip = up[0];
		username = up[1];
		password = up[2];
		database = up[3];
		type=1;
	}else{
		user = up[0];
		servicePwd = up[1];
		vsql = up[2];
		type=2;
	}
	var url = webPath+"/dataimport/validateSource";
	$("#info_"+db).html("");
    $.ajax({
		type : 'POST',
		url : url,
		data : {
			type : function() {
				return type;
			},
			database : function() {
				return database;
			},
			groupId : 2,
			ip : function() {
				return ip;
			},
			port : function() {
				return port;
			},
			sid : function() {
				return sid;
			},
			username : function() {
				return username;
			},
			pwd : function() {
				return password;
			},
			user : function() {
				return user;
			},
			serviceId : function() {
				return serviceId;
			},
			servicePwd : function() {
				return servicePwd;
			},
			vsql : function() {
				return vsql;
			}
		},
		success : function(data) {
			$("#info_" + db).show();
			var resJson = eval("(" + data + ")");
			if (resJson.CODE == "000") {
				// 成功
				$("#info_" + db).html(resJson.MSG);
				$("#info_" + db).addClass("alert-success");
				$("#info_" + db).removeClass("alert-danger");
			} else {
				// 失败
				if(resJson.MSG!=""&&resJson.MSG.length>60)
					$("#info_" + db).html(resJson.MSG.substring(0,59));
				else
					$("#info_" + db).html(resJson.MSG);
				$("#info_" + db).removeClass("alert-success");
				$("#info_" + db).addClass("alert-danger");
			}
		}
	});
}


function appendDs(){
	
	if($("#type").val()==1){
		var database = $("#database").val();
		var ip = $("#ip").val();
		var port = $("#port").val();
		var sid = $("#sid").val();
		var username = $("#username").val();
		var password = $("#pwd").val();
		var dbAlias = ip+"___"+port+"___"+sid;
		var dbId = ip.replace(/\./g, "")+"___"+port+"___"+sid;
		if($("#tr_"+dbId).length>0){
			$("#tr_"+dbId).remove();
		}
		
		$("#dsDiv").show();
		var up = ip+",,"+username+",,"+password+",,"+database+",,1";
		var dbTr='<tr ng-repeat="item in ctrl.buildflows" class="ng-scope" id="tr_'+dbId+'">'
	        	+'<td>'+dbAlias+'<input type="hidden" id="'+dbId+'" name="'+dbAlias+'" value="'+up+'"/></td>'
	        	+'<td><a href="javascript:checkDb(\''+dbId+'\')">测试连接</a>  <a href="javascript:editDb(\''+dbId+'\')">编辑</a>  <a href="javascript:deleteDb(\''+dbId+'\')">删除</a>'
	        	+'<div id="info_'+dbId+'" style="float: right; display: none">ok</div></td>'
	        	+'</tr>';
		$("#dsTable").append(dbTr);
		
	}else if($("#type").val()==2){
		var user = $("#user").val();
		var serviceId = $("#serviceId").val();
		var servicePwd = $("#servicePwd").val();
		var vsql = $("#vsql").val();
		var dbAlias = user.split("@")[0]+"___"+serviceId;
		var dbId = user.split("@")[0]+"___"+serviceId;
		
		if($("#tr_"+dbId).length>0){
			$("#tr_"+dbId).remove();
		}
		$("#dsDiv").show();
		var up = user+",,"+servicePwd+",,"+vsql+",,2";
		var dbTr='<tr ng-repeat="item in ctrl.buildflows" class="ng-scope" id="tr_'+dbId+'">'
	        	+'<td>'+dbAlias+'<input type="hidden" id="'+dbId+'" name="'+dbAlias+'" value="'+up+'"/></td>'
	        	+'<td><a href="javascript:checkDb(\''+dbId+'\')">测试连接</a>  <a href="javascript:editDb(\''+dbId+'\')">编辑</a>  <a href="javascript:deleteDb(\''+dbId+'\')">删除</a>'
	        	+'<div id="info_'+dbId+'" style="float: right; display: none">ok</div></td>'
	        	+'</tr>';
		$("#dsTable").append(dbTr);
		
	}
	
}
function editDb(db){
	var dbStr = db.split("___");
	var port = dbStr[1];
	var up = $("#"+db).val().split(",,");
	if(up.length==5){
		$("#dbs").hide();
		$("#common").show();
		$("#type").val(1);
		var ip = up[0];
		var username = up[1];
		var password = up[2];
		var database = up[3];

		$("#database").val(database);
		$("#ip").val(ip);
		$("#port").val(port);
		$("#sid").val(dbStr[2]);
		$("#username").val(username);
		$("#pwd").val(password);
	}else{
		$("#dbs").show();
		$("#common").hide();
		$("#type").val(2);
		var user = up[0];
		var servicePwd = up[1];
		var vsql = up[2];
		$("#user").val(user);
		$("#serviceId").val(dbStr[1]);
		$("#servicePwd").val(servicePwd);
		$("#vsql").val(vsql);
	}
	$("#type").focus();
}
function validateDs(){
	var v = true;
	if(!check($(".other-name").val()))
		return false;
	$(".SQL-data" + " :input").each(function () {
        if ($(this).val() == "" && $(this).attr("class")!='SQL-save') {
        	alert($(this).attr("class")+"不能为空！");
        	v = false;
        	return v;
        }
    });
	
	//验证别名是否重复
	if(v&&!checkAlias($(".other-name").val())){
		v=confirm("数据源别名有重复，是否覆盖？");
	}
	return v;
}
function saveDs(){
	if(!dbCommit)
		return;
	var overwrite = false;
	var resV = true;
	if($("#type").val()==1){
		if($("#tr_"+$("#ip").val().replace(/\./g, "")+"___"+$("#port").val()+"___"+$("#sid").val()).length>0){
			resV = confirm("datasource already exist. overwrite ?");
			overwrite = true;
		}
	}else if($("#type").val()==2){
		if($("#tr_"+$("#user").val().split("@")[0]+"___"+$("#serviceId").val()).length>0){
			resV = confirm("datasource already exist. overwrite ?");
			overwrite = true;
		}
	}
	
	
	if(!resV)
		return;
	
	var url = webPath+"/dataimport/saveDs";
	$("#dbinfo").html("");
    $.ajax({
   	 type: 'POST',
        url: url,
        data : {
        	type : function() {
				return $("#type").val();
			},
        	database : function() {
				return $("#database").val();
			},
			groupId : 2,
			ip : function() {
				return $("#ip").val();
			},
			port : function() {
				return $("#port").val();
			},
			sid : function() {
				return $("#sid").val();
			},
			username : function() {
				return $("#username").val();
			},
			pwd : function() {
				return $("#pwd").val();
			},
			user : function() {
				return $("#user").val();
			},
			serviceId : function() {
				return $("#serviceId").val();
			},
			servicePwd : function() {
				return $("#servicePwd").val();
			},
			vsql : function() {
				return $("#vsql").val();
			},
			overwrite : overwrite
		},
        success: function (data) {
        	$("#dbinfo").show();
	       	 var resJson = eval("(" + data + ")");
	       	 if(resJson.CODE=="000"){
	       		 //成功
	       		 $("#dbinfo").html(resJson.MSG);
	       		 $("#dbinfo").removeClass("alert-fail-info").addClass("alert-success-info");
	       		 appendDs();
	       		 dislpaySqlDs();
	       	 }else{
	       		 //失败
	       		 $("#dbinfo").html(resJson.MSG);
	       		 $("#dbinfo").removeClass("alert-success-info").addClass("alert-fail-info");
	       	 }
        }
    });
	
}
function deleteDb(db){
	
	if(!dbCommit)
		return;
	
	var url = webPath+"/dataimport/deleteDs";
	$("#info_"+db).html("");
    $.ajax({
   	 type: 'POST',
        url: url,
        data: {groupId : 2,
        	uId:$("#uId").val(),
        	alias:$("#"+db).attr("name")
        },
        success: function (data) {
        	$("#info_"+db).show();
	       	 var resJson = eval("(" + data + ")");
	       	 if(resJson.CODE=="000"){
	       		 //成功
	       		 $("#info_"+db).html(resJson.MSG);
	       		 $("#info_"+db).addClass("alert-success");
	       		 $("#info_"+db).removeClass("alert-danger");
	       		 
	       		 $("#tr_"+db).delay(100).remove();
	       		 dislpaySqlDs();
	       	 }else{
	       		 //失败
	       		 $("#info_"+db).html(resJson.MSG);
	       		 $("#info_"+db).removeClass("alert-success");
	      		 $("#info_"+db).addClass("alert-danger");
	       	 }
        }
    });
}






function checkAlias(){
	$("#aliasDiv").find(".text-danger").remove();
	if($("#alias").val()==''){
		$("#aliasDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">主表SQL别名不能为空</small>');
	}else{
//		if($("#alias").val().match(/[^A-Za-z0-9]/ig)){
//			$("#aliasDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">主表SQL别名只能为数字和字母</small>');
//		}
	}
	changePrisqlCommit();
}
function checkDrAlias(){
	$("#drAliasDiv").find(".text-danger").remove();
	if($("#drAlias").val()==''||$("#drAlias").val()==-1){
		$("#drAliasDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">主表数据源不能为空</small>');
	}
	changePrisqlCommit();
}
function checkPrimaryKey(){
	$("#primaryKeyDiv").find(".text-danger").remove();
	if($("#primaryKey").val()==''){
		$("#primaryKeyDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">主键不能为空</small>');
	}
	changePrisqlCommit();
}
function checkSql(){
	$("#sqlDiv").find(".text-danger").remove();
	if($("#sql").val()==''){
		$("#sqlDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">主表SQL不能为空</small>');
	}else{
		var rs = stripscript($("#sql").val());
		$("#sql").val(rs);
	}
	changePrisqlCommit();
}
function checkFalias(){
	$("#faliasDiv").find(".text-danger").remove();
	if($("#falias").val()==''){
		$("#faliasDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">辅助表SQL别名不能为空</small>');
	}
//	else if($("#falias").val().match(/[^A-Za-z0-9]/ig)){
//		$("#faliasDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">辅助表SQL别名只能为数字和字母</small>');
//	}
	canSqlCommit();
}

function checkFdralias(){
	$("#fdrAliasDiv").find(".text-danger").remove();
	if($("#fdrAlias").val()==''||$("#fdrAlias").val()==-1){
		$("#fdrAliasDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">辅助表数据源不能为空</small>');
	}else{
		var fdralias = $("#fdrAlias").val().split("___");
		if(fdralias.length==3){
			$("#indexDiv").hide();
			$("#index2Div").hide();
		}
		if(fdralias.length==2){
			$("#indexDiv").show();
			$("#index2Div").show();
		}
	}
	canSqlCommit();
	
}

function checkRelation(){
	$("#relationDiv").find(".text-danger").remove();
	if($("#relation").val()==''||$("#relation").val()==-1){
		$("#relationDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">对应关系不能为空</small>');
	}
	canSqlCommit();
}
function checkIndexAlias(){
//	$("#indexAliasDiv").find(".text-danger").remove();
//	if($("#indexAlias").val()==''){
//		$("#indexAliasDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">索引别名不能为空</small>');
//	}
	//TODO
}
function checkIndexSql(){
//	$("#indexSqlDiv").find(".text-danger").remove();
//	if($("#indexSql").val()==''){
//		$("#indexSqlDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">索引sql不能为空</small>');
//	}
	
	if($("#indexSql").val()!=''){
		var rs = stripscript($("#indexSql").val());
		$("#indexSql").val(rs);
	}
}
function checkFSql(){
	$("#fSqlDiv").find(".text-danger").remove();
	if($("#fsql").val()==''){
		$("#fSqlDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">辅助表sql不能为空</small>');
	}else{
		var rs = stripscript($("#fsql").val());
		$("#fsql").val(rs);
	}
	canSqlCommit();

}





function changePrisqlCommit(){
	
	if($("#alias").val()==''||$("#drAlias").val()==''||
			$("#primaryKey").val()==''||$("#sql").val()==''
			||$("#drAlias").val()==-1){
		prisqlCommit = false;
		$("#testPriSqlBut").removeClass("btn-success");
		$("#savePriSqlBut").removeClass("btn-success");
		$("#deletePriSqlBut").removeClass("btn-success");
	}else{
		sqlCommit = true;
		prisqlCommit = true;
		$("#testPriSqlBut").addClass("btn-success");
		$("#savePriSqlBut").addClass("btn-success");
		$("#deletePriSqlBut").addClass("btn-success");
	}
	canCommit();
}



//function checkSql(){
//	$("#sqlDiv").find(".text-danger").remove();
//	if($("#sql").val()==''){
//		$("#sqlDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">SQL不能为空</small>');
//	}else{
//		var rs = stripscript($("#sql").val());
//		$("#sql").val(rs);
//		canSqlCommit();
//	}
//	
//}
function canSqlCommit(){
	
	if($("#falias").val()==''||$("#fdrAlias").val()==''||
			$("#relation").val()==''||$("#fsql").val()==''
			||$("#fdrAlias").val()==-1||$("#relation").val()==-1){
		
		sqlCommit = false;
		$("#saveFSqlBut").removeClass("btn-success");
	}else{
		sqlCommit = true;
		$("#saveFSqlBut").addClass("btn-success");
	}
}





function testPriSql(){
	if(!prisqlCommit)
		return;
	
	var url = webPath+"/dataimport/validateSql";
	$("#sqlinfo").html("");
    $.ajax({
   	 type: 'POST',
        url: url,
        data: {groupId : 2,
        	uId:$("#uId").val(),
        	alias:$("#alias").val(),
        	drAlias:$("#drAlias").find("option:selected").text(),
        	primaryKey:$("#primaryKey").val(),
        	sql:$("#sql").val(),
        	isPrimary:true
        },
        success: function (data) {
        	$("#sqlinfo").show();
	       	 var resJson = eval("(" + data + ")");
	       	 if(resJson.CODE=="000"){
	       		 //成功
	       		 $("#sqlinfo").html(resJson.MSG);
	       		 $("#sqlinfo").removeClass("alert-fail-info").addClass("alert-success-info");

	       	 }else{
	       		 //失败
	       		 $("#sqlinfo").html(resJson.MSG);
	       		 $("#sqlinfo").removeClass("alert-success-info").addClass("alert-fail-info");
	       	 }
        }
    });
}

function savePriSql(){
	if(!prisqlCommit)
		return;
	
	var url = webPath+"/dataimport/saveSql";
	$("#sqlinfo").html("");
    $.ajax({
   	 type: 'POST',
        url: url,
        data: {groupId : 2,
        	uId:$("#uId").val(),
        	alias:$("#alias").val(),
        	drAlias:$("#drAlias").find("option:selected").text(),
        	isPrimary:true,
        	sql:$("#sql").val().replace(/"/g, "'"),
        	isPrimary:true
        },
        success: function (data) {
        	$("#sqlinfo").show();
	       	 var resJson = eval("(" + data + ")");
	       	if(resJson.CODE=="000"){
	       		 //成功
	       		 $("#sqlinfo").html(resJson.MSG);
	       		 $("#sqlinfo").removeClass("alert-fail-info").addClass("alert-success-info");

	       	 }else{
	       		 //失败
	       		 $("#sqlinfo").html(resJson.MSG);
	       		 $("#sqlinfo").removeClass("alert-success-info").addClass("alert-fail-info");
	       	 }
        }
    });
}

function deletePriSql(){

	var url = webPath+"/dataimport/deleteSql";
	$("#sqlinfo").html("");
    $.ajax({
   	 type: 'POST',
        url: url,
        data: {groupId : 2,
        	uId:$("#uId").val(),
        	isPrimary:true
        },
        success: function (data) {
        	$("#sqlinfo").show();
	       	 var resJson = eval("(" + data + ")");
	       	if(resJson.CODE=="000"){
	       		 //成功
	       		 $("#sqlinfo").html(resJson.MSG);
	       		 $("#sqlinfo").removeClass("alert-fail-info").addClass("alert-success-info");
	       		 initNullPriSql();
	       	 }else{
	       		 //失败
	       		 $("#sqlinfo").html(resJson.MSG);
	       		 $("#sqlinfo").removeClass("alert-success-info").addClass("alert-fail-info");
	       	 }
        }
    });
}
function saveFSql(){
	if(!sqlCommit)
		return;
	var overwrite = false;
	var resV = true;
	if($("#tr_"+$("#falias").val()).length>0){
		resV = confirm("sql already exist. overwrite ?");
		overwrite = true;
	}
	if(!resV)
		return;
	
	var falias = $("#falias").val();
	if($("#mapObj").val()=="true"){
		falias = $("#falias").val().replace(/___/g, ".");
	}
	
	var url = webPath+"/dataimport/saveSql";
	$("#fsqlinfo").html("");
    $.ajax({
   	 type: 'POST',
        url: url,
        data: {groupId : 2,
        	uId:$("#uId").val(),
        	falias:falias,
        	mapObj:$("#mapObj").val(),
        	fdrAlias:$("#fdrAlias").find("option:selected").text(),
        	relation:$("#relation").val(),
        	indexAlias:$("#indexAlias").val(),
        	indexSql:$("#indexSql").val(),
        	fsql:$("#fsql").val(),
        	isPrimary:false,
        	overwrite:overwrite
        },
        success: function (data) {
        	$("#fsqlinfo").show();
	       	 var resJson = eval("(" + data + ")");
	       	 if(resJson.CODE=="000"){
	       		appedFSql($("#fdrAlias").val());
	       		 //成功
	       		 $("#fsqlinfo").html(resJson.MSG);
	       		 $("#fsqlinfo").removeClass("alert-fail-info").addClass("alert-success-info");

	       	 }else{
	       		 //失败
	       		 $("#fsqlinfo").html(resJson.MSG);
	       		 $("#fsqlinfo").removeClass("alert-success-info").addClass("alert-fail-info");

	       	 }
	       	 
	       	sqlCommit = true;
	       	canCommit();
        }
    });
}


function appedFSql(fdrAlias){
	if(fdrAlias=="")
		fdrAlias = $("#fdrAlias").val();
	if(fdrAlias.split("___").length==3){
		var mapObj = $("#mapObj").val();
		var falias = $("#falias").val();
		if(mapObj=="true"){
			falias = falias.replace(/\./g, "___");
		}
		var faliasDisplay = $("#falias").val();

		var relation = $("#relation").val();
		var fsql = $("#fsql").val();
		var fsqlShow = fsql;
		
		if($("#tr_"+falias).length>0){
			$("#tr_"+falias).remove();
		}
		if(fsql!=""&&fsql.length>57)
			fsqlShow = fsql.substring(0,54)+"...";
		$("#fsqlDivs").show();
		var up = fdrAlias+",,"+relation+",,"+fsql+",,"+mapObj;
		var dbTr='<tr ng-repeat="item in ctrl.buildflows" class="ng-scope" id="tr_'+falias+'">'
	        	+'<td>'+faliasDisplay+'<input type="hidden" id="'+falias+'" name="'+falias+'" value="'+up+'"/></td>'
	        	+'<td>'+fsqlShow+'</td>'
	        	+'<td> <a href="javascript:editFsql(\''+falias+'\')">编辑</a>  <a href="javascript:deleteFsql(\''+falias+'\')">删除</a>'
	        	+'<div id="info_'+falias+'" style="float: right; display: none">ok</div></td>'
	        	+'</tr>';
		$("#sqlTable").append(dbTr);
		
	}else if(fdrAlias.split("___").length==2){
		var mapObj = $("#mapObj").val();
		var falias = $("#falias").val();
		if(mapObj=="true"){
			falias = falias.replace(/\./g, "___");
		}
		var faliasDisplay = $("#falias").val();
		var relation = $("#relation").val();
		var fsql = $("#fsql").val();
		var indexAlias = $("#indexAlias").val();
		var indexSql = $("#indexSql").val();
		var fsqlShow = fsql;

		
		if($("#tr_"+falias).length>0){
			$("#tr_"+falias).remove();
		}
		if(fsql!=""&&fsql.length>57)
			fsqlShow = fsql.substring(0,54)+"...";
		$("#fsqlDivs").show();
		var up = fdrAlias+",,"+relation+",,"+fsql+",,"+mapObj;
		if(indexAlias!="")
			up += ",,"+indexAlias;
		if(indexSql!="")
			up += ",,"+indexSql;
		var dbTr='<tr ng-repeat="item in ctrl.buildflows" class="ng-scope" id="tr_'+falias+'">'
	        	+'<td>'+faliasDisplay+'<input type="hidden" id="'+falias+'" name="'+falias+'" value="'+up+'"/></td>'
	        	+'<td><'+fsqlShow+'</td>'
	        	+'<td> <a href="javascript:editFsql(\''+falias+'\')">编辑</a>  <a href="javascript:deleteFsql(\''+falias+'\')">删除</a>'
	        	+'<div id="info_'+falias+'" style="float: right; display: none">ok</div></td>'
	        	+'</tr>';
		$("#sqlTable").append(dbTr);
		
	}
}
function editFsql(falias){
	var up = $("#"+falias).val().split(",,");
	var fdrAlias = up[0];
	var relation = up[1];
	var fsql = up[2];
	var mapObj = up[3];
	$("#falias").val(falias);
	if(mapObj=="true"){
		$("#falias").val(falias.replace(/___/g, "."));
	}
	
	$("#fdrAlias").val(fdrAlias.replace(/\./g, ""));
	$("#relation").val(relation);
	$("#fsql").val(fsql);
	$("#mapObj").val(mapObj);
	if(fdrAlias.split("___").length==2){
		$("#indexAlias").val(up[3]);
		$("#indexSql").val(up[4]);
	}
}
function deleteFsql(falias){
	var url = webPath+"/dataimport/deleteSql";
//	$("#sqlinfo").html("");
	var up = $("#"+falias).val().split(",,");
	var mapObj = up[3];
	var faliasht = falias;
	if(mapObj=="true"){	
		faliasht = faliasht.replace(/___/g, ".");
	}
    $.ajax({
   	 type: 'POST',
        url: url,
        data: {groupId : 2,
        	uId:$("#uId").val(),
        	falias:faliasht,
        	isPrimary:false
        },
        success: function (data) {
//        	$("#sqlinfo").show();
	       	 var resJson = eval("(" + data + ")");
	       	 if(resJson.CODE=="000"){
	       		 $("#tr_"+falias).remove();
	       		 //成功
//	       		 $("#sqlinfo").html(resJson.MSG);
//	       		 $("#sqlinfo").addClass("alert-success");
//	       		 $("#sqlinfo").removeClass("alert-danger");
	       	 }else{
	       		 //失败
//	       		 $("#sqlinfo").html(resJson.MSG);
//	       		 $("#sqlinfo").removeClass("alert-success");
//	      		 $("#sqlinfo").addClass("alert-danger");
	       	 }
        }
    });
	
	
}

function canCommit(){
	if(sqlCommit&&dbCommit&&prisqlCommit){
		oneCommit = true;
		$("#imp").addClass("btn-success");
	}else{
		oneCommit = false;
		$("#imp").removeClass("btn-success");
	}
}

function startImport(){
	if(oneCommit){
		disAbleImport();
		var url = webPath+"/dataimport/import";
		$("#info").show();
		$("#info").html("import begin...");
		$(document).scrollTop($(document).height() );
		//每隔2s 获取一次进度;
        runningIt = setInterval("loadRunning()",1*1000);
	  
        $.ajax({
	   	 type: 'POST',
	        url: url,
	        data: {groupId : 2},
	        success: function (data) {
				 window.clearInterval(runningIt);
		       	 var resJson = eval("(" + data + ")");
		       	 if(resJson.CODE=="000"){
		       		 //成功
		       		 $("#info").append("<p>").append(resJson.MSG).append("</p>");
		       	 }else{
		       		 //失败
		       		 $("#info").append("<p>").append(resJson.MSG).append("</p>");
		       	 }
		       	 if(!oneCommit){
		     		 $("#info").append("imported end.");
			       	 $("#info").scrollTop($("#info")[0].scrollHeight);
			       	 ableImport();
		       	 }
	        }
	    });
		
	}else{
		// buchong
	}
	
}



function loadRunning(){
	var url = webPath+"/dataimport/running";
	$.ajax({
	   	 type: 'POST',
	        url: url,
	        success: function (data) {
		       	 var resJson = eval("(" + data + ")");
		       	 $("#info").append(resJson.MSG);
		       	 $("#info").scrollTop($("#info")[0].scrollHeight);
//		       	 alert(resJson.CODE);
		       	 if(resJson.CODE=="999" && !oneCommit){
		       		 //stop load
					 window.clearInterval(runningIt);
					 $("#info").append("imported end.");
			       	 $("#info").scrollTop($("#info")[0].scrollHeight);
			       	 ableImport();
		       	 }
		        }
	    });
}

function dislpaySqlDs(drAlias){
	var url = webPath+"/dataimport/loadDs";
	$.ajax({
	   	 type: 'POST',
	        url: url,
	        success: function (data) {
	        	var resJson = eval("(" + data + ")");
		       	 if(resJson.CODE=="000"){
		       		 //成功
		       		 $("#drAlias").empty();
		       		 $("#drAlias").append("<option value=-1>请选择</option>");
		       		 $("#drAlias").append(resJson.MSG);
		       		 $("#fdrAlias").empty();
		       		 $("#fdrAlias").append("<option value=-1>请选择</option>");
		       		 $("#fdrAlias").append(resJson.MSG);
//TODO		       		 
		       		 if(drAlias!="")
		       			 $("#drAlias").val(drAlias.replace(/\./g, ""));
//		       		 if(fdrAlias!="")
//		       			 $("#fdrAlias").val(fdrAlias.replace(/\./g, ""));
		       			 
		       	 }else{
		       		 //失败
		       		 
		       	 }
	        }
	    });
}

function initDisplay(type,ip,port,sid,username,pwd,database,user,
		serviceId,servicePwd,vsql){
	$("#dsDiv").hide();
	$("#common").hide()
	$("#dbs").hide()
	$("#info").hide();
	$("#type").val(type);
	if(type==1){
		$("#database").val(database);
		$("#ip").val(ip);
		$("#port").val(port);
		$("#sid").val(sid);
		$("#username").val(username);
		if(database=="")
			$("#database").val(1);
		$("#pwd").val(pwd);
		$("#common").show()
		$("#dbs").hide()
		$("#vsql").val('');

	}else if(type==2){
		$("#user").val(user);
		$("#serviceId").val(serviceId);
		$("#servicePwd").val(servicePwd);
		$("#vsql").val(vsql);
		$("#dbs").show()
		$("#common").hide()

	}
	appendDs();
	canDbCommit();
//	canSqlCommit();

}
function dislpayPriSql(alias,drAlias,primaryKey,sql){
	$("#alias").val(alias);
	$("#drAlias").val(drAlias.replace(/\./g, ""));
//	$("#primaryKey").val(primaryKey);
	$("#sql").val(sql);
	
	changePrisqlCommit();
}
function initFSql(falias,fdrAlias,relation,fsql,indexAlias,indexSql,mapObj){
	$("#falias").val(falias);
	$("#fdrAlias").val(fdrAlias.replace(/\./g, ""));
	$("#relation").val(relation);
	$("#fsql").val(fsql);
	$("#indexAlias").val(indexAlias);
	$("#indexAlias").val(indexAlias);
	$("#mapObj").val(mapObj);

	
	appedFSql(fdrAlias);
//	canSqlCommit();
}


function initNullFSql(){
	$("#falias").val("");
	$("#fdrAlias").val(-1);
	$("#relation").val(-1);
	$("#fsql").val("");
	$("#indexDiv").hide();
	$("#index2Div").hide();

	$("#info").hide();
	initDisplayForSafari();
}
function initNullPriSql(){
	$("#alias").val("");
	$("#drAlias").val(-1);
//	$("#primaryKey").val("");
	$("#sql").val("");
}


function initDisplayForSafari(){
	$("#ipDiv").find(".text-danger").remove();
	$("#portDiv").find(".text-danger").remove();
	$("#sidDiv").find(".text-danger").remove();
	$("#usernameDiv").find(".text-danger").remove();
	$("#pwdDiv").find(".text-danger").remove();
	$("#sqlDiv").find(".text-danger").remove();
	
	$("#userDiv").find(".text-danger").remove();
	$("#serviceIdDiv").find(".text-danger").remove();
	$("#servicePwdDiv").find(".text-danger").remove();
	$("#vsqlDiv").find(".text-danger").remove();

}

function initCommit(dsSize,fsqlSize){
	if(dsSize>0&&fsqlSize>0)
		ableImport();
	if(dsSize==0){
		$("#type").val(1);
		$("#type").change();
	}
}


function disAbleImport(){
	oneCommit = false;
//	$("#imp").removeClass("btn-success");
	$("#imp").button('loading');

}
function ableImport(){
	oneCommit = true;
	$("#imp").addClass("btn-success");
	$("#imp").button('reset');

}

function startClear(){
	var url = webPath+"/dataimport/clear";
	$("#info").show();
	$("#info").html("clear begin...");
	$(document).scrollTop($(document).height() );
	//每隔2s 获取一次进度;
    $.ajax({
   	 type: 'POST',
        url: url,
        success: function (data) {
	       	 var resJson = eval("(" + data + ")");
	       	 if(resJson.CODE=="000"){
	       		 //成功
	       		 $("#info").append("<p>").append(resJson.MSG).append("</p>");
	       	 }else{
	       		 //失败
	       		 $("#info").append("<p>").append(resJson.MSG).append("</p>");
	       	 }
        }
    });
	
}