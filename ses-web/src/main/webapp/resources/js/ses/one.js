var oneCommit = false;
var dbCommit = false;
var sqlCommit = false;
var onerunningIt;


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
//	$("#checkDbCon").focus();
}


function canDbCommit(){
	if($("#ip").val()==''||$("#port").val()==''||$("#sid").val()==''||$("#username").val()==''||$("#pwd").val()==''){
		dbCommit = false;
		$("#checkDbCon").removeClass("btn-success");
		$("#saveDb").removeClass("btn-success");
		$("#ddb").removeClass("btn-success");
	}else{
		dbCommit = true;
		$("#checkDbCon").addClass("btn-success");
		$("#saveDb").addClass("btn-success");
		$("#ddb").addClass("btn-success");
	}
}

function checkDb(){
	if(!dbCommit)
		return;
	
	var url = webPath+"/dataimport/validateSource";
	$("#dbinfo").html("");
    $.ajax({
   	 type: 'POST',
        url: url,
        data: $("#one").serialize(),
        success: function (data) {
        	$("#dbinfo").show();
	       	 var resJson = eval("(" + data + ")");
	       	 if(resJson.CODE=="000"){
	       		 //成功
	       		 $("#dbinfo").html(resJson.MSG);
//	       		 $("#dbinfo").addClass("alert-success");
//	       		 $("#dbinfo").removeClass("alert-danger");
	       		 $("#dbinfo").removeClass("alert-fail-info").addClass("alert-success-info");
	       	 }else{
	       		 //失败
	       		 $("#dbinfo").html(resJson.MSG);
	       		 $("#dbinfo").removeClass("alert-success-info").addClass("alert-fail-info");

	       	 }
        }
    });
}

function saveDs(){
	if(!dbCommit)
		return;
	
	var url = webPath+"/dataimport/saveDs";
	$("#dbinfo").html("");
    $.ajax({
   	 type: 'POST',
        url: url,
        data: $("#one").serialize(),
        success: function (data) {
        	$("#dbinfo").show();
	       	 var resJson = eval("(" + data + ")");
	       	 if(resJson.CODE=="000"){
	       		 //成功
	       		 $("#dbinfo").html(resJson.MSG);
	       		 $("#dbinfo").removeClass("alert-fail-info").addClass("alert-success-info");
	       	 }else{
	       		 //失败
	       		 $("#dbinfo").html(resJson.MSG);
	       		 $("#dbinfo").removeClass("alert-success-info").addClass("alert-fail-info");

	       	 }
        }
    });
	
}
function deleteDb(){
	if(!dbCommit)
		return;
	
	var url = webPath+"/dataimport/deleteDs";
	$("#dbinfo").html("");
    $.ajax({
   	 type: 'POST',
        url: url,
        data: $("#one").serialize(),
        success: function (data) {
        	$("#dbinfo").show();
	       	 var resJson = eval("(" + data + ")");
	       	 if(resJson.CODE=="000"){
	       		 //成功
	       		 $("#dbinfo").html(resJson.MSG);
	       		 $("#dbinfo").removeClass("alert-fail-info").addClass("alert-success-info");

	       		 initNullDb();
	       	 }else{
	       		 //失败
	       		 $("#dbinfo").html(resJson.MSG);
	       		 $("#dbinfo").removeClass("alert-success-info").addClass("alert-fail-info");

	       	 }
        }
    });
}



function checkSql(){
	$("#sqlDiv").find(".text-danger").remove();
	if($("#sql").val()==''){
		$("#sqlDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">SQL不能为空</small>');
	}else{
		var rs = stripscript($("#sql").val()).replace(/"/g, "'");
		$("#sql").val(rs);
//		if(rs!=""){
//			sqlCommit = false;
//			$("#checkSqlCon").removeClass("btn-success");
//			$("#saveSql").removeClass("btn-success");
//			$("#dSql").removeClass("btn-success");
//			$("#sqlDiv").append('<small class="text-danger ng-binding ng-scope" ng-if="available.buildflowName" style="font-size:14px;margin-left:10px;">SQL含有特殊字符</small>');
//		}else{
			canSqlCommit();
//		}
	}
	
}
function canSqlCommit(){
	if($("#sql").val()==''){
		sqlCommit = false;
		$("#checkSqlCon").removeClass("btn-success");
		$("#saveSql").removeClass("btn-success");
		$("#dSql").removeClass("btn-success");
	}else{
		sqlCommit = true;
		$("#checkSqlCon").addClass("btn-success");
		$("#saveSql").addClass("btn-success");
		$("#dSql").addClass("btn-success");
		canDbCommit();
		canCommit();
	}
}

function sqlCon(){
	if(!sqlCommit)
		return;

	var url = webPath+"/dataimport/validateSql";
	$("#sqlinfo").html("");
    $.ajax({
   	 type: 'POST',
        url: url,
        data: $("#one").serialize(),
        success: function (data) {
        	$("#sqlinfo").show();
	       	 var resJson = eval("(" + data + ")");
	       	 if(resJson.CODE=="000"){
	       		 //成功
	       		 $("#sqlinfo").html(resJson.MSG);
	       		 $("#sqlinfo").removeClass("alert-fail-info").addClass("alert-success-info");

	       	 }else{
	       		 //失败
	       		 var infoStr = resJson.MSG;
	       		 if(infoStr!=""&&infoStr.length>115)
	       			infoStr = infoStr.substring(0,115)+"...";
	       		 $("#sqlinfo").html(infoStr);
	       		 $("#sqlinfo").removeClass("alert-success-info").addClass("alert-fail-info");
	       	 }
        }
    });
}

function saveSqlFun(){
	if(!sqlCommit)
		return;
	
	var url = webPath+"/dataimport/saveSql";
	$("#sqlinfo").html("");
    $.ajax({
   	 type: 'POST',
        url: url,
        data: $("#one").serialize(),
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
function deleteSql(){
	if(!sqlCommit)
		return;
	
	var url = webPath+"/dataimport/deleteSql";
	$("#sqlinfo").html("");
    $.ajax({
   	 type: 'POST',
        url: url,
        data: $("#one").serialize(),
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


function canCommit(){
	if(sqlCommit&&dbCommit){
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
        onerunningIt = setInterval("loadRunning()",1*1000);
        
	    $.ajax({
	   	 type: 'POST',
	        url: url,
	        data: {groupId : 1},
	        success: function (data) {
				 window.clearInterval(onerunningIt);
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
		       	 if(resJson.CODE=="999" && !oneCommit){
		       		 //stop load
					 window.clearInterval(onerunningIt);
					 $("#info").append("imported end.");
			       	 $("#info").scrollTop($("#info")[0].scrollHeight);
			       	 ableImport();
		       	 }
		        }
	    });
}



function initDisplay(sql,ip,port,sid,username,pwd,database){
	$("#info").hide();
	$("#database").val(database);
	$("#ip").val(ip);
	$("#port").val(port);
	$("#sid").val(sid);
	$("#username").val(username);
	$("#sql").val(sql);
	if(database=="")
		$("#database").val(1);
	$("#pwd").val(pwd);
	canDbCommit();
	canSqlCommit();
	
	initDisplayForSafari();

}

function initNullDb(){
	$("#database").val("");
	$("#ip").val("");
	$("#port").val("");
	$("#sid").val("");
	$("#username").val("");
	$("#pwd").val("");
	canDbCommit();
}

function disAbleImport(){
	oneCommit = false;
//	$("#imp").removeClass("btn-success");
	$("#imp").button('loading');

}
function ableImport(){
	oneCommit = true;
//	$("#imp").addClass("btn-success");
	$("#imp").button('reset');

}


function initDisplayForSafari(){
	$("#ipDiv").find(".text-danger").remove();
	$("#portDiv").find(".text-danger").remove();
	$("#sidDiv").find(".text-danger").remove();
	$("#usernameDiv").find(".text-danger").remove();
	$("#pwdDiv").find(".text-danger").remove();
	$("#sqlDiv").find(".text-danger").remove();

}


