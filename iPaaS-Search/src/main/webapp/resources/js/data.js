
function toImport(){
	$("#importBut").attr('disabled', 'disabled');
	$("#importBut").css("background","#A9E2FF");
	$("#query_hint").show();
	$("#ds").removeAttr("onclick");
	$("#sql").removeAttr("onclick");
	$("#result").html("");
	var url = ctx+"/dataimport/import";
    $.ajax({
   	 type: 'POST',
        url: url,
//        data: "",
        success: function (data) {
         $("#query_hint").hide();
       	 var resJson = eval("(" + data + ")");
       	 if(resJson.CODE=="000"){
       		 //成功
    		 $("#result").html("OK! "+resJson.MSG);
    		 $("#result").css("color","green");
       	 }else{
       		 //失败
       		$("#result").html("fail! "+resJson.MSG);
       		$("#result").css("color","red");
       	 }
     	 $("#importBut").css("background","#00ADEF");
     	 $("#importBut").removeAttr('disabled');
     	 $("#ds").attr("onclick","toDs()");
    	 $("#sql").attr("onclick","toSql()");
        }
    });
}


function LogOut(){
	if(confirm("是否需要切换用户？")){
		window.location=ctx+"/login/doLogout";
	}
}



function toDs(){
	window.location=ctx+"/dataimport/toDs";
}
function toSql(){
	window.location=ctx+"/dataimport/toSql";
}

function appendSql(falias,drAlias,priKey,relationName,relation,indexAlias,indexSql,filedSql){
	$('.table').append('<div class="table-content" id="'+falias+'">'
			+'<div class="table-short">'+falias+'</div>'
			+'<div class="table-short">'+drAlias+'</div>'
			+'<div class="table-short">'+getText(priKey)+'</div>'
			+'<div class="table-short">'+getText(relationName)+'</div>'
			+'<div class="table-short">'+getText(indexAlias)+'</div>'
			+'<div class="table-SQL">'+getText(indexSql)+'</div>'
			+'<div class="table-SQL">'+filedSql+'</div>'
			+'</div><br id="br_'+falias+'">');
}
function getText(index){
	if(index==""){return "&nbsp;";}else{return index;}
}
function appendDs(alias,ip,port,sid,username,pwd,database){
	$('.data').append('<div class="dataContent" id="'+alias+'"><div class="tittle-name">'+alias
			+'</div><div class="dataType">普通数据库</div><div class="data-from"><label>IP: </label><span>'+ip
			+'</span><label>Port: </label><span>'+port
			+'</span><label>Sid: </label><span>'+sid
			+'</span><label>用户名: </label><span>'+username
			+'</span><label>密码: </label><span>'+pwd
			+'</span></div>'
			+'</div></div>');
}

function appendDBS(alias,user,serviceId,servicePwd,vsql){
	$('.data').append('<div class="dataContent" id="'+alias+'"><div class="tittle-name">'+alias
			+'</div><div class="dataType">DBS</div><div class="data-from"><label>云用户: </label><span>'+user
			+'</span><label>服务ID: </label><span>'+serviceId
			+'</span><label>服务密码: </label><span>'+servicePwd
			+'</span><label>验证sql: </label><span>'+vsql
			+'</span></div></div></div>');
}

function getRelationName(relation){
	if(relation==1){
		return "一对一";
	}
	if(relation==2){
		return "一对多";
	}
}
