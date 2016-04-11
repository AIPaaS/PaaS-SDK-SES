var save = false;
function saveDataSource(){
	if(!save){
		window.location=ctx+"/dataimport/toSql";
		return;
	}
	 var url = ctx+"/dataimport/validateSource";
	 $(".next").attr('disabled', 'disabled');
	 $(".next").css("background","#A9E2FF");
	 $("#errorInfo").html("");
     $.ajax({
    	 type: 'POST',
         url: url,
         data: $("#hiddenData").serialize(),
         success: function (data) {
        	 var resJson = eval("(" + data + ")");
        	 if(resJson.CODE=="000"){
        		 //成功
        		 $(".dataContent").each(function () {
        			 $(this).css("background","");
        		 });
        		 $("#errorInfo").html("");
        		 window.location=ctx+"/dataimport/toSql";
        	 }else{
        		 //失败
        		 var aliass = resJson.alias;
        		 if(aliass!=""){
        			 if(aliass.indexOf(",")>0){
        				 var aliasTemp = aliass.split(",");
        				 for(var i=0;i<aliasTemp.length;i++){
            				 $("#"+aliasTemp[i]).css("background","red");
        				 }
        			 }else{
        				 $("#"+aliass).css("background","red");
        			 }
        		 }
        		 $("#errorInfo").html(resJson.MSG);
        		 //alert(resJson.MSG);
        	 }
        	 $(".next").removeAttr('disabled');
        	 $(".next").css("background","#00ADEF");
         }
     });
}


// DBS、普通数据库切换
function cutLi(i){
	if (i=='0') {
		$('.data-type li').eq(0).addClass('cur').siblings('li').removeClass('cur');
		$('.SQL-data').css('display','none');
		$('.DBS-data').css('display','block');
	}else if (i=='1'){
		$('.data-type li').eq(1).addClass('cur').siblings('li').removeClass('cur');
		$('.DBS-data').css('display','none');
		$('.SQL-data').css('display','block');
	};
}

// 插入DBS数据
function saveDBS(){
	if(validateDBS()){
		appendDBS($(".other-name").val(),$(".user").val(),$(".serviceId").val(),$(".servicePwd").val(),
				$(".vsql").val());
		save = true;
	}
}
function appendDBS(alias,user,serviceId,servicePwd,vsql){
	if($("#"+alias).length>0){
		$("#"+alias).remove();
	}
	if($("#data_"+alias).length>0){
		$("#data_"+alias).remove();
	}
	$('.data').append('<div class="dataContent" id="'+alias+'"><div class="tittle-name">'+alias
			+'</div><div class="dataType">DBS</div><div class="data-from"><label>云用户: </label><span>'+user
			+'</span><label>服务ID: </label><span>'+serviceId
			+'</span><label>服务密码: </label><span>'+servicePwd
			+'</span><label>验证sql: </label><span>'+vsql
			+'</span></div><div class="operate"><span class="clear" onclick="clearDs(\''+alias+'\')"">清除</span>'
			+'<span class="edit" onclick="editorDs(\''+alias+'\')">编辑</span></div></div>');
	$("#hiddenData").append('<input type="hidden" id="data_'+alias+'" name="data_'+alias+'" value="'
			+'2'+'|'+user+'|'+serviceId+'|'+servicePwd
			+'|'+vsql
			+'">');
}
// 清除数据
function clearDs(id){
	save=true;
	$('#'+id+'').remove();
	$('#data_'+id).remove();
}

//插入普通数据库
function saveDs(){
	if(validateDs()){
		appendDs($(".other-name").val(),$(".ip").val(),$(".port").val(),$(".sid").val(),
				$(".username").val(),$(".pwd").val(),$(".database").val());
		save = true;
	}
}
function appendDs(alias,ip,port,sid,username,pwd,database){
	if($("#"+alias).length>0){
		$("#"+alias).remove();
	}
	if($("#data_"+alias).length>0){
		$("#data_"+alias).remove();
	}
	$('.data').append('<div class="dataContent" id="'+alias+'"><div class="tittle-name">'+alias
			+'</div><div class="dataType">普通数据库</div><div class="data-from"><label>IP: </label><span>'+ip
			+'</span><label>Port: </label><span>'+port
			+'</span><label>Sid: </label><span>'+sid
			+'</span><label>用户名: </label><span>'+username
			+'</span><label>密码: </label><span>'+pwd
			+'</span></div><div class="operate"><span class="clear" onclick="clearDs(\''+alias+'\')"">清除</span>'
			+'<span class="edit" onclick="editorDs(\''+alias+'\')">编辑</span></div></div>');
	$("#hiddenData").append('<input type="hidden" id="data_'+alias+'" name="data_'+alias+'" value="'
			+'1'+'|'+database+'|'+ip+'|'+port+'|'+sid
			+'|'+username+'|'+pwd+'">');
}


function editorDs(alias){
	var values = $("#data_"+alias).val();
	if(values=="")
		return;
	var valueArray = values.split("|");
	$(".other-name").val(alias);
	//dbs
	if(valueArray[0]=="2"){
		cutLi(0);
		$(".user").val(valueArray[1]);
		$(".serviceId").val(valueArray[2]);
		$(".servicePwd").val(valueArray[3]);
		$(".vsql").val(valueArray[4]);
	}else if(valueArray[0]=="1"){
		cutLi(1);
		$(".database").val(valueArray[1]);
		$(".ip").val(valueArray[2]);
		$(".port").val(valueArray[3]);
		$(".sid").val(valueArray[4]);
		$(".username").val(valueArray[5]);
		$(".pwd").val(valueArray[6]);
	}

}




function validateDBS(){
	var v = true;
	if(!check($(".other-name").val()))
		return false;
	$(".DBS-data" + " :input").each(function () {
		if($(this).val() == "" && $(this).attr("class")!='DBS-save') {
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
function check(obj){
	  if(obj == "")return false;
	  if(obj.match(/[^A-Za-z0-9]/ig)){
	    alert("只能为数字和字母");
	    return false;
	  }
	  return true;
}
function checkAlias(alias){
	var vv = true;
	$(".dataContent").each(function () {
		if(alias==$(this).attr("id")){
//			alert("别名有重复");
			vv = false;
			return;
		}
	});
	return vv;
}

function LogOut(){
	if(confirm("是否需要切换用户？")){
		window.location=ctx+"/login/doLogout";
	}
}
