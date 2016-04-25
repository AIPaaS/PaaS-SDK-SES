// JavaScript Document
function toDs(){
	window.location=ctx+"/dataimport/toDs";
}

function saveSql(){
	$("#nex").attr('disabled', 'disabled');
	$(".nex").css("background","#A9E2FF");
	$("#pre").attr('disabled', 'disabled');
	$("#pre").css("background","#A9E2FF");
	$("#query_hint").show();
	if(!validatePriSql()){
     	$("#nex").removeAttr('disabled');
      	$(".nex").css("background","#00ADEF");
      	$("#pre").removeAttr('disabled');
      	$("#pre").css("background","#00ADEF");
		return;
	}
	var url = ctx+"/dataimport/saveSql";
    $.ajax({
   	 type: 'POST',
        url: url,
        data: $("#hiddenSqlData").serialize(),
        success: function (data) {
         $("#query_hint").hide();
       	 var resJson = eval("(" + data + ")");
       	 if(resJson.CODE=="000"){
       		 //成功
       		 $(".table-content").each(function () {
       			 $(this).css("background","");
       		 });
       		 window.location=ctx+"/dataimport/toData";
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
       	 }
      	$(".nex").removeAttr('disabled');
      	$(".nex").css("background","#00ADEF");
      	$("#pre").removeAttr('disabled');
      	$("#pre").css("background","#00ADEF");
        }
    });
}



var relation = 1;
$(function(){
	$('.relation li').click(function(){
		$(this).addClass('double').siblings('li').removeClass('double');
		if(relation==1){
			relation = 2;
		}else{
			relation = 1;
		}
	});
});


function saveTable(){
	if(validateSql()){
		appendFsql($("#falias").val(),$("#drAlias").val(),getRelationName(relation),relation,
				$("#indexAlias").val(),$("#indexSql").val(),$("#filedSql").val());
	}
	
}
function appendFsql(falias,drAlias,relationName,relation,indexAlias,indexSql,filedSql){
	if($("#"+falias).length>0){
		$("#"+falias).remove();
		$("#br_"+falias).remove();
	}
	if($("#data_"+falias).length>0){
		$("#data_"+falias).remove();
	}
	
	$('.table').append('<div class="table-content" id="'+falias+'">'
			+'<div class="table-short">'+falias+'</div>'
			+'<div class="table-short">'+drAlias+'</div>'
			+'<div class="table-short">'+relationName+'</div>'
			+'<div class="table-short">'+getIndex(indexAlias)+'</div>'
			+'<div class="table-SQL">'+getIndex(indexSql)+'</div>'
			+'<div class="table-SQL">'+filedSql+'</div>'
			+'<div class="table-operate"><span class="clear" onclick="clearAssist(\''+falias+'\')">清除</span>'
			+'<span class="edit" onclick="editorFsql(\''+falias+'\')">编辑</span></div></div><br id="br_'+falias+'">');
	$("#hiddenSqlData").append('<input type="hidden" id="data_'+falias+'" name="data_'+falias+'" value="'
			+drAlias+'|'+relation+'|'+indexAlias+'|'+indexSql+'|'+filedSql+'">');
}
function editorFsql(falias){
	var values = $("#data_"+falias).val();
	if(values=="")
		return;
	var valueArray = values.split("|");
	$("#falias").val(falias);
	$("#drAlias").val(valueArray[0]);
	$("#relation").val(valueArray[1]);
	relation = valueArray[1];
	if(valueArray[1]==1){
		$(".relation li").eq(0).addClass('double');
		$(".relation li").eq(1).removeClass('double');
	}else{
		$(".relation li").eq(0).removeClass('double');
		$(".relation li").eq(1).addClass('double');
	}
	$("#indexAlias").val(valueArray[2]);
	$("#indexSql").val(valueArray[3]);
	$("#filedSql").val(valueArray[4]);
}

function getIndex(index){
	if(index==""){return "&nbsp;";}else{return index;}
}
function validateSql(){
	var v = true;
	if($("#falias").val()==""){
		alert("别名不能为空！");
		$("#falias").focus();
		return false;
	}
	if(!check($("#falias").val()))
		return false;
	if($("#filedSql").val()==""){
		alert("sql不能为空！");
		$("#filedSql").focus();
		return false;
	}
	//验证别名是否重复
	if(v&&!checkAlias($("#falias").val())){
		v=confirm("辅助表别名有重复，是否覆盖？");
	}
	return v;
}


function boxNone(){
	$('.SQL-txt').css('display','none');
}
function boxSee(){
	$('#main-sql').css('display','block');
}
function boxSeeIndexSql(){
	$('#assist-index-sql').css('display','block');
}
function boxSee3(i){
	$('#'+num1+'').children('.SQL-txt').css('display','block');

}
function boxSeeFiledSql(){
	$('#assist-filed-sql').css('display','block');
}
function boxHiddenFiledSql(){
	$('#assist-filed-sql').css('display','none');
}
function boxNone3(i){
	$('#'+num1+'').children('.SQL-txt').css('display','none');

}

function clearAssist(id){
	$('#'+id+'').remove();
	$('#data_'+id).remove();
	$('#br_'+id).remove();
}
var flag=true;
function turn(num){
	if (num==num) {
		if (flag) {
			$('.'+num+'').html('收缩');
			$('.'+num+'').css('background','url(../images/off.jpg) no-repeat right center');
			flag=false;
		}else{
			$('.'+num+'').html('展开');
			$('.'+num+'').css('background','url(../images/on.jpg) no-repeat right center');
			flag=true;
		};
	};
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
	$(".table-content").each(function () {
		if(alias==$(this).attr("id")){
//			alert("别名有重复");
			vv = false;
			return;
		}
	});
	return vv;
}
function getRelationName(relation){
	if(relation==1){
		return "一对一";
	}
	if(relation==2){
		return "一对多";
	}
}
function validatePriSql(){
	var v = true;
	if($("#palias").val()==""){
		alert("主表别名不能为空！");
		$("#palias").focus();
		return false;
	}
	if(!check($("#palias").val()))
		return false;
	if($("#primaryKey").val()==""){
		alert("主键不能为空！");
		$("#primaryKey").focus();
		return false;
	}
	if($("#sql").val()==""){
		alert("sql不能为空！");
		$("#sql").focus();
		return false;
	}
	if(v){
		if($("#pri_data_"+$("#palias").val()).length>0){
			$("#pri_data_"+$("#palias").val()).remove();
		}
		$("#hiddenSqlData").append('<input type="hidden" id="pri_data_'+falias+'" name="pri_data_'+$("#palias").val()+'" value="'
				+$("#pdsalias").val()+'|'+$("#primaryKey").val()+'|'+$("#sql").val()+'">');
	}
	return v;
}

function initPriSql(alias,dsAlias,priKey,sql){
	$("#palias").val(alias);
	$("#pdsalias").val(dsAlias);
	$("#primaryKey").val(priKey);
	$("#sql").val(sql);
}


function LogOut(){
	if(confirm("是否需要切换用户？")){
		window.location=ctx+"/login/doLogout";
	}
}