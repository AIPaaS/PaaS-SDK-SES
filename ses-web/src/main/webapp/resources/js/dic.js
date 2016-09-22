


function validateDic(){
	var v = true;
	var indexWord = $("#indexWord").val();
	var stopWord = $("#stopWord").val();
	if(stopWord=="" && indexWord==""){
		alert("还未选择上传文件");
		v = false;
    	return v;
	}
	if(indexWord!=""){
		var indexExt = indexWord.substring(indexWord.lastIndexOf("."),indexWord.length);
		if( indexExt !=".dic"&&indexExt != ".txt"){
			alert("文件格式必须为.dic 或 .txt格式");
			v = false;
	    	return v;
		}
	}
	if(stopWord!=""){
		var stopExt = stopWord.substring(stopWord.lastIndexOf("."),stopWord.length);
		if(stopExt!=".dic"&&stopExt != ".txt"){
			alert("文件格式必须为.dic 或 .txt格式");
			v = false;
	    	return v;
		}
	}
	return v;
}

function  elasticsearchFilds(){
	var filds=$("#filds").val();
	var inputText = $("#inputText").val();
	var addr = $("#addr").val();
	var indexName = $("#indexName").val();
	if(filds=="" || filds==undefined){
		alert("请选择分词字段");
		return false;
	}
	if(inputText==""){
		alert("请请输入分词内容");
		return false;
	}
	//执行分词
	var url = "${ctx}../../../dic/getFildWords";
	$.ajax({
    	   type: "POST",
    	   url: url,	
    	   dataType : "text",
		   data : {filds:filds,inputText:inputText,addr:addr,indexName:indexName},
		   success:function(data){
			   $("#showText").text(data);
		   }
       }); 
}

