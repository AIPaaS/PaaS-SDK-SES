


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

