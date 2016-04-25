//parseInt("bc",16); //表示把字符串bc转换为16进制，结果：188
//　　parseInt("10",8); //表示把字符串10转换为8进制，结果：8
//　　parseInt("10",2); //表示把字符串10转换为2进制，结果：2	
var chars = [ '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C',
		'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q',
		'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c',
		'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
		'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' ];
function generateMixed(n) {
	var res = "";
	for (var i = 0; i < n; i++) {
		var id = Math.ceil(Math.random() * 61);
		res += chars[id];
	}
	return res;
}


//校验特殊字符
function stripscript(s) {
	if(s=="")
		return s;
	s = s.replace(/\n/g, " ");
    s = s.replace(/\t/g, " ");
    s = s.replace(/\\n/g, " ");
    s = s.replace(/\\t/g, " ");
    s = s.replace(/\\r/g, " ");
    s = s.replace(/\\f/g, " ");
    s = s.replace(/\\b/g, " ");
    s = s.replace(/"/g, "'");
    var rs = "";
    var pattern = new RegExp("[`~!#$^&|{}:;\\[\\]<>/?~！#￥……&（）&;—|{}【】’‘；：”“。，、？\]")
    for (var i = 0; i < s.length; i++) {
        rs = rs + s.substr(i, 1).replace(pattern, '');
    }
    
    return rs;
}
