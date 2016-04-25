var escapeable = /["\\\x00-\x1f\x7f-\x9f]/g, meta = {
	'\b' : '\\b',
	'\t' : '\\t',
	'\n' : '\\n',
	'\f' : '\\f',
	'\r' : '\\r',
	'"' : '\\"',
	'\\' : '\\\\'
};

var replaceEscape = function(string) {
	if (!string && typeof(string)!="undefined" && string!=0){
		if (string.match(escapeable)) {
			return '"'
					+ string.replace(escapeable, function(a) {
						var c = meta[a];
						if (typeof c === 'string') {
							return c;
						}
						c = a.charCodeAt();
						return '\\u00' + Math.floor(c / 16).toString(16)
								+ (c % 16).toString(16);
					}) + '"';
		}
	}
	return string;
}