// Simple yet flexible JSON editor plugin.
// Turns any element into a stylable interactive JSON editor.

// Copyright (c) 2013 David Durman

// Licensed under the MIT license (http://www.opensource.org/licenses/mit-license.php).

// Dependencies:

// * jQuery
// * JSON (use json2 library for browsers that do not support JSON natively)

// Example:

//     var myjson = { any: { json: { value: 1 } } };
//     var opt = { change: function() { /* called on every change */ } };
//     /* opt.propertyElement = '<textarea>'; */ // element of the property field, <input> is default
//     /* opt.valueElement = '<textarea>'; */  // element of the value field, <input> is default
//     $('#mydiv').jsonEditor(myjson, opt);

(function( $ ) {

    $.fn.jsonEditor = function(json, options) {
        options = options || {};
        // Make sure functions or other non-JSON data types are stripped down.
        json = parse(stringify(json));
        
        var K = function() {};
        var onchange = options.change || K;
        var onpropertyclick = options.propertyclick || K;

        return this.each(function() {
            JSONEditor($(this), json, onchange, onpropertyclick, options.propertyElement, options.valueElement);
        });
        
    };
    
    function JSONEditor(target, json, onchange, onpropertyclick, propertyElement, valueElement) {
        var opt = {
            target: target,
            onchange: onchange,
            onpropertyclick: onpropertyclick,
            original: json,
            propertyElement: propertyElement,
            valueElement: valueElement
        };
        construct(opt, json, opt.target);
        
        $(opt.target).on('blur focus', '.property, .value', function() {
            $(this).toggleClass('editing');
        });
       
    }

    function isObject(o) { return Object.prototype.toString.call(o) == '[object Object]'; }
    function isArray(o) { return Object.prototype.toString.call(o) == '[object Array]'; }
    function isBoolean(o) { return Object.prototype.toString.call(o) == '[object Boolean]'; }
    function isNumber(o) { return Object.prototype.toString.call(o) == '[object Number]'; }
    function isString(o) { return Object.prototype.toString.call(o) == '[object String]'; }
    var types = 'object array boolean number string null';

    // Feeds object `o` with `value` at `path`. If value argument is omitted,
    // object at `path` will be deleted from `o`.
    // Example:
    //      feed({}, 'foo.bar.baz', 10);    // returns { foo: { bar: { baz: 10 } } }
    function feed(o, path, value) {
        var del = arguments.length == 2;
        
        if (path.indexOf('.') > -1) {
            var diver = o,
                i = 0,
                parts = path.split('.');
            for (var len = parts.length; i < len - 1; i++) {
                diver = diver[parts[i]];
            }
            if (del) delete diver[parts[len - 1]];
            else diver[parts[len - 1]] = value;
        } else {
            if (del) delete o[path];
            else o[path] = value;
        }
        return o;
    }

    // Get a property by path from object o if it exists. If not, return defaultValue.
    // Example:
    //     def({ foo: { bar: 5 } }, 'foo.bar', 100);   // returns 5
    //     def({ foo: { bar: 5 } }, 'foo.baz', 100);   // returns 100
    function def(o, path, defaultValue) {
        path = path.split('.');
        var i = 0;
        while (i < path.length) {
            if ((o = o[path[i++]]) == undefined) return defaultValue;
        }
        return o;
    }

    function error(reason) { if (window.console) { console.error(reason); } }
    
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
    
    function addExpander(item) {
        if (item.children('.expander').length == 0) {
            var expander =   $('<span>',  { 'class': 'expander' });
            expander.bind('click', function() {
                var item = $(this).parent();
                item.toggleClass('expanded');
            });
            item.prepend(expander);
        }
    }

    function addListAppender(item, handler) {
        var appender = $('<div>', { 'class': 'item appender' }),
            btn      = $('<button></button>', { 'class': 'property btn btn-sm  btn-primary' });
        btn.text('添加字段');
        appender.append(btn);
        item.append(appender);

        btn.click(handler);
        
        /** 删除特定情况下的添加按钮*/
        var addNewBtn = item.children().last();
        var title = addNewBtn.prev().find("input").attr('title');
        if(title == "store" || title == "properties" || title == "format"){
        	addNewBtn.remove();
        }
        
        return appender;
    }

    function addNewValue(json) {
    
        if (isArray(json)) {
            json.push(null);
            return true;
        }

        if (isObject(json)) {
            var i = 1, newName = "newkey";

            while (json.hasOwnProperty(newName)) {
                newName = "newkey" + i;
                i++;
            }

            json[newName] = null;
            return true;
        }

        return false;
    }
    
    function construct(opt, json, root, path) {
        path = path || '';
        
        root.children('.item').remove();
        
        for (var key in json) {
        	
            if (!json.hasOwnProperty(key)) continue;

            var item     =   $('<div>',   { 'class': 'item', 'data-path': path }),
                property =   $(opt.propertyElement || '<input>', { 'class': 'property' }),
                value    =   $('<select>', { 'class': 'value'    });
                //value    =   $(opt.valueElement || '<input>', { 'class': 'value'    });

            if (isObject(json[key]) || isArray(json[key])) {
                addExpander(item);
            }
            
            item.append(property);
            root.append(item);
            property.val(key).attr('title', key);
            var val = stringify(json[key]);
            var prop = property.val();
            if(prop == "type"|| prop == "index" || prop == "analyze" || prop == "store" || prop == "properties"){
            	property.attr('disabled', 'disabled').css("color", "#555").css("border", "0px");
            }

            var option0 = $("<option>").val("\"string\"").text("data-type");
            var option1 = $("<option>").val("\"string\"").text("string");
            var option2 = $("<option>").val("\"long\"").text("long");
            var option3 = $("<option>").val("\"integer\"").text("integer");
            var option4 = $("<option>").val("\"short\"").text("short");
            var option5 = $("<option>").val("\"byte\"").text("byte");
            var option6 = $("<option>").val("\"double\"").text("double");
            var option7 = $("<option>").val("\"float\"").text("float");
            var option8 = $("<option>").val("\"date\"").text("date");
            var option9 = $("<option>").val("\"boolean\"").text("boolean");
            var option10 = $("<option>").val("\"object\"").text("object");
            
            if (key == "type") {
                value.append(option1).append(option2).append(option3).append(option4).append(option5).append(option6).append(option7).append(option8).append(option9).append(option10).val(val);
                item.append(value);
            }else if (key == "format" ) {
                var option1 = $("<option>").val("yyyy-MM-dd").text("yyyy-MM-dd");
                var option2 = $("<option>").val("yyyy-MM-dd HH:mm").text("yyyy-MM-dd HH:mm");
                var option3 = $("<option>").val("yyyy-MM-dd HH:mm:ss").text("yyyy-MM-dd HH:mm:ss");
                value.append(option1).append(option2).append(option3).val(val);
                item.append(value);
            }else if (key == "index" || key == "analyze" || key == "store" ) {
                var option1 = $("<option>").val(true).text(true);
                var option2 = $("<option>").val(false).text(false);
                value.append(option1).append(option2).val(val);
                item.append(value);
            }else {
              
                if (val && val != "null") {
                    value    =   $(opt.valueElement || '<input>', { 'class': 'value' }); 
                    item.append(value);
                    value.val(val).attr('title', val).hide();
                }else if(val == "null"){
                    value.append(option0).append(option1).append(option2).append(option3).append(option4).append(option5).append(option6).append(option7).append(option8).append(option9).append(option10).val(val);
                    item.append(value);
                }
               
            }
            
            assignType(item, json[key]);

            property.change(propertyChanged(opt));
            value.change(valueChanged(opt));
            property.click(propertyClicked(opt));
            
            if (isObject(json[key]) || isArray(json[key])) {
                construct(opt, json[key], item, (path ? path + '.' : '') + key);
            }
        }

        if (isObject(json) || isArray(json)) {
            addListAppender(root, function () {
                addNewValue(json);
                construct(opt, json, root, path);
                opt.onchange(parse(stringify(opt.original)));
            })
        }


    }

    function updateParents(el, opt) {
        $(el).parentsUntil(opt.target).each(function() {
            var path = $(this).data('path');
            path = (path ? path + '.' : path) + $(this).children('.property').val();
            var val = stringify(def(opt.original, path, null));
            $(this).children('.value').val(val).attr('title', val);
        });
    }

    function propertyClicked(opt) {
        return function() {
            var path = $(this).parent().data('path');            
            var key = $(this).attr('title');

            var safePath = path ? path.split('.').concat([key]).join('\'][\'') : key;
            
            opt.onpropertyclick('[\'' + safePath + '\']');
        };
    }
    
    function propertyChanged(opt) {
        return function() {
            var path = $(this).parent().data('path'),
                val = parse($(this).next().val()),
                newKey = $(this).val().toLowerCase(),
                oldKey = $(this).attr('title');

            $(this).attr('title', newKey);
            $(this).val(newKey);
            feed(opt.original, (path ? path + '.' : '') + oldKey);
            if (newKey) feed(opt.original, (path ? path + '.' : '') + newKey, val);

            updateParents(this, opt);
            opt.onchange(parse(stringify(opt.original)));
            var copyto = assembleCopytoJson();
            if (!newKey) {
            	$(this).parent().remove();
            	$("#fulltextSection ul").remove();
            	var cpOldKeyFront = "\"" + oldKey + "\",";
            	var cpOldKeyLast = ",\"" + oldKey + "\"";
            	var rmOldKeyStr;
            	if(copyto.indexOf(cpOldKeyFront) > -1){
            		rmOldKeyStr = copyto.replaceAll(cpOldKeyFront, "");
            	}else if(copyto.indexOf(cpOldKeyLast) > -1){
            		rmOldKeyStr = copyto.replaceAll(cpOldKeyLast, "");
            	}
                initCopyto(rmOldKeyStr);
                
            }else {
            	$("#fulltextSection ul").remove();
            	var replaceOldKeyStr = copyto.replaceAll(oldKey, newKey);
                initCopyto(replaceOldKeyStr);
            }
            var selector = $("#pk");
			if(selector.val() == oldKey){
				selector.empty();
				selector.append('<option value="_id">_id</option>'); 
				selector.val("_id");
			}
        };
    }

    function valueChanged(opt) {
        return function() {
            var key = $(this).prev().val(),
                val = parse($(this).val() || 'null'),
                item = $(this).parent(),
                path = item.data('path');
            if(key == "format"){
            	var formatSelect = item.find("select");
            	val = formatSelect.val();
            	formatSelect.find("option[text='"+val+"']").attr("selected",true);
            	feed(opt.original, (path ? path + '.format' : ''), val);
            }
            if(item.hasClass("null")){
                if((val == "integer" || val == "string"  || val == "long" || val == "date" || 
                	val == "short" || val == "byte" || val == "double" || val == "boolean" || val == "float")){
                    var type = val;
                    val={"type":"integer","index":true, "analyze": true ,"store":false};
                    val.type = type;
                }else if(val=="object"){
                    val={"properties": {"field-name":{"type":"integer","index":true, "analyze": true ,"store":false}}};
                }

                item.find('select').remove();
                var value    =   $('<input>', { 'class': 'value'    }); 
                item.append(value).removeClass("null").addClass("object");
                value.val(stringify(val)).attr('title', stringify(val)).hide(); 
            }
            
            feed(opt.original, (path ? path + '.' : '') + key, val);
           

            if ((isObject(val) || isArray(val)) && !$.isEmptyObject(val)) {
                construct(opt, val, item, (path ? path + '.' : '') + key);
                //var addNewBtn = item.find('.expander, .item');
               
                addExpander(item);
            } else {
                item.find('.expander, .item').remove();
            }

            assignType(item, val);

            updateParents(this, opt);
            
            opt.onchange(parse(stringify(opt.original)));
            
            
        };
    }
    
    function assignType(item, val) {
        var className = 'null';
        
        if (isObject(val)) className = 'object';
        else if (isArray(val)) className = 'array';
        else if (isBoolean(val)) className = 'boolean';
        else if (isString(val)) className = 'string';
        else if (isNumber(val)) className = 'number';

        item.removeClass(types);
        item.addClass(className);
    }

})( jQuery );
