
function track(action) {
    if (typeof _gaq !== 'undefined') {
    _gaq.push(['_trackEvent', 'JSON', action]);
    }
}
//
var json = JSON.parse($("#json").val());
//{"newKey1":{"type":"string","index":true,"analyze":true,"store":true},"newKey2":{"type":"integer","index":true,"analyze":true,"store":true},"newKey":{"field-name1":{"type":"integer","index":true,"analyze":true,"store":true}}}
;

function printJSON() {
    $('#json').val(JSON.stringify(json));

}

function updateJSON(data) {
    json = data;
    printJSON();
}

function showPath(path) {
    $('#path').text(path);
}

$(document).ready(function() {
    $('#beautify').click(function(evt) {
            track('Beautify');
                evt.preventDefault();
                var jsonText = $('#json').val();
                $('#json').val(JSON.stringify(JSON.parse(jsonText), null, 4));
                $('#json').autosize();
            });
    $('#uglify').click(function(evt) {
        track('Uglify');
            evt.preventDefault();
            var jsonText = $('#json').val();
            $('#json').val(JSON.stringify(JSON.parse(jsonText)));
            $('#json').autosize();
        });
    $('#rest > button').click(function() {
        var url = $('#rest-url').val();
        $.ajax({
            url: url,
            dataType: 'jsonp',
            jsonp: $('#rest-callback').val(),
            success: function(data) {
                json = data;
                $('#editor').jsonEditor(json, { change: updateJSON, propertyclick: showPath});
                printJSON();
            },
            error: function() {
                alert('Something went wrong, double-check the URL and callback parameter.');
            }
        });
    });

    $('#json').change(function() {
        var val = $('#json').val();

        if (val) {
            try { json = JSON.parse(val); }
            catch (e) { alert('Error in parsing json. ' + e); }
        } else {
            json = {};
        }
        
        $('#editor').jsonEditor(json, { change: updateJSON, propertyclick: showPath});
    });

    $('#expander').click(function() {
        var editor = $('#editor');
        editor.toggleClass('expanded');
        $(this).text(editor.hasClass('expanded') ? '收起' : '展开');
    });
    
    printJSON();
    $('#editor').jsonEditor(json, { change: updateJSON, propertyclick: showPath});
    $("#editor").find("input[title='store']").each(function(){
    	$(this).parent().next().remove();
    });
    $("#editor").find("input[title='properties']").each(function(){
    	$(this).parent().parent().children(".appender").remove();
    });
});


