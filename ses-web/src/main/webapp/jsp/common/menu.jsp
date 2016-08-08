<%@ page language="java" pageEncoding="UTF-8"%>
<div class="ui navbar ng-scope">
	<ul class="nav nav-pills nav-stacked">

		<li class="logo logoa"><a href="${ctx}/ses/mapping" id="mappingNav" data-toggle="tooltip" data-placement="right" title="模型构建"><i class="icon-wrench"></i></a></li>
		<li class="logo logoa"><a href="${ctx}/dataimport/toOne"  id="toOneNav" data-toggle="tooltip" data-placement="right" title="简单模型数据导入"><i class="icon-home"></i></a></li>
		<li class="logo logoa"><a href="${ctx}/dataimport/toMany"  id="toManyNav" data-toggle="tooltip" data-placement="right" title="复杂模型数据导入"><i class="icon-sitemap"></i></a></li>
		<li class="logo logoa"><a href="${ctx}/dic/index"  id="indexNav" data-toggle="tooltip" data-placement="right" title="词库管理"><i class="icon-tasks"></i></a></li>
		<li class="logo logoa"><a href="${ctx}/overview/overview"  id="overviewNav" data-toggle="tooltip" data-placement="right" title="数据查看"><i class="icon-eye-open"></i></a></li>
	</ul>

	<ul class="nav nav-bottom nav-pills nav-stacked">
		<li class="logo"><a href="${ctx}"><i class="icon-qrcode"></i></a></li>
	</ul>
</div>
<script type="text/javascript">
	String.prototype.endWith = function(s) {
		if (s == null || s == "" || this.length == 0 || s.length > this.length)
			return false;
		if (this.substring(this.length - s.length) == s)
			return true;
		else
			return false;
		return true;
	}
	$(function() {
		$('[data-toggle="tooltip"]').tooltip();
		var lct = window.location.href;
		var currentPage = lct.substring(lct.lastIndexOf('/') + 1, lct.length);
		
		if(currentPage == "assembleMapping"){
			$("#mappingNav").css("background-color","#353c43");
			$("#mappingNav i").css("color","#2ecc71");
		}else {
			$("#"+currentPage+"Nav").css("background-color","#353c43");
			$("#"+currentPage+"Nav i").css("color","#2ecc71");
		}

	})
</script>