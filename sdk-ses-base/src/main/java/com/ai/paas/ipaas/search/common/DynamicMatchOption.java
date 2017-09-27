package com.ai.paas.ipaas.search.common;

import java.util.ArrayList;
import java.util.List;

/**
 * 设置动态模板是哪些字段用于分词，哪些分词不使用分词，特别对于String类型。
 * 构造参数里面，当使用simple时，使用*xx，或者xx*模式，如*Id,会匹配userId,
 * 构造参数使用pattern时，支持正则表达式："^profit_\d+$" 使用PATH时，name.*，*.middle 注意默认是不分词
 * 而且要注意顺序，按顺序匹配 即在最后要有个* 的通配，对于String
 * 
 * @author douxiaofeng
 *
 */
public class DynamicMatchOption {
	public enum MatchType {
		// 对于QueryString 或者 Match的词之间的关系
		SIMPLE(1), PATTERN(2), PATH(3);
		private final int value;

		MatchType(int v) {
			value = v;
		}

		@org.codehaus.jackson.annotate.JsonValue
		public int value() {
			return value;
		}

		@org.codehaus.jackson.annotate.JsonCreator
		public static MatchType fromValue(int typeCode) {
			for (MatchType c : MatchType.values()) {
				if (c.value == typeCode) {
					return c;
				}
			}
			throw new IllegalArgumentException("Invalid Match type code: " + typeCode);

		}
	}

	private String name;
	private String match;
	private String unmatch;
	private MatchType matchType = MatchType.SIMPLE;
	private boolean analyzed = false;

	public DynamicMatchOption(String name, MatchType matchType, String match, String unmatch, boolean analyzed) {
		this.name = name;
		this.matchType = matchType;
		this.match = match;
		this.unmatch = unmatch;
		this.analyzed = analyzed;
	}

	public String getMatch() {
		return match;
	}

	public void setMatch(String match) {
		this.match = match;
	}

	public String getUnmatch() {
		return unmatch;
	}

	public void setUnmatch(String unmatch) {
		this.unmatch = unmatch;
	}

	public MatchType getMatchType() {
		return matchType;
	}

	public void setMatchType(MatchType matchType) {
		this.matchType = matchType;
	}

	public boolean isAnalyzed() {
		return analyzed;
	}

	public void setAnalyzed(boolean analyzed) {
		this.analyzed = analyzed;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public static void main(String[] args) {
		List<DynamicMatchOption> matchs = new ArrayList<>();
		// 以id开头的都不分词
		DynamicMatchOption matchNotAnalyzed = new DynamicMatchOption("NotAnalyzed",
				DynamicMatchOption.MatchType.PATTERN, "^\\S*?Id$", null, false);
		matchs.add(matchNotAnalyzed);
		// 以name\title\content的进行分词
		DynamicMatchOption matchAnalyzed = new DynamicMatchOption("Analyzed", DynamicMatchOption.MatchType.PATTERN,
				"^\\S*?[(name)|(title)|(content)]\\S*?$", null, false);
		// 默认不进行分词
		matchs.add(matchAnalyzed);
		DynamicMatchOption defaultNotAnalyzed = new DynamicMatchOption("default", DynamicMatchOption.MatchType.SIMPLE,
				"*", null, false);
		matchs.add(defaultNotAnalyzed);
	}
}
