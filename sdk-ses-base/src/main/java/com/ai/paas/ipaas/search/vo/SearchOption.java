package com.ai.paas.ipaas.search.vo;


//搜索选项定义

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

public class SearchOption implements Serializable {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private SearchLogic searchLogic = SearchLogic.must;
	private SearchType searchType = SearchType.querystring;
	private DataFilter dataFilter = DataFilter.exists;
	private TermOperator termOperator = TermOperator.OR;
	/* querystring精度，取值[1-100]的整数 */
	private String queryStringPrecision = "90%";
	/* 排名权重 */
	private float boost = 1.0f;
	private boolean highlight = false;

	public enum TermOperator {
		// 对于QueryString 或者 Match的词之间的关系
		AND(1), OR(2);
		private final int value;

		TermOperator(int v) {
			value = v;
		}

		@org.codehaus.jackson.annotate.JsonValue
		public int value() {
			return value;
		}

		@org.codehaus.jackson.annotate.JsonCreator
		public static TermOperator fromValue(int typeCode) {
			for (TermOperator c : TermOperator.values()) {
				if (c.value == typeCode) {
					return c;
				}
			}
			throw new IllegalArgumentException("Invalid Status type code: " + typeCode);

		}
	}

	public enum SearchType {
		/* 按照quert_string搜索，搜索非词组时候使用 */
		querystring(1)
		/* 按照区间搜索 */, range(2)
		/* 按照词组搜索，搜索一个词时候使用 */, term(3), match(4);
		private final int value;

		SearchType(int v) {
			value = v;
		}

		@org.codehaus.jackson.annotate.JsonValue
		public int value() {
			return value;
		}

		@org.codehaus.jackson.annotate.JsonCreator
		public static SearchType fromValue(int typeCode) {
			for (SearchType c : SearchType.values()) {
				if (c.value == typeCode) {
					return c;
				}
			}
			throw new IllegalArgumentException("Invalid Status type code: " + typeCode);

		}
	}

	public enum SearchLogic {
		/* 逻辑must关系 */
		must(1)
		/* 逻辑should关系 */, should(2), must_not(3);
		private final int value;

		SearchLogic(int v) {
			value = v;
		}

		@org.codehaus.jackson.annotate.JsonValue
		public int value() {
			return value;
		}

		@org.codehaus.jackson.annotate.JsonCreator
		public static SearchLogic fromValue(int typeCode) {
			for (SearchLogic c : SearchLogic.values()) {
				if (c.value == typeCode) {
					return c;
				}
			}
			throw new IllegalArgumentException("Invalid Status type code: " + typeCode);

		}

		public void convertQueryBuilder(BoolQueryBuilder rootQueryBuilder, QueryBuilder childQueryBuilder) {
			switch (this) {
			case should: {
				rootQueryBuilder.should(childQueryBuilder);
				break;
			}
			case must: {
				rootQueryBuilder.must(childQueryBuilder);
				break;
			}
			case must_not: {
				rootQueryBuilder.mustNot(childQueryBuilder);
				break;
			}
			default: {
				throw new RuntimeException("");
			}
			}
		}
	}

	/**
	 * @author DOUXF return enum
	 */
	public enum DataFilter {
		/* 只显示有值的 *//* 显示没有值的 *//* 显示全部 */
		exists(1), notExists(2), all(3);
		private final int value;

		DataFilter(int v) {
			value = v;
		}

		@org.codehaus.jackson.annotate.JsonValue
		public int value() {
			return value;
		}

		@org.codehaus.jackson.annotate.JsonCreator
		public static DataFilter fromValue(int typeCode) {
			for (DataFilter c : DataFilter.values()) {
				if (c.value == typeCode) {
					return c;
				}
			}
			throw new IllegalArgumentException("Invalid Status type code: " + typeCode);

		}

	}

	public SearchOption(SearchType searchType, SearchLogic searchLogic, String queryStringPrecision,
			DataFilter dataFilter, float boost, int highlight) {
		this.setSearchLogic(searchLogic);
		this.setSearchType(searchType);
		this.setQueryStringPrecision(queryStringPrecision);
		this.setDataFilter(dataFilter);
		this.setBoost(boost);
		this.setHighlight(highlight > 0 ? true : false);
	}

	public SearchOption() {
	}

	public SearchOption(SearchLogic searchLogic, SearchType searchType) {
		this.setSearchLogic(searchLogic);
		this.setSearchType(searchType);
	}

	public SearchOption(SearchLogic searchLogic, SearchType searchType, TermOperator termOperator) {
		this.setSearchLogic(searchLogic);
		this.setSearchType(searchType);
		this.termOperator = termOperator;
	}

	public TermOperator getTermOperator() {
		return termOperator;
	}

	public void setTermOperator(TermOperator termOperator) {
		this.termOperator = termOperator;
	}

	public DataFilter getDataFilter() {
		return this.dataFilter;
	}

	public void setDataFilter(DataFilter dataFilter) {
		this.dataFilter = dataFilter;
	}

	public boolean isHighlight() {
		return this.highlight;
	}

	public void setHighlight(boolean highlight) {
		this.highlight = highlight;
	}

	public float getBoost() {
		return this.boost;
	}

	public void setBoost(float boost) {
		this.boost = boost;
	}

	public SearchLogic getSearchLogic() {
		return this.searchLogic;
	}

	public void setSearchLogic(SearchLogic searchLogic) {
		this.searchLogic = searchLogic;
	}

	public SearchType getSearchType() {
		return this.searchType;
	}

	public void setSearchType(SearchType searchType) {
		this.searchType = searchType;
	}

	public String getQueryStringPrecision() {
		return this.queryStringPrecision;
	}

	public void setQueryStringPrecision(String queryStringPrecision) {
		this.queryStringPrecision = queryStringPrecision;
	}

	public static long getSerialversionuid() {
		return SearchOption.serialVersionUID;
	}

	public static String formatDate(Object object) {
		if (object instanceof java.util.Date) {
			return SearchOption.formatDateFromDate((java.util.Date) object);
		}
		return SearchOption.formatDateFromString(object.toString());
	}

	public static boolean isDate(Object object) {
		return object instanceof java.util.Date
				|| Pattern.matches("[1-2][0-9][0-9][0-9]-[0-9][0-9].*", object.toString());
	}

	public static String formatDateFromDate(Date date) {
		if (null == date)
			return null;
		SimpleDateFormat dateFormat_hms = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZ");
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		try {
			String result = dateFormat_hms.format(date);
			return result;
		} catch (Exception e) {
		}
		try {
			String result = dateFormat.format(date) + "00:00:00";
			return result;
		} catch (Exception e) {
		}
		return dateFormat_hms.format(new Date());
	}

	public static String formatDateFromString(String date) {
		if (null == date)
			return null;
		SimpleDateFormat gmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZ");
		SimpleDateFormat normal = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat simple = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date value = gmt.parse(date);
			return SearchOption.formatDateFromDate(value);
		} catch (Exception e) {
		}
		try {
			Date value = normal.parse(date);
			return SearchOption.formatDateFromDate(value);
		} catch (Exception e) {
		}
		try {
			Date value = simple.parse(date);
			return SearchOption.formatDateFromDate(value);
		} catch (Exception e) {
		}
		return gmt.format(new Date());
	}
}