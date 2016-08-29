package com.ai.paas.ipaas.search.common;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class JsonBuilder {
	private XContentBuilder _builder = null;

	public JsonBuilder() throws Exception {
		_builder = XContentFactory.jsonBuilder();
	}

	public JsonBuilder startObject() throws IOException {
		_builder.startObject();
		return this;
	}

	public JsonBuilder endObject() throws IOException {
		_builder.endObject();
		return this;
	}

	public JsonBuilder field(String name, String value) throws IOException {
		_builder.field(name, value);
		return this;
	}

	public JsonBuilder field(String name, Integer value) throws IOException {
		_builder.field(name, value);
		return this;
	}

	public JsonBuilder field(String name, Date value) throws IOException {
		_builder.field(name, value);
		return this;
	}

	public JsonBuilder field(String name, int value) throws IOException {
		_builder.field(name, value);
		return this;
	}

	public JsonBuilder field(String name, Long value) throws IOException {
		_builder.field(name, value);
		return this;
	}

	public JsonBuilder field(String name, long value) throws IOException {
		_builder.field(name, value);
		return this;
	}

	public JsonBuilder field(String name, Float value) throws IOException {
		_builder.field(name, value);
		return this;
	}

	public JsonBuilder field(String name, float value) throws IOException {
		_builder.field(name, value);
		return this;
	}

	public JsonBuilder field(String name, Double value) throws IOException {
		_builder.field(name, value);
		return this;
	}

	public JsonBuilder field(String name, double value) throws IOException {
		_builder.field(name, value);
		return this;
	}

	public XContentBuilder getBuilder() {
		return _builder;
	}

	public String toString() {
		return _builder.toString();
	}

}
