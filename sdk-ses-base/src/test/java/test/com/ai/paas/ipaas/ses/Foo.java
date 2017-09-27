package test.com.ai.paas.ipaas.ses;

public class Foo {
	private String id;
	private String name;
	private long age;
	private String desc;

	public Foo(String id, String name, long age, String desc) {
		this.id = id;
		this.name = name;
		this.age = age;
		this.desc = desc;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getAge() {
		return age;
	}

	public void setAge(long age) {
		this.age = age;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}
