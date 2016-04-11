import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Test {

	public static void main(String[] args) {
		System.out.println(encode("中文"));
		System.out.println(decode(encode("中文")));
//		try {
//			column();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
		
		
		System.out.println("\"anbc");
		System.out.println("\"anbc");
	}

	private static void column() throws SQLException {
		org.apache.tomcat.jdbc.pool.PoolProperties p = new org.apache.tomcat.jdbc.pool.PoolProperties();
		p.setUrl("jdbc:mysql://10.1.228.202:31316/devrdb21");
		p.setDriverClassName("com.mysql.jdbc.Driver");
		p.setValidationQuery("SELECT 1");
		p.setUsername("devrdbusr21");
		p.setPassword("devrdbusr21");
		p.setJmxEnabled(true);
		p.setTestWhileIdle(false);
		p.setTestOnBorrow(true);
		p.setTestOnReturn(false);
		p.setValidationInterval(30000);
		p.setTimeBetweenEvictionRunsMillis(30000);
		p.setMaxActive(20);
		p.setInitialSize(1);
		p.setMaxWait(10000);
		p.setRemoveAbandonedTimeout(60);
		p.setMinEvictableIdleTimeMillis(30000);
		p.setMinIdle(1);
		p.setLogAbandoned(true);
		p.setRemoveAbandoned(true);
		p.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;"
				+ "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer");
		org.apache.tomcat.jdbc.pool.DataSource datasource = new org.apache.tomcat.jdbc.pool.DataSource();
		datasource.setPoolProperties(p);

		Connection con = null;
		PreparedStatement statement = null;
		String sql = "select id,cache_host_Ip,cache_memory as cacheMemory from mcs_resource_pool";
		ResultSet rs = null;

		try {
			con = datasource.getConnection();
			statement = con.prepareStatement(sql);
			rs = statement.executeQuery();
			while (rs.next()) {
				for (int j = 0; j < rs.getMetaData().getColumnCount(); j++) {
					System.out.print(rs.getMetaData().getColumnName(j + 1)
							+ "=" + rs.getString(j + 1) + "\t");
					System.out.print(rs.getMetaData().getColumnLabel(j + 1)
							+ "=" + rs.getString(j + 1) + "\t");
				}
				System.out.println();
			}

		} catch (Exception ee) {
			ee.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (statement != null) {
				statement.close();
			}
			if (con != null) {
				con.close();
			}
		}

	}

	// 转化字符串为十六进制编码
	public static String toHexString(String s) {
		String str = "";
		for (int i = 0; i < s.length(); i++) {
			int ch = (int) s.charAt(i);
			String s4 = Integer.toHexString(ch);
			str = str + s4;
		}
		return str;
	}

	// 转化十六进制编码为字符串
	public static String toStringHex(String s) {
		byte[] baKeyword = new byte[s.length() / 2];
		for (int i = 0; i < baKeyword.length; i++) {
			try {
				baKeyword[i] = (byte) (0xff & Integer.parseInt(
						s.substring(i * 2, i * 2 + 2), 16));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		try {
			s = new String(baKeyword, "utf-8");// UTF-16le:Not
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		return s;
	}

	/*
	 * 16进制数字字符集
	 */
	private static String hexString = "0123456789ABCDEF";

	/*
	 * 将字符串编码成16进制数字,适用于所有字符（包括中文）
	 */
	public static String encode(String str) {
		// 根据默认编码获取字节数组
		byte[] bytes = str.getBytes();
		StringBuilder sb = new StringBuilder(bytes.length * 2);
		// 将字节数组中每个字节拆解成2位16进制整数
		for (int i = 0; i < bytes.length; i++) {
			sb.append(hexString.charAt((bytes[i] & 0xf0) >> 4));
			sb.append(hexString.charAt((bytes[i] & 0x0f) >> 0));
		}
		return sb.toString();
	}

	/*
	 * 将16进制数字解码成字符串,适用于所有字符（包括中文）
	 */
	public static String decode(String bytes) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(
				bytes.length() / 2);
		// 将每2位16进制整数组装成一个字节
		for (int i = 0; i < bytes.length(); i += 2)
			baos.write((hexString.indexOf(bytes.charAt(i)) << 4 | hexString
					.indexOf(bytes.charAt(i + 1))));
		return new String(baos.toByteArray());
	}

}
