package com.ai.paas.ipaas.ses.dataimport.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.ses.mapping.controller.SesController;

public class HttpClientUtil {
	private static final transient Logger LOGGER = LoggerFactory.getLogger(HttpClientUtil.class);

	public static String sendPostRequest(String url, String data)
			throws IOException, URISyntaxException {
		CloseableHttpClient httpclient = HttpClients.createDefault();
		HttpPost httpPost = new HttpPost(new URL(url).toURI());
		StringEntity dataEntity = new StringEntity(data,
				ContentType.APPLICATION_JSON);
		httpPost.setEntity(dataEntity);
		CloseableHttpResponse response = httpclient.execute(httpPost);
		try {
			if (response.getStatusLine().getStatusCode() == 200) {
				HttpEntity entity = response.getEntity();
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(entity.getContent()));
				StringBuffer buffer = new StringBuffer();
				String tempStr;
				while ((tempStr = reader.readLine()) != null)
					buffer.append(tempStr);
				return buffer.toString();
			} else {
				throw new RuntimeException("error code "
						+ response.getStatusLine().getStatusCode() + ":"
						+ response.getStatusLine().getReasonPhrase());
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			response.close();
			httpclient.close();
		}
	}

	public static String send(String address, String param) {
		LOGGER.info("restful address : " + address);
		LOGGER.info("param : " + param);
		String result = "";
		try {
			result = HttpClientUtil.sendPostRequest(address, param);
			LOGGER.info("result : " + result);
		} catch (IOException e) {
			String errorMessage = e.getMessage();
			LOGGER.error(errorMessage, e);
		} catch (URISyntaxException e) {
			String errorMessage = e.getMessage();
			LOGGER.error(errorMessage, e);
		}
		// 请求发生异常后，result 为 空
		return result;
	}

	/**
	 * 发送GET请求
	 * 
	 * @param url
	 *            目的地址
	 * @param parameters
	 *            请求参数，Map类型。
	 * @return 远程响应结果
	 * @throws Exception
	 */
	public static String sendPost(String url, Map<String, String> parameters)
			throws Exception {
		StringBuffer buffer = new StringBuffer();

		CloseableHttpResponse response = null;
		CloseableHttpClient httpclient = null;

		try {
			httpclient = HttpClients.createDefault();
			HttpPost httpPost = new HttpPost(new URL(url).toURI());
			List<BasicNameValuePair> nameValuePairs = new ArrayList<BasicNameValuePair>();
			for (String name : parameters.keySet()) {
				nameValuePairs.add(new BasicNameValuePair(name, parameters
						.get(name)));
			}
			response = httpclient.execute(httpPost);

			if (response.getStatusLine().getStatusCode() == 200) {
				HttpEntity entity = response.getEntity();
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(entity.getContent()));
				String tempStr;
				while ((tempStr = reader.readLine()) != null)
					buffer.append(tempStr);
				return buffer.toString();
			} else {
				String errorStr = "error code "
						+ response.getStatusLine().getStatusCode() + ":"
						+ response.getStatusLine().getReasonPhrase();
				LOGGER.info(errorStr);
				throw new RuntimeException(errorStr);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (response != null)
				response.close();
			if (httpclient != null)
				httpclient.close();
		}
	}

	/**
	 * 发送GET请求
	 * 
	 * @param url
	 *            目的地址
	 * @param parameters
	 *            请求参数，Map类型。
	 * @return 远程响应结果
	 */
	public static String sendGet(String url, Map<String, String> parameters) {
		StringBuffer buffer = new StringBuffer();// 返回的结果
		BufferedReader in = null;// 读取响应输入流
		StringBuffer sb = new StringBuffer();// 存储参数
		String params = "";// 编码之后的参数
		try {
			// 编码请求参数
			if (parameters.size() == 1) {
				for (String name : parameters.keySet()) {
					sb.append(name)
							.append("=")
							.append(java.net.URLEncoder.encode(
									parameters.get(name), "UTF-8"));
				}
				params = sb.toString();
			} else {
				for (String name : parameters.keySet()) {
					sb.append(name)
							.append("=")
							.append(java.net.URLEncoder.encode(
									parameters.get(name), "UTF-8")).append("&");
				}
				String temp_params = sb.toString();
				params = temp_params.substring(0, temp_params.length() - 1);
			}
			String full_url = url + "?" + params;
			LOGGER.info("restful address : " + full_url);
			// 创建URL对象
			java.net.URL connURL = new java.net.URL(full_url);
			// 打开URL连接
			java.net.HttpURLConnection httpConn = (java.net.HttpURLConnection) connURL
					.openConnection();
			// 建立实际的连接
			httpConn.connect();
			// 定义BufferedReader输入流来读取URL的响应,并设置编码方式
			in = new BufferedReader(new InputStreamReader(
					httpConn.getInputStream(), "UTF-8"));
			String line;
			// 读取返回的内容
			while ((line = in.readLine()) != null) {
				buffer.append(line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return buffer.toString();
	}

	public static void main(String[] args) {
		Map<String, String> map = new HashMap<String, String>();
		map.put("userId", "AFCBF872DD4C4C03AAFFBB2221B1AC93");
		map.put("serviceId", "MCS001");
		map.put("newPwd", "111111");
		map.put("oldPwd", "123456");
		String result = HttpClientUtil.sendGet(
				"http://10.1.228.198:14821/iPaas-UAC/service/modifyServPwd",
				map);
		System.out.println("++++++++++++  " + result);
	}
}
