package com.ai.paas.ipaas.ses.dataimport.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;

public class HttpClientUtil {
	private static final Log logger = LogFactory.getLog(HttpClientUtil.class);
    public static String sendPostRequest(String url, String param) throws IOException, URISyntaxException {
    	logger.info("restful request url:"+url);
    	logger.info("restful request param:"+param);
    	CloseableHttpClient httpclient = null;
    	CloseableHttpResponse response = null;
    	StringBuffer buffer = new StringBuffer();
        try {
        	 httpclient = HttpClients.createDefault();
             HttpPost httpPost = new HttpPost(new URL(url).toURI());
             StringEntity dataEntity = new StringEntity(param, ContentType.APPLICATION_JSON);
             httpPost.setEntity(dataEntity);
             response = httpclient.execute(httpPost);
            if (response.getStatusLine().getStatusCode() == 200) {
                HttpEntity entity = response.getEntity();
                BufferedReader reader = new BufferedReader(new InputStreamReader(
                        entity.getContent()));                
                String tempStr;
                while ((tempStr = reader.readLine()) != null)
                    buffer.append(tempStr);
            } else {
                throw new RuntimeException("error code " + response.getStatusLine().getStatusCode()
                        + ":" + response.getStatusLine().getReasonPhrase());
            }
        }catch(Exception e){
        	e.printStackTrace();
        	logger.error(e.getMessage(),e);
        } finally {
        	try {
        		if(response != null ){
        			response.close();    			
        		}
        		if(httpclient != null ){
        			httpclient.close();    			
        		}
        	} catch (IOException e) {
	        	logger.error(e.getMessage(),e);
			}
        }
        return buffer.toString();
    }
}
