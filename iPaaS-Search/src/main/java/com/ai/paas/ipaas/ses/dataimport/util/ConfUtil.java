package com.ai.paas.ipaas.ses.dataimport.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfUtil {
    private static Properties mConfig;

    private static String default_config_1 = "/common/conf.properties";
    
	private static final transient Logger log = LoggerFactory.getLogger(ConfUtil.class);
	
	
	private ConfUtil(){
		
	}

    static {
        mConfig = new Properties();
        InputStream is = null;
        try {
            // we'll need this to get at our properties files in the classpath
            @SuppressWarnings("rawtypes")
			Class config_class = ConfUtil.class;
            // first, lets load our default properties
            is = new FileInputStream(new File(config_class.getResource(default_config_1).toURI())); 
            mConfig.load(is);

        }
        catch (Exception e) {
        	log.error(e.getMessage(),e);
        }finally{
        	if(is!=null){
        		try {
					is.close();
				} catch (IOException e) {
		        	log.error(e.getMessage(),e);
				}
        	}
        }

    }
    /**
     * Retrieve a property value
     * @param key Name of the property
     * @return String Value of property requested, null if not found
     */
    public static String getProperty(String key) {
        return mConfig.getProperty(key);
    }

    /**
     * Retrieve a property value
     * @param key Name of the property
     * @param defaultValue Default value of property if not found
     * @return String Value of property requested or defaultValue
     */
    public static String getProperty(String key, String defaultValue) {
        String value = mConfig.getProperty(key);
        if (value == null)
            return defaultValue;
        return value;
    }
}
