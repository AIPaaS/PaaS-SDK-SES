package com.ai.paas.ipaas.ses.dataimport.dbs.config;

import java.lang.reflect.Method;

/**
 * dump config
 * <p/>
 * Created by gaoht on 15/6/5.
 */
public class FormatConfig {
    public FormatConfig() {

    }

//    @com.ai.paas.ipaas.ses.dataimport.dbs.NotNull
    private String userName;

//    @com.ai.paas.ipaas.ses.dataimport.dbs.NotNull
    private String password;

//    @com.ai.paas.ipaas.ses.dataimport.dbs.NotNull
    private String serviceId;

//    @com.ai.paas.ipaas.ses.dataimport.dbs.NotNull
    private String authAddress;

    private String fieldsTerminatedBy;

    private String fieldsEnclosedBy;

    private String fieldsEscapedBy;

    private String linesStartingBy;

    private String linesTerminatedBy;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getServiceId() {
        return serviceId;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public String getAuthAddress() {
        return authAddress;
    }

    public void setAuthAddress(String authAddress) {
        this.authAddress = authAddress;
    }

    public String getFieldsTerminatedBy() {
        return fieldsTerminatedBy == null ? "\t" : fieldsTerminatedBy;
    }

    public void setFieldsTerminatedBy(String fieldsTerminatedBy) {
        this.fieldsTerminatedBy = fieldsTerminatedBy;
    }

    public String getFieldsEnclosedBy() {
        return fieldsEnclosedBy == null ? "" : fieldsEnclosedBy;
    }

    public void setFieldsEnclosedBy(String fieldsEnclosedBy) {
        this.fieldsEnclosedBy = fieldsEnclosedBy;
    }

    public String getFieldsEscapedBy() {
        return fieldsEscapedBy == null ? "\\" : fieldsEscapedBy;
    }

    public void setFieldsEscapedBy(String fieldsEscapedBy) {
        this.fieldsEscapedBy = fieldsEscapedBy;
    }

    public String getLinesStartingBy() {
        return linesStartingBy == null ? "" : linesStartingBy;
    }

    public void setLinesStartingBy(String linesStartingBy) {
        this.linesStartingBy = linesStartingBy;
    }

    public String getLinesTerminatedBy() {
        return linesTerminatedBy == null ? "\n" : linesTerminatedBy;
    }

    public void setLinesTerminatedBy(String linesTerminatedBy) {
        this.linesTerminatedBy = linesTerminatedBy;
    }

    public String getLoadDataConfig() {
        StringBuilder config = new StringBuilder();
        if (fieldsTerminatedBy != null && fieldsTerminatedBy.length() > 0) {
            config.append(" TERMINATED BY '").append(fieldsTerminatedBy.trim()).append("' ");
        }
        if (fieldsEnclosedBy != null && fieldsEnclosedBy.length() > 0) {
            config.append(" ENCLOSED BY '").append(fieldsEnclosedBy.trim()).append("' ");
        }
        if (fieldsEscapedBy != null && fieldsEscapedBy.length() > 0) {
            config.append(" ESCAPED BY '").append(fieldsEscapedBy.trim()).append("' ");
        }

        if (config.length() > 0) {
            config.insert(0, " FIELDS ");
        }

        String fields = config.toString();
        config.setLength(0);
        if (linesStartingBy != null && linesStartingBy.length() > 0) {
            config.append(" STARTING BY '").append(linesStartingBy.trim()).append("' ");
        }
        if (linesTerminatedBy != null && linesTerminatedBy.length() > 0) {
            config.append(" TERMINATED BY '").append(linesTerminatedBy.trim()).append("' ");
        }

        if (config.length() > 0) {
            config.insert(0, " LINES ");
        }

        return fields + config.toString();
    }

    public static final <T extends FormatConfig> T createConfig(String[] args, Class<T> type) {
        T config;
        try {
            config = type.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("internal error,can not create config object",e);
        }
        for (String arg : args) {
            if (!arg.contains("=")) {
                throw new RuntimeException("invalid param:" + arg);
            }
            String[] paramPair = arg.split("=");
            String paramName = paramPair[0];
            String setMethodName = "set" + paramName.substring(0, 1).toUpperCase() + paramName.substring(1, paramName.length());
            Method setMethod;
            try {
                setMethod = type.getMethod(setMethodName, String.class);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("invalid paramName:" + paramName,e);
            }

            setMethod.setAccessible(true);
            try {
                setMethod.invoke(config, paramPair[1]);
            } catch (Exception e) {
                throw new RuntimeException("internal error,can not set config " + arg,e);
            }
        }

        return config;
    }


    public static String pathToFileName(String path) {
        if (path.contains("/")) {
            path = path.substring(path.lastIndexOf("/"), path.length());
        }

        if (path.contains(".")) {
            path = path.split("\\.")[0];
        }
        return path;
    }
}
