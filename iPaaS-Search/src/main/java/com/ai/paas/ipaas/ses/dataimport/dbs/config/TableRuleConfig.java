package com.ai.paas.ipaas.ses.dataimport.dbs.config;

import com.ai.paas.ipaas.ccs.constants.ConfigException;
import com.ai.paas.ipaas.ccs.inner.CCSComponentFactory;
import com.ai.paas.ipaas.ccs.inner.ICCSComponent;
import com.ai.paas.ipaas.dbs.distribute.DistributedDBRule;
import com.ai.paas.ipaas.dbs.util.CiperTools;
import com.ai.paas.ipaas.uac.service.UserClientFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.ai.paas.ipaas.uac.vo.AuthResult;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.sql.SQLException;
import java.util.*;

/**
 * Table rule
 * <p/>
 * Created by gaoht on 15/6/5.
 */
public class TableRuleConfig {

    private final String userName;
    private final String password;
    private final String serviceId;
    private final String authAddress;

    private final String logicDbPath;
    private final String tableRulePath;
    private final String tableMetaPath;

    private ICCSComponent config;

    private Map<String, LogicDb> logicDbMap = new HashMap<String, LogicDb>();

    private Map<String, TableRule> tableRuleMap = new HashMap<String, TableRule>();

    private Set<String> logicTableSet = new HashSet<String>();

    private final Gson gson = new Gson();

    private final Map<String, Integer> tableIndexMap = new HashMap<String, Integer>();

    public TableRuleConfig(String userName, String password, String serviceId, String authAddress) {
        this.userName = userName;
        this.password = password;
        this.serviceId = serviceId;
        this.authAddress = authAddress;
        this.logicDbPath = "/DBS/" + this.serviceId + "/logicdb";
        this.tableRulePath = "/DBS/" + this.serviceId + "/tableRules";
        this.tableMetaPath = "/DBS/" + this.serviceId + "/meta/table";
    }


    public void init(String tableName, String tableConfig) throws ConfigException {
        if (tableName == null) {
            return;
        }
        tableName = tableName.toLowerCase();


        if (tableConfig != null && tableConfig.length() > 0) {
            if (!tableConfig.contains("(") || !tableConfig.contains(")")) {
                return;
            }
            String[] columns = tableConfig.substring(tableConfig.indexOf("(") + 1, tableConfig.indexOf(")")).split(",");
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                tableIndexMap.put(column.toLowerCase(), i);
            }
        }

        if (config == null) {
            initConfig();
        }


        String path = this.tableMetaPath + "/" + tableName;
        if (!this.config.exists(path)) {
            return;
        }
        //load table rule
        String tableRulePath = this.tableRulePath + "/" + tableName;
        if (this.config.exists(tableRulePath)) {
            TableRule tableRule = new TableRule();
            JsonObject json = gson.fromJson(this.config.get(tableRulePath), JsonObject.class);
            int subTableCount = json.get("subTableCount").getAsInt();
            if (subTableCount > 1) {
                tableRule.dbRule = json.get("logicDBRule").getAsJsonObject().toString();
                tableRule.rule = json.get("tableNameRule").getAsJsonObject().toString();
                tableRule.logicDBColumnIndex = json.get("logicDBRule").getAsJsonObject()
                        .get("keyValue").getAsJsonObject().get("keyColumn").getAsString();
                tableRule.tableColumnIndex = json.get("tableNameRule").getAsJsonObject().get("keyColumn").getAsString();
                tableRule.isPartition = true;
            } else {
                tableRule.isPartition = false;
            }


            tableRule.tableName = tableName;
            tableRule.tableConfig = tableConfig;
            tableRule.logicDBPattern = json.get("logicDBPattern").getAsString();
            tableRule.tableNamePattern = json.get("tableNamePattern").getAsString();
            tableRuleMap.put(tableName, tableRule);
        }

        JsonObject tableMeta = gson.fromJson(this.config.get(path), JsonObject.class);
        for (Map.Entry<String, JsonElement> tableMetaEntry : tableMeta.entrySet()) {
            //load logic db
            String logicDbPath = this.logicDbPath + "/" + tableMetaEntry.getKey();
            if (this.config.exists(logicDbPath)) {
                LogicDb logicDb = this.initLogicDb(tableMetaEntry.getKey());
                if (logicDb != null) {
                    logicDbMap.put(tableMetaEntry.getKey(), logicDb);
                }

            }
        }

    }

    /**
     * init logicDb
     *
     * @param dbName
     * @return
     * @throws ConfigException
     */
    private LogicDb initLogicDb(String dbName) throws ConfigException {
        String logicDbPath = this.logicDbPath + "/" + dbName.toLowerCase();
        if (this.config.exists(logicDbPath)) {
            LogicDb logicDb = new LogicDb();
            JsonObject json = gson.fromJson(this.config.get(logicDbPath), JsonObject.class);
            logicDb.name = json.get("logicDB").getAsString();
            logicDb.masterUrl = getDbStr(json, "master").get(0);
            logicDb.slaveUrl = getDbStr(json, "slaves");

            return logicDb;
        }
        return null;
    }

    /**
     * init user's tables
     */
    public void init(List<String> tableNameList) throws ConfigException {
    	if(userName==null||userName.length()==0)
    		return;
    	
        if (config == null) {
            initConfig();
        }
        if (tableNameList == null || tableNameList.isEmpty()) {
            String path = this.tableMetaPath;
            if (this.config.exists(path)) {
                tableNameList = this.config.listSubPath(path);
            }
        }

        if (tableNameList == null || tableNameList.isEmpty()) {
            return;
        }

        for (String tableName : tableNameList) {
            String path = this.tableMetaPath + "/" + tableName.toLowerCase();
            if (this.config.exists(path)) {
                logicTableSet.add(tableName);
                JsonObject realTableConfig = gson.fromJson(this.config.get(path), JsonObject.class);
                for (Map.Entry<String, JsonElement> configEntry : realTableConfig.entrySet()) {

                    LogicDb logicDb;
                    if (this.logicDbMap.containsKey(configEntry.getKey())) {
                        logicDb = this.logicDbMap.get(configEntry.getKey());
                    } else {
                        logicDb = this.initLogicDb(configEntry.getKey());
                        this.logicDbMap.put(configEntry.getKey(), logicDb);
                    }
                    if (logicDb != null) {
                        String realTableStr = configEntry.getValue().getAsString();
                        if (realTableStr.contains(",")) {
                            for (String realTableName : realTableStr.split(",")) {
                                TablePair tp = new TablePair();
                                tp.logicTableName = tableName;
                                tp.realTableName = realTableName;
                                logicDb.tables.add(tp);
                            }
                        } else {
                            TablePair tp = new TablePair();
                            tp.logicTableName = tableName;
                            tp.realTableName = configEntry.getValue().getAsString();
                            logicDb.tables.add(tp);
                        }
                    }
                }
            }
        }

    }


    public final String getDb(String tableName, Object[] value) throws SQLException {

        TableRule rule = tableRuleMap.get(tableName.toLowerCase());
        if (rule.isPartition) {
            Integer columnIndex = this.tableIndexMap.get(rule.logicDBColumnIndex);
            JsonObject obj = new JsonObject();
            Integer id = DistributedDBRule.calculateDistributeId(obj.getAsJsonObject(rule.dbRule), value[columnIndex], null);
            return DistributedDBRule.calculatePattern(id, rule.logicDBPattern);
        } else {
            return rule.logicDBPattern;
        }

    }

    public final String getTable(String tableName, Object[] value) throws SQLException {
        TableRule rule = tableRuleMap.get(tableName.toLowerCase());
        if (rule.isPartition) {
            Integer columnIndex = this.tableIndexMap.get(rule.tableColumnIndex);
            JsonObject obj = new JsonObject();
            Integer id = DistributedDBRule.calculateDistributeId(obj.getAsJsonObject(rule.rule), value[columnIndex], new HashMap<String, Map<Object, Integer>>());
            return DistributedDBRule.calculatePattern(id, rule.tableNamePattern);
        } else {
            return rule.tableNamePattern;
        }

    }

    public final String getTableConfig(String tableName) throws SQLException {
        return tableRuleMap.get(tableName).tableConfig;
    }

    public final Set<String> getLogicTableSet() {
        return this.logicTableSet;
    }


    public final Map<String, LogicDb> getLogicDbMap() {
        return this.logicDbMap;
    }


    private List<String> getDbStr(JsonObject json, String dbName) {
        JsonElement e = json.get(dbName);
        List<String> realDbName = new ArrayList<String>();
        List<String> ret = new ArrayList<String>();
        if (e.isJsonArray()) {
            for (int i = 0; i < e.getAsJsonArray().size(); i++) {
                JsonObject slave = e.getAsJsonArray().get(i).getAsJsonObject();
                realDbName.add(slave.get("slave").getAsString());
            }
        } else {
            realDbName.add(e.getAsString());
        }
        for (String names : realDbName) {
            JsonObject realDb = json.get(names).getAsJsonObject();
            ret.add(realDb.get("url").getAsString() + "&user=" + realDb.get("username").getAsString() + "&password=" +
                    CiperTools.decrypt(realDb.get("password").getAsString()));
        }
        return ret;

    }

    public static class LogicDb {
        public String name;

        public String masterUrl;

        public List<String> slaveUrl;
        public List<TablePair> tables = new ArrayList<TablePair>();

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof LogicDb)) return false;

            LogicDb logicDb = (LogicDb) o;

            return name.equals(logicDb.name);

        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    private void initConfig() throws ConfigException {
    	if(userName==null||userName.length()==0)
    		return;
        AuthDescriptor auth = new AuthDescriptor();
        auth.setAuthAdress(authAddress);
        auth.setPassword(password);
        auth.setServiceId(serviceId);
        auth.setUserName(userName);

        AuthResult authResult = UserClientFactory.getUserClient().authUser(
                auth);
        this.config = CCSComponentFactory.getConfigClient(
                authResult.getConfigAddr(), authResult.getConfigUser(),
                authResult.getConfigPasswd());
    }

    public static class TablePair {

        public String logicTableName;

        public String realTableName;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TablePair)) return false;

            TablePair tablePair = (TablePair) o;

            return !(logicTableName != null ? !logicTableName.equals(tablePair.logicTableName) : tablePair.logicTableName != null);

        }

        @Override
        public int hashCode() {
            return logicTableName != null ? logicTableName.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "{" +
                    "logicTableName='" + logicTableName + '\'' +
                    ", realTableName='" + realTableName + '\'' +
                    '}';
        }
    }


    public static class TableRule {

        public String tableName;

        public String dbRule;

        public String rule;

        public String logicDBPattern;
        public String tableNamePattern;
        public boolean isPartition;
        public String logicDBColumnIndex;
        public String tableColumnIndex;
        public String tableConfig;
    }
}
