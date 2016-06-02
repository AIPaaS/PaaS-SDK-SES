package test.com.ai.paas.ipaas.ses;

import com.ai.paas.ipaas.search.service.ISearchClient;
import com.ai.paas.ipaas.search.service.SearchClientFactory;
import com.ai.paas.ipaas.search.vo.Results;
import com.ai.paas.ipaas.search.vo.SearchOption;
import com.ai.paas.ipaas.search.vo.SearchOption.DataFilter;
import com.ai.paas.ipaas.search.vo.SearchOption.SearchLogic;
import com.ai.paas.ipaas.search.vo.SearchOption.SearchType;
import com.ai.paas.ipaas.search.vo.SearchVo;
import com.ai.paas.ipaas.search.vo.SearchfieldVo;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SearchTest {

    private static final String AUTH_ADDR = "http://10.1.245.4:19811/service-portal-uac-web/service/auth";
    private static AuthDescriptor ad = null;
    private static ISearchClient is = null;
    private static Gson gson = new Gson();


    static {
        ad = new AuthDescriptor(AUTH_ADDR, "B1F464FC22E745D79EE67B2691112795", "1q2w3e", "SES001");
        try {
            is = SearchClientFactory.getSearchClient(ad);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void insertTest() {

        List<String> list = new ArrayList<String>();
        Map notice = new HashMap();
        notice.put("userid", 3);
        notice.put("username", "马培亮");
        notice.put("mobile", "1860001000");
        notice.put("birthday", "1987-01-08");
        Map role = new HashMap();
        role.put("roleid", 1);
        role.put("rolename", "员工");
        notice.put("role", role);
        notice.put("address", "北京市海淀区西三旗大街");
        notice.put("createtime", "2016-01-07");
        notice.put("company", "亚信科技(中国)有限公司");
        Gson gson = new Gson();
        list.add(gson.toJson(notice));
        is.bulkInsertData(list);
    }


    @Test
    public void searchTest() {
        List<SearchfieldVo> fieldList = new ArrayList<SearchfieldVo>();
        SearchfieldVo searchAbsTractVo = new SearchfieldVo();
        searchAbsTractVo.setFiledName("userid");
        List<String> fvSet = new ArrayList<String>();
        fvSet.add("西二旗大街");
        searchAbsTractVo.setFiledValue(fvSet);
        searchAbsTractVo.setOption(new SearchOption(SearchType.querystring, SearchLogic.should, "1", DataFilter.exists, 1.0f, 1));
        fieldList.add(searchAbsTractVo);
        Results<Map<String, Object>> result = is.searchIndex(fieldList, 0, 10, SearchLogic.should, null, "desc");
        System.out.println(result.getCount() + "+++++++++++++++++++++++++++++");
    }

    @Test
    public void complextTest() {

        List<SearchVo> fieldList = new ArrayList<SearchVo>();
        SearchVo vo1 = new SearchVo();
        List<SearchfieldVo> searchfieldList1 = new ArrayList<SearchfieldVo>();
        SearchfieldVo mobileFiledVo = new SearchfieldVo();
        mobileFiledVo.setFiledName("mobile");
        List<String> fvSet = new ArrayList<String>();
        fvSet.add("18600360249");
        mobileFiledVo.setFiledValue(fvSet);
        mobileFiledVo.setOption(new SearchOption(SearchType.querystring, SearchLogic.should, "100", DataFilter.exists, 1.0f, 1));

        SearchfieldVo rolenameVo = new SearchfieldVo();
        rolenameVo.setFiledName("rolename");
        List<String> fvSeta = new ArrayList<String>();
        fvSeta.add("管理员");
        rolenameVo.setFiledValue(fvSeta);
        rolenameVo.setOption(new SearchOption(SearchType.querystring, SearchLogic.should, "100", DataFilter.exists, 1.0f, 1));
        searchfieldList1.add(mobileFiledVo);
        searchfieldList1.add(rolenameVo);
        vo1.setSearchFieldList(searchfieldList1);
        vo1.setSearchLogic(SearchLogic.must);
        fieldList.add(vo1);


        SearchVo Vo2 = new SearchVo();
        List<SearchfieldVo> searchfieldList2 = new ArrayList<SearchfieldVo>();
        SearchfieldVo createtimeVo = new SearchfieldVo();
        createtimeVo.setFiledName("createtime");
        List<String> fvSet1 = new ArrayList<String>();
        fvSet1.add("2016-01-07");
        createtimeVo.setFiledValue(fvSet1);
        createtimeVo.setOption(new SearchOption(SearchType.querystring, SearchLogic.should, "100", DataFilter.exists, 1.0f, 1));

        SearchfieldVo usernameVo = new SearchfieldVo();
        usernameVo.setFiledName("username");
        List<String> fvSet2 = new ArrayList<String>();
        fvSet2.add("马化腾");
        usernameVo.setFiledValue(fvSet2);
        usernameVo.setOption(new SearchOption(SearchType.querystring, SearchLogic.should, "100", DataFilter.exists, 1.0f, 1));
        searchfieldList2.add(usernameVo);
        searchfieldList2.add(createtimeVo);
        Vo2.setSearchFieldList(searchfieldList2);
        Vo2.setSearchLogic(SearchLogic.must);
        fieldList.add(Vo2);
        Results<Map<String, Object>> result = is.complexSearch(fieldList, 0, 10, SearchLogic.should, null, "desc");
        System.out.println(result.getCount() + "+++++++++++++++++++++++++++++");
        System.out.println(gson.toJson(result));
    }

    @Test
    public void rangIntSearchTest() {
        List<SearchfieldVo> fieldList = new ArrayList<SearchfieldVo>();
        SearchfieldVo searchAbsTractVo = new SearchfieldVo();
        searchAbsTractVo.setFiledName("userid");
        List<String> fvSet = new ArrayList<String>();
        fvSet.add("3");
        fvSet.add("6");
        searchAbsTractVo.setFiledValue(fvSet);
        searchAbsTractVo.setOption(new SearchOption(SearchType.range, SearchLogic.should, "100", DataFilter.exists, 1.0f, 1));
        fieldList.add(searchAbsTractVo);
        Results<Map<String, Object>> result = is.searchIndex(fieldList, 0, 10, SearchLogic.should, null, "desc");
        System.out.println(result.getCount() + "+++++++++++++++++++++++++++++");
        System.out.println(gson.toJson(result));
    }

    @Test
    public void intAscSearchTest() {
        List<SearchfieldVo> fieldList = new ArrayList<SearchfieldVo>();
        SearchfieldVo searchAbsTractVo = new SearchfieldVo();
        searchAbsTractVo.setFiledName("userid");
        List<String> fvSet = new ArrayList<String>();
        fvSet.add("3");
        fvSet.add("6");
        searchAbsTractVo.setFiledValue(fvSet);
        searchAbsTractVo.setOption(new SearchOption(SearchType.range, SearchLogic.should, "100", DataFilter.exists, 1.0f, 1));
        fieldList.add(searchAbsTractVo);
        Results<Map<String, Object>> result = is.searchIndex(fieldList, 0, 10, SearchLogic.should, "userid", "asc");
        System.out.println(result.getCount() + "+++++++++++++++++++++++++++++");
        System.out.println(gson.toJson(result));
    }

    @Test
    public void intDescSearchTest() {
        List<SearchfieldVo> fieldList = new ArrayList<SearchfieldVo>();
        SearchfieldVo searchAbsTractVo = new SearchfieldVo();
        searchAbsTractVo.setFiledName("userid");
        List<String> fvSet = new ArrayList<String>();
        fvSet.add("3");
        fvSet.add("6");
        searchAbsTractVo.setFiledValue(fvSet);
        searchAbsTractVo.setOption(new SearchOption(SearchType.range, SearchLogic.should, "100", DataFilter.exists, 1.0f, 1));
        fieldList.add(searchAbsTractVo);
        Results<Map<String, Object>> result = is.searchIndex(fieldList, 0, 10, SearchLogic.should, "userid", "desc");
        System.out.println(result.getCount() + "+++++++++++++++++++++++++++++");
        System.out.println(gson.toJson(result));
    }

    @Test
    public void dateAscSearchTest() {
        List<SearchfieldVo> fieldList = new ArrayList<SearchfieldVo>();
        SearchfieldVo searchAbsTractVo = new SearchfieldVo();
        searchAbsTractVo.setFiledName("birthday");
        List<String> fvSet = new ArrayList<String>();
        fvSet.add("1988-01-01");
        fvSet.add("1990-12-30");
        searchAbsTractVo.setFiledValue(fvSet);
        searchAbsTractVo.setOption(new SearchOption(SearchType.range, SearchLogic.should, "100", DataFilter.exists, 1.0f, 1));
        fieldList.add(searchAbsTractVo);
        Results<Map<String, Object>> result = is.searchIndex(fieldList, 0, 10, SearchLogic.should, "birthday", "asc");
        System.out.println(result.getCount() + "+++++++++++++++++++++++++++++");
        System.out.println(gson.toJson(result));
    }

    @Test
    public void dateDescSearchTest() {
        List<SearchfieldVo> fieldList = new ArrayList<SearchfieldVo>();
        SearchfieldVo searchAbsTractVo = new SearchfieldVo();
        searchAbsTractVo.setFiledName("birthday");
        List<String> fvSet = new ArrayList<String>();
        fvSet.add("1988-01-01");
        fvSet.add("1990-12-30");
        searchAbsTractVo.setFiledValue(fvSet);
        searchAbsTractVo.setOption(new SearchOption(SearchType.range, SearchLogic.should, "100", DataFilter.exists, 1.0f, 1));
        fieldList.add(searchAbsTractVo);
        Results<Map<String, Object>> result = is.searchIndex(fieldList, 0, 10, SearchLogic.should, "birthday", "desc");
        System.out.println(result.getCount() + "+++++++++++++++++++++++++++++");
        System.out.println(gson.toJson(result));
    }


    @Test
    public void rangMustSearchTest() {
        Gson gson = new Gson();
        List<SearchfieldVo> fieldList = new ArrayList<SearchfieldVo>();
        SearchfieldVo searchAbsTractVo = new SearchfieldVo();
        searchAbsTractVo.setFiledName("ses_sid");
        List<String> fvSet = new ArrayList<String>();
        fvSet.add("疯狂");
        searchAbsTractVo.setFiledValue(fvSet);
        searchAbsTractVo.setOption(new SearchOption(SearchType.querystring, SearchLogic.should, "100", DataFilter.exists, 1.0f, 1));
        fieldList.add(searchAbsTractVo);

        SearchfieldVo searchAbsTractVo1 = new SearchfieldVo();
        searchAbsTractVo1.setFiledName("user_code");
        List<String> fvSet1 = new ArrayList<String>();
        fvSet1.add("电影");
        searchAbsTractVo1.setFiledValue(fvSet1);
        searchAbsTractVo1.setOption(new SearchOption(SearchType.querystring, SearchLogic.should, "100", DataFilter.exists, 1.0f, 1));
        fieldList.add(searchAbsTractVo1);


        SearchfieldVo searchAbsTractVo2 = new SearchfieldVo();
        searchAbsTractVo2.setFiledName("id");
        List<String> fvSet2 = new ArrayList<String>();
        fvSet2.add("153");
        fvSet2.add("158");
        searchAbsTractVo2.setFiledValue(fvSet2);
        searchAbsTractVo2.setOption(new SearchOption(SearchType.range, SearchLogic.should, "100", DataFilter.exists, 1.0f, 1));
        fieldList.add(searchAbsTractVo2);


        Results<Map<String, Object>> result = is.searchIndex(fieldList, 0, 10, SearchLogic.must, null, "desc");
        System.out.println(result.getCount() + "+++++++++++++++++++++++++++++");
        System.out.println(gson.toJson(result));
    }


    @Test
    public void cleanTest() {
        is.cleanData();
    }


    @Test
    public void seTest() {

    }


    @Test
    public void deTest() {
//		List<SearchfieldVo> fieldList = new ArrayList<SearchfieldVo>();
//		
//		SearchfieldVo vo = new SearchfieldVo();
//		vo.setFiledName("cust_name");
//		List<String> set = new ArrayList<String>();
//		vo.setFiledValue(set);
//		SearchOption op = new SearchOption();
//		op.setSearchLogic(SearchOption.SearchLogic.should);
//		op.setSearchType(SearchOption.SearchType.querystring);
//		vo.setOption(op);
//		fieldList.add(vo);
//		System.out.println("start time +++++++++++++++++++++"+ new Date().getTime());
//		is.deleteData(fieldList);
//		System.out.println("end   time +++++++++++++++++++++"+ new Date().getTime());
    }


    @Test
    public void testSearch() {
        List<SearchfieldVo> searchfieldVos = new ArrayList<SearchfieldVo>();
        SearchfieldVo searchfieldVo = new SearchfieldVo("audiencecode", "10",
                new SearchOption(SearchLogic.should, SearchType.term));

        SearchfieldVo searchfieldVo2 = new SearchfieldVo();
        searchfieldVo2.setOption(new SearchOption(SearchLogic.must, SearchType.term));
        searchfieldVo2.setFiledName("audiencecode");
        searchfieldVo2.addFieldValue("11");

        SearchfieldVo searchfieldVo3 = new SearchfieldVo();
        searchfieldVo3.setOption(new SearchOption(SearchLogic.must, SearchType.term));
        searchfieldVo3.setFiledName("categorid");
        searchfieldVo3.addFieldValue("10000010000000");

        searchfieldVo3.addSubSearchFieldVo(searchfieldVo).addSubSearchFieldVo(searchfieldVo2);

        SearchfieldVo searchfieldVo4 = new SearchfieldVo("skuname","福建",new SearchOption(SearchLogic.must, SearchType.term));

        searchfieldVos.add(searchfieldVo3);
        searchfieldVos.add(searchfieldVo4);
        Results<Map<String, Object>> results = is.search(searchfieldVos, 0, 10, null, null);
        Assert.assertEquals(3, results.getCount());
    }

    @Test
    public void testSimpleAggregation(){
        List<SearchfieldVo> searchfieldVos = new ArrayList<SearchfieldVo>();
        SearchfieldVo searchfieldVo = new SearchfieldVo("audiencecode", "10",
                new SearchOption(SearchLogic.should, SearchType.term));

        SearchfieldVo searchfieldVo2 = new SearchfieldVo();
        searchfieldVo2.setOption(new SearchOption(SearchLogic.must, SearchType.term));
        searchfieldVo2.setFiledName("audiencecode");
        searchfieldVo2.addFieldValue("11");

        SearchfieldVo searchfieldVo3 = new SearchfieldVo();
        searchfieldVo3.setOption(new SearchOption(SearchLogic.must, SearchType.term));
        searchfieldVo3.setFiledName("categorid");
        searchfieldVo3.addFieldValue("10000010000000");

        searchfieldVo3.addSubSearchFieldVo(searchfieldVo).addSubSearchFieldVo(searchfieldVo2);

        SearchfieldVo searchfieldVo4 = new SearchfieldVo("skuname","福建",new SearchOption(SearchLogic.must, SearchType.term));

        searchfieldVos.add(searchfieldVo3);
        searchfieldVos.add(searchfieldVo4);
        Results<Map<String, Long>> results = is.simpleAggregation(searchfieldVos, "audiencecode");
        Assert.assertEquals(3, results.getCount());
        Assert.assertEquals(1L, Long.parseLong(String.valueOf(results.getSearchList().get(0).get("12"))));
    }
}
