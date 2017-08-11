package test.com.ai.paas.ipaas.ses;

import java.net.InetSocketAddress;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class ChineseMatchTest {
	private TransportClient client = null;
	private Settings settings = Settings.settingsBuilder().put("client.transport.ping_timeout", "60s")
			.put("client.transport.sniff", "true").put("client.transport.ignore_cluster_name", "true").build();

	public void testMatch() {
		client = TransportClient.builder().settings(settings).build();
		client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("127.0.0.1", 9300)));
		SearchRequestBuilder searchRequestBuilder = null;
		searchRequestBuilder = client.prepareSearch("user").setSearchType(SearchType.QUERY_THEN_FETCH)
				.setScroll(new TimeValue(60000)).setQuery(createQueryBuilder()).setSize(100).setExplain(true);
		SearchResponse searchResponse = searchRequestBuilder.get();
		System.out.print(searchResponse.getHits().getTotalHits());
	}

	private QueryBuilder createQueryBuilder() {

		return QueryBuilders.matchQuery("name", QueryParser.escape("芦玉!@#$%^&*()';,.~")).operator(MatchQueryBuilder.Operator.AND)
				.minimumShouldMatch("90%").type(MatchQueryBuilder.Type.PHRASE_PREFIX);
	}

	public static void main(String[] args) {
		ChineseMatchTest test = new ChineseMatchTest();
		test.testMatch();
	}

}
