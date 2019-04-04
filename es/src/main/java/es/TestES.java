package es;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

public class TestES {

    private volatile static TransportClient client ;
    public static TransportClient getClient() throws UnknownHostException {
        if(null == client){
            synchronized (TransportClient.class){
                //指定集群名称
                //Settings settings = Settings.builder().put("cluster.name","elasticsearch").build();
                client = new PreBuiltTransportClient(Settings.EMPTY)
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
            }
        }
        return client;
    }

    public static void main(String[] args)  {

        GetResponse response = client.prepareGet("hik", "mac", "1").get();
        System.out.println(response.getSourceAsString());

//==============索引管理>>>>>>>>>>>>>>>>>
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        IndicesExistsResponse exResponse = indicesAdminClient.prepareExists("indexName").get();
        System.out.println(exResponse.isExists());

        TypesExistsResponse existsResponse = indicesAdminClient.prepareTypesExists("indexName")
                .setTypes("type1","type2").get();
        System.out.println(existsResponse.isExists());

        CreateIndexResponse cResponse = indicesAdminClient.prepareCreate("indexName").get();
        System.out.println(cResponse.isAcknowledged());

        CreateIndexResponse cResponse1 = indicesAdminClient.prepareCreate("indexName")
                .setSettings( Settings.builder().put("index.num_or_shards",3) ).get();

        UpdateSettingsResponse upResponse = indicesAdminClient.prepareUpdateSettings("indexName")
                .setSettings( Settings.builder().put("index.num_or_shards",3) ).get();

        GetSettingsResponse getResponse = indicesAdminClient.prepareGetSettings("indexName","typeName").get();
        for(ObjectObjectCursor<String, Settings> cursor : getResponse.getIndexToSettings()){
            String index = cursor.key;
            Settings settings = cursor.value;
            Integer shards = settings.getAsInt("index.num_or_shards",null);
        }

        DeleteIndexResponse deleteIndexResponse = indicesAdminClient.prepareDelete("indexName").get();
        System.out.println(deleteIndexResponse.isAcknowledged());

        indicesAdminClient.prepareRefresh("indexName").get();
        CloseIndexResponse closeIndexResponse = indicesAdminClient.prepareClose("indexNama").get();

//>>>>>>>>>>>>>索引管理=============================

//===========文档管理>>>>>>>>>>>>>>
        //新建文档
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("macAddress","111111111"+(int)(Math.random()*100));
        json.put("postDate",new Date());
        json.put("devNo","dev00"+(int)Math.random());
        IndexResponse response1 = client.prepareIndex("hik", "mac").setSource(json).get();
        System.out.println(response1.status());
        // 获取文档
        GetResponse response2 = client.prepareGet("hik","mac","1").get();
        String content = response2.getSourceAsString();
        //删除文档
        DeleteResponse response3 = client.prepareDelete("hik","mac","1").get();
        System.out.println(response3.status());
        //批量获取
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("hik","mac","1").add("hik","mac","1").get();
        for(MultiGetItemResponse itemResponses : multiGetItemResponses){
            GetResponse response4 = itemResponses.getResponse();
            System.out.println(response4.getSourceAsString());
        }

        //搜索
        QueryBuilder matchQuery =matchQuery("title","java")
                .operator(Operator.AND);
        HighlightBuilder highlighter = new HighlightBuilder().field("title");
        SearchResponse response4 = client.prepareSearch("books")
                .setQuery(matchQuery)
                .highlighter(highlighter)
                .setSize(100)
                .get();
        SearchHits hits = response4.getHits();
        System.out.println("搜索到： "+hits.getTotalHits()+" 条数据.");
        for(SearchHit hit : hits){
            System.out.println("source: "+hit.getSourceAsString());
            System.out.println("source as map : "+ hit.getSource());
            System.out.println("price: "+hit.getSource().get("price"));
        }
        //全文查询
        QueryBuilder matchallqurey = QueryBuilders.matchAllQuery();
        QueryBuilder queryPhraseQuery = QueryBuilders.matchPhraseQuery("foo","hello world");
        QueryBuilder queryPhrasePrefixQuery = QueryBuilders.matchPhrasePrefixQuery("foo","hello world");
        QueryBuilder queryMatchQuery = QueryBuilders.multiMatchQuery("ky","user","msg");
        QueryBuilder commonTermsQuery = QueryBuilders.commonTermsQuery("name","ky");
        QueryBuilder queryStringQuery = QueryBuilders.queryStringQuery("+ky -es");
        QueryBuilder qb = QueryBuilders.simpleQueryStringQuery("+ky -es");

        //词像查询
        QueryBuilder termQuery = QueryBuilders.termQuery("title","java");
        QueryBuilder termsQuery = QueryBuilders.termsQuery("title","java","go");
        QueryBuilder rangeQuery = QueryBuilders.rangeQuery("price").from(50).to(70);
        QueryBuilder existsQuery = QueryBuilders.existsQuery("title");
        QueryBuilder prefixQuery = QueryBuilders.prefixQuery("describe","win");
        QueryBuilder wildcardQuery = QueryBuilders.wildcardQuery("describe","win?");
        QueryBuilder regexpQuery = QueryBuilders.regexpQuery("describe","win.*++");
        QueryBuilder fuzzyQuery = QueryBuilders.fuzzyQuery("describe","win");
        QueryBuilder typeQuery = QueryBuilders.typeQuery("IT");
        QueryBuilder idsQuery = QueryBuilders.idsQuery();

        //复合查询
        QueryBuilder constantScoreQuery = QueryBuilders.constantScoreQuery(
                QueryBuilders.termQuery("title","java")
        ).boost(2.0f);
        QueryBuilder disMaxQuery = QueryBuilders.disMaxQuery()
                .add(QueryBuilders.termQuery("tilte","java"))
                .add(QueryBuilders.termQuery("tilte","go"))
                .boost(2.0f).tieBreaker(0.5f);
        QueryBuilder mathcQuery1 = matchQuery("title","java");
        QueryBuilder mathcQuery2 = matchQuery("title","go");
        QueryBuilder rangeQuery1 = QueryBuilders.rangeQuery("price").gt(70);
        QueryBuilder boolQuery = boolQuery()
                .must(mathcQuery1).should(mathcQuery2).mustNot(rangeQuery1);
        QueryBuilder indicesQuery = QueryBuilders.indicesQuery(mathcQuery1,"book1","book2").noMatchQuery(mathcQuery2);

        //嵌套查询
        QueryBuilder qb1 = QueryBuilders.nestedQuery("obj1",boolQuery().must(matchQuery("ob1","blue")),ScoreMode.Avg);

        //聚合分析
        //查询最大值
        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("agg").field("price");
        SearchResponse response5 = client.prepareSearch("books").addAggregation(maxAggregationBuilder).get();
        Max agg = response5.getAggregations().get("agg");
        double value = agg.getValue();

        //最小值
        MinAggregationBuilder minAggregationBuilder = AggregationBuilders.min("agg").field("price");
        SearchResponse response6 = client.prepareSearch("books").addAggregation(maxAggregationBuilder).execute().actionGet();
        Min agg1 = response6.getAggregations().get("agg");
        double min = agg1.getValue();

        // sum ； avg ； value count ; cardinality ; stats ;  类似

        //桶聚合
        TermsAggregationBuilder termAgg = AggregationBuilders.terms("per_count").field("language");
        SearchResponse response7 = client.prepareSearch("books").addAggregation(termAgg).execute().actionGet();
        Terms genders = response7.getAggregations().get("per_count");
        for(Terms.Bucket entry : genders.getBuckets()){
            System.out.println(entry.getKey()+""+entry.getDocCount());
        }

        FilterAggregationBuilder filterAgg = AggregationBuilders.filter("agg",QueryBuilders.termQuery("title","java"));
        SearchResponse response8 = client.prepareSearch("books").addAggregation(filterAgg).execute().actionGet();
        Filter filter = response8.getAggregations().get("agg");

        AggregationBuilder filtersAgg = AggregationBuilders.filters("agg",
                new FiltersAggregator.KeyedFilter("java",QueryBuilders.termQuery("title","java")),
                new FiltersAggregator.KeyedFilter("go",QueryBuilders.termQuery("title","go")));
        SearchResponse response9 = client.prepareSearch("books").addAggregation(filtersAgg).execute().actionGet();
        Filters filter1 = response9.getAggregations().get("agg");
        for(Filters.Bucket entry : filter1.getBuckets()){
            System.out.println(entry.getKey()+""+entry.getDocCount());
        }

        AggregationBuilder rangeAgg = AggregationBuilders.range("agg").field("price").addUnboundedTo(50).addRange(50,80).addUnboundedFrom(80);
        SearchResponse response10 = client.prepareSearch("books").addAggregation(rangeAgg).execute().actionGet();
        Range range = response10.getAggregations().get("agg");
        for(Range.Bucket entry : range.getBuckets()){
            System.out.println(entry.getKey()+""+entry.getDocCount());
        }

        AggregationBuilder dateAgg = AggregationBuilders.dateRange("agg").field("time").format("yyyy-mm-dd").addUnboundedTo("").addUnboundedFrom("");
        SearchResponse response11 = client.prepareSearch("books").addAggregation(dateAgg).execute().actionGet();
        Range range1 = response11.getAggregations().get("agg");

        MissingAggregationBuilder missingAgg = AggregationBuilders.missing("agg").field("price");
        SearchResponse response12 = client.prepareSearch("books").addAggregation(missingAgg).execute().actionGet();
        Missing miss = response12.getAggregations().get("agg");

        //集群管理
        ClusterHealthResponse healths = client.admin().cluster().prepareHealth().get();
        String clusterName = healths.getClusterName();
        int numOfDataNodes = healths.getNumberOfDataNodes();
        int numOfNodes = healths.getNumberOfNodes();
        for(ClusterIndexHealth health : healths.getIndices().values()){
            String index = health.getIndex();
            int numOfShards = health.getNumberOfShards();
            int numOfReplicas = health.getNumberOfReplicas();
            ClusterHealthStatus status = health.getStatus();
        }

    }
}
