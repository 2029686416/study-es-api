package com.kuang;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.ml.EvaluateDataFrameRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
class StudyEsApiApplicationTests {

	//面向对象操作

	@Autowired
	private RestHighLevelClient restHighLevelClient;

	//测试索引的创建	Request
	@Test
	void testCreateIndex() throws IOException {
		//1、创建索引请求
		CreateIndexRequest requeset = new CreateIndexRequest("kuang_index");
		//2、执行创建请求	IndicesClient
		CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(requeset, RequestOptions.DEFAULT);
		System.out.println(createIndexResponse);

	}

}
