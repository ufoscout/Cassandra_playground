/*******************************************************************************
 * Copyright 2017 Francesco Cina'
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.example.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

@Configuration
public class CassandraConfig {

	private final String clusterIp = "127.0.0.1";
	private final int port = 9042;

	@Bean
	public Cluster cluster() {
		 return Cluster
		        	.builder()
		        	.addContactPoint(clusterIp)
		        	.withPort(port)
		        	//.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ALL))
		        	.build();
	}

	@Bean
	public Session session(Cluster cluster) {
        return cluster.connect();
	}

}
