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
package com.example.demo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;

public class CassandraTest extends DemoApplicationTests {

	@Autowired
	private Session session;
	private final String keyspace = "myKeySpace";

	@Test
	public void test() {

	    createKeyspace("SimpleStrategy", 1);
	    verifyKeyspaceExists();

	    createBookColumnFamily();
	    verifyColumnFamilyIsCreated();

	    final String title = "Effective Java";
	    final Book book = new Book(UUIDs.timeBased(), title, "Programming");
	    insertBook(book);
	    final Book foundBook = findBookById(book.getId());
	    assertBookEquals(book, foundBook);
	}

	private void createKeyspace(String replicationStrategy, int replicationFactor) {
		final StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
		      .append(keyspace).append(" WITH replication = {")
		      .append("'class':'").append(replicationStrategy)
		      .append("','replication_factor':").append(replicationFactor)
		      .append("};");
		session.execute( sb.toString() );
	}

	private void verifyKeyspaceExists() {
	    final ResultSet result = session.execute("SELECT * FROM system_schema.keyspaces;");

	    final List<String> matchedKeyspaces = result.all()
	      .stream()
	      .filter(r -> r.getString(0).equals(keyspace.toLowerCase()))
	      .map(r -> r.getString(0))
	      .collect(Collectors.toList());

	    assertEquals(matchedKeyspaces.size(), 1);
	    assertTrue(matchedKeyspaces.get(0).equals(keyspace.toLowerCase()));
	}

	private void createBookColumnFamily() {
	    final StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
	    		.append(keyspace)
	    		.append(".Books").append("(")
	    		.append("id uuid PRIMARY KEY, ")
	    		.append("title text,")
	    		.append("subject text);");

	    final String query = sb.toString();
	    session.execute(query);
	}

	private void verifyColumnFamilyIsCreated() {
	    final ResultSet result = session.execute("SELECT * FROM " + keyspace + ".books;");

	    final List<String> columnNames =
	      result.getColumnDefinitions().asList().stream()
	      .map(cl -> cl.getName())
	      .collect(Collectors.toList());

	    assertEquals(columnNames.size(), 3);
	    assertTrue(columnNames.contains("id"));
	    assertTrue(columnNames.contains("title"));
	    assertTrue(columnNames.contains("subject"));
	}

	private void insertBook(Book book) {
	    final StringBuilder sb = new StringBuilder("INSERT INTO ")
	    		.append(keyspace).append(".Books")
	    		.append("(id, title, subject) ")
	    		.append("VALUES (")
	    			.append(book.getId()).append(", '")
	    			.append(book.getTitle()).append("', '")
	    			.append(book.getSubject()).append("');");

	    final String query = sb.toString();
	    session.execute(query);
	}

	private Book findBookById(UUID id) {
	    final StringBuilder sb = new StringBuilder("Select * from ")
	    		.append(keyspace).append(".Books ")
	    		.append("WHERE ID = ").append(id).append(";");

	    final String query = sb.toString();
	    final ResultSet rs = session.execute(query);
	    final Row one = rs.one();
	    return new Book(
	    		one.get("ID", UUID.class),
	    		one.get("title", String.class),
	    		one.get("subject", String.class)
	    		);
	}

	private void assertBookEquals(Book one, Book two) {
		assertEquals(one.getId(), two.getId());
		assertEquals(one.getTitle(), two.getTitle());
		assertEquals(one.getSubject(), two.getSubject());
	}
}
