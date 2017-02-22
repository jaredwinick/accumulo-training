package com.github.jaredwinick;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

	private static final Logger log = LoggerFactory.getLogger(Util.class);
	
	public static void printAllKeyValues(final Connector connector, final String tableName) throws TableNotFoundException {
		
		Scanner scanner = connector.createScanner(tableName, new Authorizations());
		scanner.setRange(new Range());
		for (Entry<Key,Value> entry : scanner) {
		    
		    log.info("Key: [{}] Value: [{}]", entry.getKey().toString(), entry.getValue().toString());
		}
	}
}
