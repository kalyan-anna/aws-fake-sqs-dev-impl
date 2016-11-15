package com.example;

import java.io.IOException;
import java.util.Properties;

public abstract class BaseTestClass {

	static {
		try {
			Properties p = new Properties();
			p.load(ClassLoader.getSystemResourceAsStream("aws.test.properties"));
			p.entrySet().forEach(entrySet -> System.setProperty((String)entrySet.getKey(), (String)entrySet.getValue()));
		} catch (IOException e) {
			 throw new RuntimeException(e);
		}
		String basePath = System.getProperty("fileQueueService.basePath");
		FileQueueService.setupBaseDirIfAbsent(basePath);
	}

}
