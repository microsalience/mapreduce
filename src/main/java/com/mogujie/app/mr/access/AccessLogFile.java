/*
 * ??????è¡? Inc.
 * Copyright (c) 2010-2013 All Rights Reserved.
 *
 * Author     :yinxiu
 * Version    :1.0
 * Create Date:2013å¹?10???17???
 */
package com.mogujie.app.mr.access;

/**
 * è¡¨ç¤º?????¹æ?¥å?????ä»?
 * 
 * @author yinxiu
 * @version $Id: AccessLogFile.java,v 0.1 2013å¹?10???17??? ä¸????10:44:15 yinxiu Exp $
 */
public class AccessLogFile {

	public AccessLogFile(String filePath, String status) {
		this.filePath = filePath;
		this.status = status;
	}

	private String filePath;
	private String status;

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

}
