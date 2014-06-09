/*
 * ??????è¡? Inc.
 * Copyright (c) 2010-2014 All Rights Reserved.
 *
 * Author     :xingtian
 * Version    :1.0
 * Create Date:2014å¹?01???20???
 */
package com.mogujie.app.mr;

/**
 * ?????¨æ?¥å?????ä»?
 * 
 * @author xingtian
 * @version $Id: GeneralLogFile.java,v 0.1 2014å¹?01???20??? ä¸????2:20:08 xingtian Exp $
 */
public class GeneralLogFile {
	public GeneralLogFile(String filePath, String status) {
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
