/*
 * ??????è¡? Inc.
 * Copyright (c) 2010-2013 All Rights Reserved.
 *
 * Author     :yinxiu
 * Version    :1.0
 * Create Date:2013å¹?10???24???
 */
package com.mogujie.app.mr.tradetrace;

/**
 * @author yinxiu
 * @version $Id: TradeClickTraceLogFile.java,v 0.1 2013å¹?10???24??? ä¸????3:41:07 yinxiu
 *          Exp $
 */
public class TradeClickTraceLogFile {
	public TradeClickTraceLogFile(String filePath, String status) {
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
