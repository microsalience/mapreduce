/*
 * ??????�? Inc.
 * Copyright (c) 2010-2013 All Rights Reserved.
 *
 * Author     :yinxiu
 * Version    :1.0
 * Create Date:2013�?10???25???
 */
package com.mogujie.app.mr.cnjie;

/**
 * �?�???��?��?��?????�?
 * 
 * @author yinxiu
 * @version $Id: NginxAccessCNjieLogFile.java,v 0.1 2013�?10???25??? �????2:20:08 yinxiu
 *          Exp $
 */
public class NginxAccessCNjieLogFile {
	public NginxAccessCNjieLogFile(String filePath, String status) {
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
