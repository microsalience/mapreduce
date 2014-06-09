package com.mogujie.app.mr.itemsaction;

public class ItemsActionFile {
	public ItemsActionFile(String filePath, String status) {
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
