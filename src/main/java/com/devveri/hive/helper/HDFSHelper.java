package com.devveri.hive.helper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class HDFSHelper {

	private Configuration config;
	
	public HDFSHelper() {
		config = new Configuration();
		config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
	}
	
	private FileSystem getFileSystem(String dirName) throws IOException, URISyntaxException {
		return FileSystem.get(new URI(dirName.replaceAll(" ", "%20")), config);
	}

	public List<String> getDeepestDirectories(String dirName) throws IOException, URISyntaxException {
		List<String> fileList = new ArrayList<>();
		getDeepestDirectories(dirName, fileList);

		// remove path from folders
		List<String> folders = new ArrayList<>(fileList.size());
		for (String path : fileList) {
			if (dirName.endsWith("/")) {
				folders.add(path.replaceAll(dirName, ""));
			} else {
				folders.add(path.replaceAll(dirName + "/", ""));
			}
		}
		return folders;
	}

	private int getDeepestDirectories(String dirName, List<String> fileList) throws IOException, URISyntaxException {
		FileSystem fs = getFileSystem(dirName);
		FileStatus[] fileStatusArray = fs.listStatus(new Path(dirName));
		int directoryCount = 0;
		if (fileStatusArray != null) {
			for (FileStatus fileStatus : fileStatusArray) {
				String path = fileStatus.getPath().toString();
				if (fileStatus.isDirectory()) {
					++directoryCount;
					getDeepestDirectories(path, fileList);
				}
			}
		}
		if (directoryCount == 0) {
			fileList.add(dirName);
		}
		return directoryCount;
	}

	public long getDirectorySize(String dirName) throws IOException, URISyntaxException {
		FileSystem fs = getFileSystem(dirName);
		return fs.getContentSummary(new Path(dirName)).getSpaceConsumed();
	}

	public List<String> getFileList(String dirName) throws IOException, URISyntaxException {
		List<String> fileList = new ArrayList<String>();
		
		FileSystem fs = getFileSystem(dirName);
		FileStatus[] fileStatusArray = fs.listStatus(new Path(dirName));
		if (fileStatusArray != null) {
			for (FileStatus fileStatus : fileStatusArray) {
				fileList.add(fileStatus.getPath().toString()); 
			}
		}
		
		return fileList;
	}

}
