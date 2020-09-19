package org.kairosdb.bigqueue.utils;

import java.io.File;

public class FileSystemFileFactory implements FileFactory
{
	@Override
	public File newFile(String path)
	{
		return new File(path);
	}

	@Override
	public long lastModified(File file)
	{
		return file.lastModified();
	}

}
