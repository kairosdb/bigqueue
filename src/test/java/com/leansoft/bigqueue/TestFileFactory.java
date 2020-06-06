package com.leansoft.bigqueue;

import com.leansoft.bigqueue.utils.FileFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class TestFileFactory implements FileFactory
{
	private Map<String, Function<String, File>> fileLookup = new HashMap<>();
	private Map<String, Long> lastModified = new HashMap<>();

	@Override
	public File newFile(String path)
	{
		Function<String, File> fileCallable = fileLookup.get(path);

		if (fileCallable != null)
		{
			try
			{
				return fileCallable.apply(path);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}

		return new File(path);
	}

	@Override
	public long lastModified(File file)
	{
		Long modified = lastModified.computeIfAbsent(file.getAbsolutePath(), f -> file.lastModified());
		return modified;
	}

	public void setFileForPath(String path, Function<String, File> callable)
	{
		fileLookup.put(path, callable);
	}

	public void setLastModified(String path, long modified)
	{
		lastModified.put(path, modified);
	}
}
