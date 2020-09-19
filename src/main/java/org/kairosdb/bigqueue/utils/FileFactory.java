package org.kairosdb.bigqueue.utils;

import java.io.File;

public interface FileFactory
{
	File newFile(String path);
	long lastModified(File file);
}
