/**
 * Copyright 2010-2016 interactive instruments GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.interactive_instruments.etf.testdriver.bsx.partitioning;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Set;
import java.util.TreeSet;

import org.basex.core.BaseXException;

/**
 * @author J. Herrmann ( herrmann <aT) interactive-instruments (doT> de )
 */
public class DatabaseInventarization implements DatabaseVisitor {
	private final long maxDbSizeSizePerChunk;
	private long currentDbSize = 0;
	private int currentDbIndex = 0;
	private final Set<String> skippedFiles = new TreeSet<>();
	private long size = 0;
	private long fileCount = 0;

	public DatabaseInventarization(long maxDbSizeSizePerChunk) throws BaseXException {
		currentDbIndex = 0;
		fileCount = 0L;
		this.maxDbSizeSizePerChunk = maxDbSizeSizePerChunk;
	}

	@Override
	public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
			throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			return FileVisitResult.TERMINATE;
		}

		synchronized (this) {
			if (currentDbSize >= maxDbSizeSizePerChunk) {
				currentDbIndex++;
				currentDbSize = 0;
			}
			currentDbSize += attrs.size();
		}

		size += attrs.size();
		fileCount++;
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
			throws IOException {
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult visitFileFailed(Path file, IOException exc)
			throws IOException {
		skippedFiles.add(file.getFileName().toString());
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult postVisitDirectory(Path dir, IOException exc)
			throws IOException {
		return FileVisitResult.CONTINUE;
	}

	/**
	 * Number of databases.
	 *
	 * @return number of databases
	 */
	public int getDbCount() {
		return currentDbIndex + 1;
	}

	/**
	 * Total size of files.
	 *
	 * @return files size in bytes
	 */
	public long getSize() {
		return size;
	}

	/**
	 * Number of files.
	 *
	 * @return number of files
	 */
	public long getFileCount() {
		return fileCount;
	}

	public Set<String> getSkippedFiles() {
		return skippedFiles;
	}

	@Override
	public void release() {
		// nothing to do
	}
}
