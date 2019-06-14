/**
 * Copyright 2017-2019 European Union, interactive instruments GmbH
 * Licensed under the EUPL, Version 1.2 or - as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 * https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 *
 * This work was supported by the EU Interoperability Solutions for
 * European Public Administrations Programme (http://ec.europa.eu/isa)
 * through Action 1.17: A Reusable INSPIRE Reference Platform (ARE3NA).
 */
package de.interactive_instruments.etf.testdriver.bsx.partitioning;

import static de.interactive_instruments.etf.testdriver.bsx.BsxConstants.*;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.basex.core.BaseXException;
import org.basex.core.cmd.*;
import org.slf4j.Logger;

import de.interactive_instruments.FileUtils;
import de.interactive_instruments.etf.testdriver.bsx.BsxConstants;
import de.interactive_instruments.exceptions.ExcUtils;
import de.interactive_instruments.exceptions.config.InvalidPropertyException;
import de.interactive_instruments.properties.ConfigPropertyHolder;

/**
 * @author Jon Herrmann ( herrmann aT interactive-instruments doT de )
 */
public class DatabasePartitioner implements DatabaseVisitor {

    private final ConfigPropertyHolder config;
    private final long dbSizeSizePerChunkThreshold;
    private final long dbSizeSizePerChunkLowerThreshold;
    private final long minSizeForOptimization;
    private DatabaseChunk chunk;
    private final String dbBaseName;
    private final Set<String> skippedFiles = new TreeSet<>();
    private final Logger logger;

    // synchronized
    private int currentDbIndex = 0;
    // synchronized
    private String currentDbName;

    // synchronized
    // private long currentDbSize = 0;

    // synchronized
    private long fileCount = 0L;
    // synchronized
    private long size = 0L;

    // Cut the first part of the added file name
    private final int filenameCutIndex;

    // The write lock is acquired when the database is flushed,
    // read locks are acquired for adding single files
    private final Lock contextExchangeLock = new ReentrantLock();

    public DatabasePartitioner(final ConfigPropertyHolder config, final Logger logger,
            final String dbName, final int filenameCutIndex) throws BaseXException {
        this.dbBaseName = dbName;
        this.logger = logger;
        this.filenameCutIndex = filenameCutIndex;

        long chunkSize;
        try {
            chunkSize = config.getPropertyOrDefaultAsLong(BsxConstants.DB_MAX_CHUNK_THRESHOLD, DEFAULT_CHUNK_SIZE_THRESHOLD);
            // one MB
            if (chunkSize < 11000000) {
                chunkSize = 11000000;
            }
        } catch (InvalidPropertyException e) {
            ExcUtils.suppress(e);
            chunkSize = DEFAULT_CHUNK_SIZE_THRESHOLD;
        }
        this.dbSizeSizePerChunkThreshold = chunkSize;
        this.dbSizeSizePerChunkLowerThreshold = Math.round(8.5 * chunkSize);

        long minSizeForOptimization;
        final long GB_30 = 32212254720L;
        try {
            minSizeForOptimization = config.getPropertyOrDefaultAsLong(MIN_OPTIMIZATION_SIZE, GB_30);
        } catch (InvalidPropertyException e) {
            ExcUtils.suppress(e);
            minSizeForOptimization = GB_30;
        }
        this.minSizeForOptimization = minSizeForOptimization;

        this.currentDbName = dbBaseName + "-000";

        this.config = config;

        if (this.dbSizeSizePerChunkThreshold != DEFAULT_CHUNK_SIZE_THRESHOLD) {
            this.logger.info("Database chunk size threshold is set to  {}",
                    FileUtils.byteCountToDisplayRoundedSize(dbSizeSizePerChunkThreshold, 2));
        }
        this.logger.info("Creating first database {}", this.currentDbName);
        this.chunk = DatabaseChunk.newChunk(config, currentDbName);
    }

    private void flushAndOptimize(final String oldDbName, final DatabaseChunk databaseChunk) {
        try {
            new Flush().execute(databaseChunk.ctx);
            logger.info("Added {} to database {}", FileUtils.byteCountToDisplayRoundedSize(databaseChunk.size, 2), oldDbName);
            if (databaseChunk.size >= this.minSizeForOptimization) {
                logger.info("Optimizing");
                new OptimizeAll().execute(databaseChunk.ctx);
            }
        } catch (final BaseXException e) {
            logger.error("Error flushing database", e);
        }
        try {
            new Close().execute(databaseChunk.ctx);
        } catch (BaseXException e) {
            ExcUtils.suppress(e);
        }
    }

    DatabaseChunk getChunk(final long fileSize) {
        contextExchangeLock.lock();
        final DatabaseChunk currentChunk = this.chunk;
        // Create new chunk if the current size exceeds the threshold or if the
        // current chunk size is 85 % of the threshold and exceeds with the current
        // file the threshold
        if (currentChunk.size > this.dbSizeSizePerChunkThreshold ||
                (currentChunk.size + fileSize > this.dbSizeSizePerChunkThreshold &&
                        currentChunk.size > this.dbSizeSizePerChunkLowerThreshold)) {
            final String oldDbName = currentDbName;
            currentDbName = dbBaseName + "-" + String.format("%03d", ++currentDbIndex);
            try {
                this.chunk = DatabaseChunk.newChunk(config, currentDbName);
                logger.info("Created next database {} ", this.currentDbName);
            } catch (final BaseXException e) {
                this.chunk.ctx.close();
                logger.error("Next database {} could not be created", this.currentDbName, e);
            }
            contextExchangeLock.unlock();
            // created a new chunk and opened the database. We must flush the old chunk
            // before we can proceed. This can be done in parallel
            flushAndOptimize(oldDbName, currentChunk);
            // during the flush another new chunk could have been created
            return getChunk(fileSize);
        } else {
            this.chunk = currentChunk.incSize(fileSize);
            contextExchangeLock.unlock();
            return currentChunk;
        }
    }

    @Override
    public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) {
        if (Thread.currentThread().isInterrupted()) {
            return FileVisitResult.TERMINATE;
        }
        final long fileSize = attrs.size();
        try {
            final String fileName = file.toAbsolutePath().toString().substring(filenameCutIndex);
            getChunk(fileSize).add(fileName, file);
            synchronized (this) {
                fileCount++;
                size += fileSize;
            }
        } catch (IOException bsxEx) {
            // Skip not well-formed files
            logger.warn("Data import of file " + file.toString() + " failed : " + bsxEx.getMessage());
            synchronized (skippedFiles) {
                skippedFiles.add(file.getFileName().toString());
            }
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) {
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(final Path file, final IOException exc) {
        synchronized (skippedFiles) {
            skippedFiles.add(file.getFileName().toString());
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) {
        return FileVisitResult.CONTINUE;
    }

    @Override
    public void release() {
        try {
            flushAndOptimize(currentDbName, this.chunk);
            new Open(currentDbName).execute(chunk.ctx);
            new Close().execute(chunk.ctx);
            chunk.ctx.close();
            logger.info("Import completed.");
        } catch (BaseXException e) {
            logger.error("Database import failed: ", e);
        }
    }

    public Set<String> getSkippedFiles() {
        return skippedFiles;
    }

    public int getDbCount() {
        return currentDbIndex + 1;
    }

    public long getSize() {
        return size;
    }

    public long getFileCount() {
        return fileCount;
    }
}
