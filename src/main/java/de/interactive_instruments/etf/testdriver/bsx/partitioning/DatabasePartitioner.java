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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.basex.core.BaseXException;
import org.basex.core.Context;
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
    private final long minSizeForOptimization;
    private Context ctx = new Context();
    private final String dbBaseName;
    private final Set<String> skippedFiles = new TreeSet<>();
    private final Logger logger;

    // synchronized
    private int currentDbIndex = 0;
    // synchronized
    private String currentDbName;

    // synchronized
    private AtomicLong currentDbSize = new AtomicLong(0);

    // synchronized
    private long fileCount = 0L;
    // synchronized
    private long size = 0L;

    // Cut the first part of the added file name
    private final int filenameCutIndex;

    // The write lock is acquired when the database is flushed,
    // read locks are acquired for adding single files
    private final Lock flushLock = new ReentrantLock();
    private final ReentrantReadWriteLock fairCtxLock = new ReentrantReadWriteLock(true);

    public DatabasePartitioner(final ConfigPropertyHolder config, final Logger logger,
            final String dbName, final int filenameCutIndex) throws BaseXException {
        this.dbBaseName = dbName;
        this.logger = logger;
        this.filenameCutIndex = filenameCutIndex;

        long chunkSize;
        try {
            chunkSize = config.getPropertyOrDefaultAsLong(BsxConstants.DB_MAX_CHUNK_THRESHOLD, DEFAULT_CHUNK_SIZE_THRESHOLD);
        } catch (InvalidPropertyException e) {
            ExcUtils.suppress(e);
            chunkSize = DEFAULT_CHUNK_SIZE_THRESHOLD;
        }
        this.dbSizeSizePerChunkThreshold = chunkSize;

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
        setOptions(ctx);

        if (this.dbSizeSizePerChunkThreshold != DEFAULT_CHUNK_SIZE_THRESHOLD) {
            this.logger.info("Database chunk size threshold is set to  {}",
                    FileUtils.byteCountToDisplayRoundedSize(dbSizeSizePerChunkThreshold, 2));
        }
        this.logger.info("Creating first database {}", this.currentDbName);
        new CreateDB(currentDbName).execute(ctx);
    }

    private void setOptions(final Context ctx) {
        try {
            new org.basex.core.cmd.Set("AUTOFLUSH", "false").execute(ctx);
            new org.basex.core.cmd.Set("TEXTINDEX", "true").execute(ctx);
            new org.basex.core.cmd.Set("ATTRINDEX", "true").execute(ctx);
            new org.basex.core.cmd.Set("FTINDEX", "true").execute(ctx);
            new org.basex.core.cmd.Set("MAXLEN", "160").execute(ctx);

            new org.basex.core.cmd.Set("DTD", "false").execute(ctx);
            new org.basex.core.cmd.Set("XINCLUDE", "false").execute(ctx);
            new org.basex.core.cmd.Set("INTPARSE", "true").execute(ctx);

            new org.basex.core.cmd.Set("ENFORCEINDEX", "true").execute(ctx);
            new org.basex.core.cmd.Set("COPYNODE", "false").execute(ctx);

            new org.basex.core.cmd.Set("CHOP",
                    config.getPropertyOrDefault(CHOP_WHITESPACES, "true")).execute(ctx);
            // already filtered
            new org.basex.core.cmd.Set("SKIPCORRUPT", "false").execute(ctx);
        } catch (BaseXException e) {
            logger.error("Failed to set option ", e);
        }
    }

    private void flushAndOptimize(final String oldDbName, final long oldDbSize, final Context oldCtx) {
        try {
            new Flush().execute(oldCtx);
            logger.info("Added {} to database {}", FileUtils.byteCountToDisplayRoundedSize(oldDbSize, 2), oldDbName);
            if (oldDbSize >= this.minSizeForOptimization) {
                logger.info("Optimizing");
                new OptimizeAll().execute(oldCtx);
            }
        } catch (final BaseXException e) {
            logger.error("Error flushing database", e);
        }
        try {
            new Close().execute(oldCtx);
        } catch (BaseXException e) {
            ExcUtils.suppress(e);
        }
    }

    void checkForNextDatabase() {
        if (currentDbSize.get() >= dbSizeSizePerChunkThreshold && flushLock.tryLock()) {
            // get priority lock
            fairCtxLock.writeLock().lock();
            final Context oldCtx = this.ctx;
            final Context newCtx = new Context();
            setOptions(newCtx);
            this.ctx = newCtx;
            final String oldDbName = currentDbName;
            currentDbName = dbBaseName + "-" + String.format("%03d", ++currentDbIndex);
            try {
                new CreateDB(currentDbName).execute(this.ctx);
                logger.info("Created next database {} ", this.currentDbName);
            } catch (final BaseXException e) {
                this.ctx.close();
                logger.error("Next database {} could not be created", this.currentDbName, e);
            }
            final long oldDbSize = currentDbSize.getAndSet(0);
            fairCtxLock.writeLock().unlock();
            flushAndOptimize(oldDbName, oldDbSize, oldCtx);
            flushLock.unlock();
        }
    }

    @Override
    public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) {
        if (Thread.currentThread().isInterrupted()) {
            return FileVisitResult.TERMINATE;
        }
        checkForNextDatabase();
        try {
            final String fileName = file.toAbsolutePath().toString().substring(filenameCutIndex);
            fairCtxLock.readLock().lock();
            new Add(fileName, file.toString()).execute(ctx);
            currentDbSize.addAndGet(attrs.size());
            fairCtxLock.readLock().unlock();
            synchronized (this) {
                fileCount++;
                size += attrs.size();
            }
        } catch (IOException bsxEx) {
            // Skip not well-formed files
            logger.warn("Data import of file " + file.toString() + " failed : " + bsxEx.getMessage());
            synchronized (skippedFiles) {
                skippedFiles.add(file.getFileName().toString());
            }
            try {
                fairCtxLock.readLock().unlock();
            } catch (IllegalMonitorStateException ign) {
                ExcUtils.suppress(ign);
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
            if (flushLock.tryLock(6, TimeUnit.MINUTES)) {
                flushAndOptimize(currentDbName, currentDbSize.get(), this.ctx);
                new Open(currentDbName).execute(ctx);
                new Close().execute(ctx);
                ctx.close();
                logger.info("Import completed.");
                flushLock.unlock();
            } else {
                logger.info("Failed to acquire lock, for final import");
            }
        } catch (InterruptedException | BaseXException e) {
            logger.error("Database import failed: ", e);
            try {
                flushLock.unlock();
            } catch (final IllegalMonitorStateException ign) {
                ExcUtils.suppress(ign);
            }
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
