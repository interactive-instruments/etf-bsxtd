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

import static de.interactive_instruments.etf.testdriver.bsx.BsxConstants.CHOP_WHITESPACES;

import java.nio.file.Path;

import org.basex.core.BaseXException;
import org.basex.core.Context;
import org.basex.core.cmd.Add;
import org.basex.core.cmd.CreateDB;

import de.interactive_instruments.properties.ConfigPropertyHolder;

/**
 * @author Jon Herrmann ( herrmann aT interactive-instruments doT de )
 */
final class DatabaseChunk {
    final Context ctx;
    final long size;

    static DatabaseChunk newChunk(final ConfigPropertyHolder config, final String name) throws BaseXException {
        final Context ctx = new Context();

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

        new CreateDB(name).execute(ctx);
        return new DatabaseChunk(ctx, 0);
    }

    private DatabaseChunk(final Context ctx, final long dbSize) {
        this.ctx = ctx;
        this.size = dbSize;
    }

    DatabaseChunk incSize(final long newSize) {
        return new DatabaseChunk(ctx, this.size + newSize);
    }

    void add(final String fileName, final Path path) throws BaseXException {
        new Add(fileName, path.toString()).execute(ctx);
    }
}
