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
package de.interactive_instruments.etf.testdriver.bsx;

public final class BsxConstants {

    private BsxConstants() {}

    public static final String ETS_DEF_FILE_SUFFIX = "-bsxets.xml";

    public static final String PROJECT_CHECK_FILE_SUFFIX_DEPRECATED = "-bsxpc.xq";
    public static final String PROJECT_CHECK_FILE_SUFFIX = "-check-parameters.xq";

    public static final String ETF_TESTDB_PREFIX = "etf-tdb-";

    public static final String PROJECT_DIR_KEY = "etf.projects.dir";

    public static final String DB_MAX_CHUNK_THRESHOLD = "etf.testdrivers.bsx.db.chunk.size.threshold";
    public static final long DEFAULT_CHUNK_SIZE_THRESHOLD = 33500000000L;

    public static final String CHOP_WHITESPACES = "etf.testdrivers.bsx.whitespaces.chop";

    public static final String MIN_OPTIMIZATION_SIZE = "etf.testdrivers.bsx.optimization.size.min";
}
