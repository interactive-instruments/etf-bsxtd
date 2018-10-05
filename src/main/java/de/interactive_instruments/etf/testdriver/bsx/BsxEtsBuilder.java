/**
 * Copyright 2017 European Union, interactive instruments GmbH
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.interactive_instruments.SUtils;
import de.interactive_instruments.UriUtils;
import de.interactive_instruments.XmlUtils;
import de.interactive_instruments.etf.dal.dao.Dao;
import de.interactive_instruments.etf.dal.dao.StreamWriteDao;
import de.interactive_instruments.etf.dal.dto.test.ExecutableTestSuiteDto;
import de.interactive_instruments.etf.testdriver.TypeBuildingFileVisitor;

/**
 * @author Jon Herrmann ( herrmann aT interactive-instruments doT de )
 */
class BsxEtsBuilder implements TypeBuildingFileVisitor.TypeBuilder<ExecutableTestSuiteDto> {

	private final StreamWriteDao<ExecutableTestSuiteDto> writeDao;
	private final static Logger logger = LoggerFactory.getLogger(BsxEtsBuilder.class);

	BsxEtsBuilder(final Dao<ExecutableTestSuiteDto> writeDao) {
		this.writeDao = (StreamWriteDao<ExecutableTestSuiteDto>) writeDao;
	}

	private static class BsxEtsBuilderCmd extends TypeBuildingFileVisitor.TypeBuilderCmd<ExecutableTestSuiteDto> {

		private final StreamWriteDao<ExecutableTestSuiteDto> writeDao;

		BsxEtsBuilderCmd(final Path path, final StreamWriteDao<ExecutableTestSuiteDto> writeDao)
				throws IOException, XPathExpressionException {
			super(path);
			this.writeDao = writeDao;

			// Parse ID
			final XPath xpath = de.interactive_instruments.etf.XmlUtils.newXPath();
			// final XmlUtils.XmlHandle xmlHandle = XmlUtils.newXmlHandle(xpath, path.toFile());

			final String oid = XmlUtils.newXmlHandle(xpath, path.toFile()).evaluateValue("/etf:ExecutableTestSuite[1]/@id");
			if (SUtils.isNullOrEmpty(oid)) {
				throw new IOException("ETS ID not found in " + path);
			}
			if (oid.length() != 39) {
				throw new IOException("ID " + oid + " is invalid");
			}
			this.id = oid.substring(3);

			// Set ETS dependencies
			final String[] etsStrIds = XmlUtils.newXmlHandle(xpath, path.toFile()).evaluateValues(
					"/etf:ExecutableTestSuite[1]/etf:dependencies[1]/etf:executableTestSuite/@ref");
			if (etsStrIds != null) {
				for (final String strId : etsStrIds) {
					if (strId.length() != 39) {
						throw new IOException("ID " + strId + " is invalid");
					}
					dependsOn(strId.substring(3));
				}
			}

			// Set Translation Template Bundle dependency
			final String ttbId = XmlUtils.newXmlHandle(xpath, path.toFile()).evaluateValue(
					"/etf:ExecutableTestSuite[1]/etf:translationTemplateBundle[1]/@ref");
			if (!SUtils.isNullOrEmpty(ttbId)) {
				if (ttbId.length() != 39) {
					throw new IOException("ID " + ttbId + " is invalid");
				}
				dependsOn(ttbId.substring(3));
			}

			// Set Tag dependencies
			final String[] tagIds = XmlUtils.newXmlHandle(xpath, path.toFile()).evaluateValues(
					"/etf:ExecutableTestSuite[1]/etf:tags[1]/etf:tag/@ref");
			if (tagIds != null) {
				for (final String tagId : tagIds) {
					if (tagId.length() != 39) {
						throw new IOException("ID " + tagId + " is invalid");
					}
					dependsOn(tagId.substring(3));
				}
			}
		}

		@Override
		protected ExecutableTestSuiteDto build() {
			try {
				final File file = path.toFile();
				final String hash = UriUtils.hashFromTimestampOrContent(file.toURI());
				final FileInputStream fileInputStream = new FileInputStream(file);
				return writeDao.add(fileInputStream, dto -> {
					dto.setItemHash(hash);
					dto.setLocalPath(file.getAbsolutePath());
					return dto;
				});
			} catch (IOException e) {
				logger.error("Error creating Executable Test Suite from file {}", path, e);
			}
			return null;
		}
	}

	@Override
	public TypeBuildingFileVisitor.TypeBuilderCmd<ExecutableTestSuiteDto> prepare(final Path path) {
		if (path.toString().endsWith(BsxConstants.ETS_DEF_FILE_SUFFIX)) {
			try {
				return new BsxEtsBuilderCmd(path, writeDao);
			} catch (IOException | XPathExpressionException e) {
				logger.error("Could not prepare ETS {} ", path, e);
			}
		}
		return null;
	}
}
