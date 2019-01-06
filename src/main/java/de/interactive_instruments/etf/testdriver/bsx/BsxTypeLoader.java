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

import static de.interactive_instruments.etf.testdriver.bsx.Types.TEST_ITEM_TYPES;

import java.io.IOException;

import de.interactive_instruments.etf.EtfConstants;
import de.interactive_instruments.etf.dal.dao.DataStorage;
import de.interactive_instruments.etf.dal.dao.WriteDao;
import de.interactive_instruments.etf.dal.dto.test.ExecutableTestSuiteDto;
import de.interactive_instruments.etf.dal.dto.test.TestItemTypeDto;
import de.interactive_instruments.etf.testdriver.AbstractEtsFileTypeLoader;
import de.interactive_instruments.exceptions.ExcUtils;
import de.interactive_instruments.exceptions.InitializationException;
import de.interactive_instruments.exceptions.InvalidStateTransitionException;
import de.interactive_instruments.exceptions.StorageException;
import de.interactive_instruments.exceptions.config.ConfigurationException;
import de.interactive_instruments.properties.ConfigProperties;
import de.interactive_instruments.properties.ConfigPropertyHolder;

/**
 * Used to propagate:
 * - Test Object Types (static),
 * - Test Item Types (static),
 * - Component Info (static),
 * - Executable Test Suites (dynamically reloaded on change),
 * - Translation Template Bun dles (dynamically reloaded on change)
 *
 * The Component Info is propagated by the Test Driver
 *
 * @author Jon Herrmann ( herrmann aT interactive-instruments doT de )
 */
public class BsxTypeLoader extends AbstractEtsFileTypeLoader {

	private final ConfigProperties configProperties;

	/**
	 * Default constructor.
	 */
	public BsxTypeLoader(final DataStorage dataStorage) {
		super(dataStorage, new BsxEtsBuilder(dataStorage.getDao(ExecutableTestSuiteDto.class)));
		this.configProperties = new ConfigProperties(EtfConstants.ETF_PROJECTS_DIR);
	}

	@Override
	public void doInit()
			throws ConfigurationException, InitializationException, InvalidStateTransitionException {

		this.configProperties.expectAllRequiredPropertiesSet();
		this.watchDir = configProperties.getPropertyAsFile(EtfConstants.ETF_PROJECTS_DIR);
		try {
			this.watchDir.expectDirIsReadable();
		} catch (IOException e) {
			throw new InitializationException(e);
		}

		// First propagate static types
		final WriteDao<TestItemTypeDto> testItemTypeDao = ((WriteDao<TestItemTypeDto>) dataStorageCallback
				.getDao(TestItemTypeDto.class));
		try {
			testItemTypeDao.deleteAllExisting(TEST_ITEM_TYPES.keySet());
			testItemTypeDao.addAll(TEST_ITEM_TYPES.values());
		} catch (final StorageException e) {
			try {
				testItemTypeDao.deleteAllExisting(TEST_ITEM_TYPES.keySet());
			} catch (StorageException e3) {
				ExcUtils.suppress(e3);
			}
			throw new InitializationException(e);
		}
	}

	@Override
	public ConfigPropertyHolder getConfigurationProperties() {
		return configProperties;
	}

}
