/*
 * Copyright 2014 gregory.graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution.internal.query;

import java.io.Serializable;
import java.util.Comparator;
import nz.co.gregs.dbvolution.DBRow;

/**
 *
 * @author gregory.graham
 */
public class DBRowNameComparator implements Comparator<DBRow>, Serializable {

	static final long serialVersionUID = 1L;

	public DBRowNameComparator() {
	}

	@Override
	public int compare(DBRow first, DBRow second) {
		String firstCanonicalName = first.getClass().getCanonicalName();
		String secondCanonicalName = second.getClass().getCanonicalName();
		if (firstCanonicalName != null && secondCanonicalName != null) {
			return firstCanonicalName.compareTo(secondCanonicalName);
		} else {
			return first.getClass().getSimpleName().compareTo(second.getClass().getSimpleName());
		}
	}

}
