/*
 * Copyright 2013 gregory.graham.
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
package nz.co.gregs.dbvolution.datatypes;

import java.beans.PropertyEditorSupport;

/**
 *
 * @author gregory.graham
 */
public class QueryableDatatypeEditor extends PropertyEditorSupport {

    private String format;

    public void setFormat(String format) {
        this.format = format;
    }

    @Override
    public void setAsText(String text) {
        if (format != null && format.equals("upperCase")) {
            text = text.toUpperCase();
        }
        Object value = getValue();
        if (value instanceof QueryableDatatype) {
            QueryableDatatype qdt = (QueryableDatatype) value;
            qdt.setValue(text);
        } else {
            QueryableDatatype type = QueryableDatatype.getQueryableDatatypeForObject(value);
            setValue(type);
        }
    }
}