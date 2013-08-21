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
package nz.co.gregs.dbvolution.exceptions;

/**
 *
 * @author gregory.graham
 */
public class UnexpectedNumberOfRowsException extends Exception {

    public static final long serialVersionUID = 1;
    private int expectedRows;
    private int actualRows;
    public UnexpectedNumberOfRowsException(int expected, int actual, String message, Exception cause) {
        super(message, cause);
        this.expectedRows = expected;
        this.actualRows = actual;
    }
    
    public UnexpectedNumberOfRowsException(int expected, int actual, String message) {
        this(expected,actual,message,null);
        
    }

    /**
     * @return the expectedRows
     */
    public int getExpectedRows() {
        return expectedRows;
    }

    /**
     * @return the actualRows
     */
    public int getActualRows() {
        return actualRows;
    }
    
}
