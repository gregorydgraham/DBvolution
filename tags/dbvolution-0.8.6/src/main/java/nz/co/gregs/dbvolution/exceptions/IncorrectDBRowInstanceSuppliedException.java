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
public class IncorrectDBRowInstanceSuppliedException extends RuntimeException{
    
    public static final long serialVersionUID = 1L;

    public IncorrectDBRowInstanceSuppliedException() {
        super("The Field Supplied Is Not A Field Of This Instance: use only fields from this instance.");
    }
    
}
