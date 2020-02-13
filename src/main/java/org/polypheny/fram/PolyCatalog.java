/*
 * Copyright 2016-2020 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.fram;


import java.util.UUID;


/**
 *
 */
class PolyCatalog extends AbstractCatalog {

    @Override
    public UUID getNodeId() {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    private static class SingletonHolder {

        private static PolyCatalog INSTANCE = new PolyCatalog();


        private SingletonHolder() {
        }
    }


    static PolyCatalog getInstance() {
        return SingletonHolder.INSTANCE;
    }
}
