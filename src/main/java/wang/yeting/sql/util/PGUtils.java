/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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
package wang.yeting.sql.util;


import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class PGUtils {

    private final static long[] pseudoColumnHashCodes;
    private static Set<String> keywords;

    static {
        long[] array = {
                FnvHash.Constants.CURRENT_TIMESTAMP
        };
        Arrays.sort(array);
        pseudoColumnHashCodes = array;
    }

    public static boolean isKeyword(String name) {
        if (name == null) {
            return false;
        }

        String name_lower = name.toLowerCase();

        Set<String> words = keywords;

        if (words == null) {
            words = new HashSet<String>();
            Utils.loadFromFile("META-INF/druid/parser/postgresql/keywords", words);
            keywords = words;
        }

        return words.contains(name_lower);
    }

    public static boolean isPseudoColumn(long hash) {
        return Arrays.binarySearch(pseudoColumnHashCodes, hash) >= 0;
    }
}
