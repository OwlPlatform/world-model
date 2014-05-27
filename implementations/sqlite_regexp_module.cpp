/*
 * Copyright (c) 2014 Bernhard Firner
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
 * or visit http://www.gnu.org/licenses/gpl-2.0.html
 */

/*******************************************************************************
 * REGEXP function implementation for the sqlite3 database.
 ******************************************************************************/

#include "sqlite_regexp_module.hpp"
#include "regex_store.hpp"

#include <string>

#include <sqlite3.h>

using std::string;
using std::u16string;

//Destroy the RegexStore object when the database is closed
static void xDestroy(void* userParam) {
	RegexStore* myRegStore = static_cast<RegexStore*>(userParam);
	delete myRegStore;
}

//Callback that implements regular expressions in sqlite3
static void sqlite3_regexp(sqlite3_context *context, int argc, sqlite3_value **argv) {
	//Function expects 2 arguments
  if (argc != 2) {
    sqlite3_result_error_code(context, 1);
    return;
  }

	//If the inputs aren't text then this function won't work
  if ( (sqlite3_value_type(argv[0]) != SQLITE_TEXT )
      or ((sqlite3_value_type(argv[1]) != SQLITE_TEXT ))) {
    sqlite3_result_error_code(context, 2);
    return;
  }

	//Get the stored regex from the user context
	RegexStore* myRegStore = static_cast<RegexStore*>(sqlite3_user_data(context));

  u16string pattern = u16string((const char16_t*)sqlite3_value_text16(argv[0]));
  u16string in_string = u16string((const char16_t*)sqlite3_value_text16(argv[1]));

	if (not myRegStore->preparePattern(pattern)) {
		sqlite3_result_error_code(context, 3);
	}
	sqlite3_result_int(context, myRegStore->patternMatch(in_string));
  return;
}

int initializeRegex(sqlite3 *db) {
	//TODO SQLITE_DETERMINISTIC was added in 3.8.3, should be ORed with SQLITE_UTF16
	RegexStore* myRegStore = new RegexStore();
	return sqlite3_create_function_v2(db, "REGEXP", 2, SQLITE_UTF16, myRegStore, sqlite3_regexp, NULL, NULL, xDestroy);
}

