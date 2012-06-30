#ifndef __SQLITE_REGEXP_MODULE_HPP__
#define __SQLITE_REGEXP_MODULE_HPP__

#include <sqlite3.h>

/*
 * Returns the result of a call to sqlite3_create_function
 * Adds support for REGEX searches in SQLITE3.
 */
int initializeRegex(sqlite3 *db);

#endif //Not defined __SQLITE_REGEXP_MODULE_HPP__
