#include "sqlite_regexp_module.hpp"

#include <string>

#include <sqlite3.h>

//TODO In the future C++11 support for regex should be used over these POSIX
//regex c headers.
#include <sys/types.h>
#include <regex.h>

using std::string;
using std::u16string;

//Callback that implements regular expressions in sqlite3
static void sqlite3_regexp(sqlite3_context *context, int argc, sqlite3_value **argv) {
  if (argc != 2) {
    sqlite3_result_error_code(context, 1);
    return;
  }

  if ( (sqlite3_value_type(argv[0]) != SQLITE_TEXT )
      or ((sqlite3_value_type(argv[1]) != SQLITE_TEXT ))) {
    sqlite3_result_error_code(context, 2);
    return;
  }

  u16string pattern = u16string((const char16_t*)sqlite3_value_text16(argv[0]));
  u16string in_string = u16string((const char16_t*)sqlite3_value_text16(argv[1]));
  regex_t exp;
  int err = regcomp(&exp, std::string(pattern.begin(), pattern.end()).c_str(), REG_EXTENDED);
  //Return no results if the expression did not compile.
  if (0 != err) {
    sqlite3_result_error_code(context, 3);
    return;
  }

  //Check each match to make sure it consumes the whole string
  regmatch_t pmatch;
  int match = regexec(&exp, std::string(in_string.begin(), in_string.end()).c_str(), 1, &pmatch, 0);
  if (0 == match and 0 == pmatch.rm_so and in_string.size() == pmatch.rm_eo) {
    sqlite3_result_int(context, true);
  }
  else {
    sqlite3_result_int(context, false);
  }
  //TODO FIXME This should be stored in the context pointer and freed after attempting all of the matches.
  regfree(&exp);
  return;
}

int initializeRegex(sqlite3 *db) {
	return sqlite3_create_function_v2(db, "REGEXP", 2, SQLITE_UTF16, NULL, sqlite3_regexp, NULL, NULL, NULL);
}

