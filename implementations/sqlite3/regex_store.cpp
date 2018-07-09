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
 * Intermediate storage for the regular expressions used in the sqlite3 REGEXP
 * function.
 ******************************************************************************/

#include "regex_store.hpp"

#include <string>

//TODO In the future C++11 support for regex should be used over these POSIX
//regex c headers.
#include <sys/types.h>
#include <regex.h>
//No expression at initialization
RegexStore::RegexStore() {
	is_compiled = false;
}

//Free the expression if one was compiled
RegexStore::~RegexStore() {
	if (is_compiled) {
		regfree(&exp);
	}
}

/*
 * Create a new regex pattern if necessary, or keep the old one if the
 * pattern has not changed
 * Returns true on success, false on failure.
 */
bool RegexStore::preparePattern(std::u16string& patt) {
	//New pattern? Clear out the existing regex_t
	if (is_compiled and this->pattern != patt) {
		//std::cerr<<"Replacing pattern "<<std::string(pattern.begin(), pattern.end())<<" with "<<std::string(patt.begin(), patt.end())
		regfree(&exp);
		is_compiled = false;
	}
	//Do we need to compile a new regex pattern?
	if (not is_compiled) {
		//Compile a new regex pattern using the ascii string representation
		std::string tmp_string(patt.begin(), patt.end());
		int err = regcomp(&exp, tmp_string.c_str(), REG_EXTENDED);
		//Return without creating an expression if this failed
		if (0 != err) {
			return false;
			//is_compiled remains false
		}
		else {
			pattern = patt;
			is_compiled = true;
		}
	}
	return true;
}

bool RegexStore::patternMatch(std::u16string& in_string) {
	//No pattern matches when we don't have a valid pattern
	if (not is_compiled) {
    return false;
	}

  //Check for a match
  regmatch_t pmatch;
	std::string tmp_str(in_string.begin(), in_string.end());
  int match = regexec(&exp, tmp_str.c_str(), 1, &pmatch, 0);
	//Make sure that each character was matched and that the entire input string
	//was consumed by the pattern
  if (0 == match and 0 == pmatch.rm_so and in_string.size() == pmatch.rm_eo) {
		return true;
  }
  else {
		return false;
  }
}

