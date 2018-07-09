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

#ifndef __REGEX_STORE_HPP__
#define __REGEX_STORE_HPP__

#include <string>

//TODO In the future C++11 support for regex should be used over these POSIX
//regex c headers.
#include <sys/types.h>
#include <regex.h>

class RegexStore {
	private:
		//If a regex was previously compiled and is stored in the exp variable
		bool is_compiled;
		//A pre-compiled expression
		regex_t exp;
		//The pattern that was used to make the expression
		std::u16string pattern;
	public:
		RegexStore();
		~RegexStore();
		/*
		 * Create a new regex pattern if necessary, or keep the old one if the
		 * pattern has not changed
		 * Returns true on success, false on failure.
		 */
		bool preparePattern(std::u16string& patt);
		/*
		 * Returns true if in_string matches the current regex pattern.
		 * Always returns false if is_compiled is false.
		 */
		bool patternMatch(std::u16string& in_string);
};

#endif

