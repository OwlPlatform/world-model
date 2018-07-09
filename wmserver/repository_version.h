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
 * Uses a preprocessor symbol to define GIT_REPO_VERSION
 * If this symbol was not defined then the string "unknown" will be used.
 * Compile like this:
 *   g++ -DGIT_REPO_VERSION=\"`git diff-tree -s --pretty="Commit: %h, Date: %aD" HEAD`\"
 ******************************************************************************/

//Default to unknown if the version wasn't defined
#ifndef GIT_REPO_VERSION
#define GIT_REPO_VERSION "unknown"
#endif

