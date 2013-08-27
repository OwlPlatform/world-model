/*
 * Copyright (c) 2013 Bernhard Firner
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
 * A thread-safe set (but can possibly block during the destructor).
 ******************************************************************************/

#include <set>
#include <mutex>

#ifndef __THREADSAFE_SET__
#define __THREADSAFE_SET__

template <typename T>
class ThreadsafeSet {
	private:
		std::mutex _access;
		std::set<T> _set;

		///Disable copy constructors
		ThreadsafeSet& operator=(const ThreadsafeSet&) = delete;
		ThreadsafeSet(const ThreadsafeSet&) = delete;

	public:

		///Constructor
		ThreadsafeSet() {};

		///Destructor (may block until all accesses are complete)
		~ThreadsafeSet() {
			std::unique_lock<std::mutex> lck(_access);
			_set.clear();
		}

		void insert(const T& value) {
			std::unique_lock<std::mutex> lck(_access);
			_set.insert(value);
		}

		size_t erase(const T& value) {
			std::unique_lock<std::mutex> lck(_access);
			return _set.erase(value);
		}

		template<class UnaryFunction>
		void for_each(UnaryFunction f) {
			std::unique_lock<std::mutex> lck(_access);
			for (auto I : _set) {
				f(I);
			}
		}
};

#endif //ndef __THREADSAFE_SET__

