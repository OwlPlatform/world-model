/*
 * Copyright (c) 2012 Bernhard Firner and Rutgers University
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
 * Helper classes that make it easier for world models to support standing
 * queries.
 ******************************************************************************/

#ifndef __STANDING_QUERY_HPP__
#define __STANDING_QUERY_HPP__

#include <algorithm>
#include <list>
#include <functional>
#include <map>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <threadsafe_set.hpp>

#include <owl/world_model_protocol.hpp>

//TODO In the future C++11 support for regex should be used over these POSIX
//regex c headers.
#include <sys/types.h>
#include <regex.h>

using world_model::WorldState;

class StandingQuery {
	private:
		/***************************************************************************
		 * Private static objects and functions
		 **************************************************************************/
		struct Update {
			WorldState state;
			bool invalidate_attributes;
			bool invalidate_objects;
		};

		//Input/output queue. Input from solver threads, output to standing query thread
		static std::mutex solver_data_mutex;
    static std::queue<Update> solver_data;

		/**
		 * Loop that moves data from the internal data queue to interested client
		 * threads.
		 */
		static void dataProcessingLoop();

		/**
		 * Thread that runs the dataProcessingLoop.
		 */
		static std::thread data_processing_thread;

		/**
		 * Mutex that guarantees that only one data_processing_thread is running.
		 */
		static std::mutex data_processing_mutex;

		/**
		 * The set of all current standing queries, used to find all of the
		 * existing StandingQuery objects so that data can be offered to them.
		 */
    static ThreadsafeSet<StandingQuery*> subscriptions;

		/**
		 * A mutex to protect access to the @subscriptions set.
		 */
    static std::mutex subscription_mutex;

    /**
		 * The origin_attributes map is used to quickly check if any data from an
		 * origin is interesting.
		 */
    static std::map<std::u16string, std::set<std::u16string>> origin_attributes;

    /**
		 * Mutex for the origin_attributes map.
		 */
    static std::mutex origin_attr_mutex;

		/***************************************************************************
		 * Variables used for the regular expression matching and internal data
		 * storage for the query.
		 **************************************************************************/
    //Need to lock this mutex before changing @cur_state
    std::mutex data_mutex;
    //Place where the world model will store data for this standing query.
    WorldState cur_state;
    //Remember which URIs and attributes match this query
    //For attributes remember which of the desired attributes they matched
    std::map<world_model::URI, bool> uri_accepted;
    std::map<world_model::URI, std::set<size_t>> uri_matches;
    //Remember accepted attributes so that the standing query can notify
    //the subscriber when identifiers and attributes are expired or deleted
    //The data_mutex must be locked before modifying this structure
    std::map<world_model::URI, std::set<std::u16string>> current_matches;
    ///This contains empty sets for entries without matches
    std::map<std::u16string, std::set<size_t>> attribute_accepted;
    world_model::URI uri_pattern;
    std::vector<std::u16string> desired_attributes;
    regex_t uri_regex;
    std::map<std::u16string, regex_t> attr_regex;
		//True if this query should also retrieve data
    bool get_data;
		//True after the provided regular expression successfully compiles
    bool regex_valid;

		/**
		 * Partial matches are when some attributes matched, but not all so the
		 * data does not yet match the query. Partial matches are stored so that
		 * when more data arrives that completes the match the entire data set can
		 * be quickly set.  This also allows for rapid rechecking of a match if
		 * attributes are deleted or expired.
		 */
    WorldState partial;

	public:
    /**
     * Push new data from a solver into the internal data queue. A thread will
		 * transfer this data to interested client threads.
		 * This is thread-safe and non-blocking (except for memory allocation),
		 * meant to be called from solver threads.
     */
		//static void pushData(WorldState& ws);

		static void for_each(std::function<void(StandingQuery*)> f);

		/**
		 * Create a new standing query, initializing internal regex code and adding
		 * this StandingQuery to the internal list of queries that should see
		 * incoming data from solvers.
		 * Start the @data_processing_thread if this is the first standing query.
		 */
    StandingQuery(WorldState& cur_state, const world_model::URI& uri,
        const std::vector<std::u16string>& desired_attributes, bool get_data = true);

		/**
		 * Remove this standing query from the internal list of queries.
		 * If there are no more standing queries stop the @data_processing_thread.
		 */
		~StandingQuery();

    ///Copy constructor
    StandingQuery(const StandingQuery&);

		///Assignment
    StandingQuery& operator=(const StandingQuery&);

		/**
		 * Get any new data given to this standing query since the last time that
		 * getData was called.
		 */
		WorldState getData();

    /**
     * Update the list of attributes provided by origins.
     */
    static void addOriginAttributes(std::u16string& origin, std::set<std::u16string>& attributes);

		/**
		 * Offer data from the input queue for every StandingQuery
		 * @invalidate is true if the object or attributes are not longer valid,
		 * due to expiration or deletion, and should be removed.
		 */
		static void offerData(WorldState& ws, bool invalidate_attributes, bool invalidate_objects);

    /**
     * Return true if this origin has data that this standing query might
     * be interested in and false otherwise.
     */
    bool interestingOrigin(std::u16string& origin);

    /**
     * Return a subset of the world state that this query is interested in.
     * Also remember partial matches so that later calls to showInterested
     * do not need to provide the entire world state. This call can
     * quickly check if any data is interesting by seeing if the origin
     * itself is interesting, but will skip this if the world state
     * contains data from multiple origins.
     */
    WorldState showInterested(WorldState& ws, bool multiple_origins = false);

    /**
     * Return a subset of the world state that this query is interested in.
     * This is similar to showInterested but enforced exact string matches
     * for transient names and does not store transients as partial results.
     * This call can * quickly check if any data is interesting by seeing if
     * the origin itself is interesting, but will skip this if the world state
     * contains data from multiple origins.
     */
    WorldState showInterestedTransient(WorldState& ws, bool multiple_origins = false);

    /**
     * Invalidate a subset of the world state that would be modified if the
     * supplied URI is expired or deleted.
     */
		void invalidateObject(world_model::URI name, world_model::Attribute creation);

    /**
     * Return a subset of the world state that would be modified if the
     * supplied URI attributes are expired or deleted.
     */
    void invalidateAttributes(world_model::URI name,
        std::vector<world_model::Attribute>& attrs_to_remove);

    /**
     * Insert data in a thread safe way
     * Data is not checked to see if it matches the given
     * query first so the caller must check that first, on their own
     * or with the showInterested function call.
     */
    void insertData(WorldState& ws);
};

#endif //ifndef __STANDING_QUERY_HPP__

