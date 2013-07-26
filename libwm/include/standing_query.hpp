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
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include <owl/world_model_protocol.hpp>

//Lock-free queue for pushing and consuming data
#include <boost/lockfree/queue.hpp>

//TODO In the future C++11 support for regex should be used over these POSIX
//regex c headers.
#include <sys/types.h>
#include <regex.h>

/**
 * Standing queries are used when the same query would be repeated many times.
 * The standing query call returns an object that can be used to retrieve
 * any updates to the query since the last time it was checked. If this object
 * is destroyed then it ends the standing query.
 */
class StandingQuery {
  public:
    typedef std::map<world_model::URI, std::vector<world_model::Attribute>> world_state;
  private:
    //Need to lock this mutex before changing the current state
    std::mutex data_mutex;
    //Place where the world model will store data for this standing query.
    world_state cur_state;
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
    bool get_data;
    bool regex_valid;

    ///Mutex for the origin_attributes map
    static std::mutex origin_attr_mutex;
    ///Attributes that different origin's will offer
    static std::map<std::u16string, std::set<std::u16string>> origin_attributes;

    //No copying or assignment. Deleting the copy constructor prevents passing
    //by value which would cause trouble with the compiled regex and the mutex.
    StandingQuery& operator=(const StandingQuery&) = delete;
    StandingQuery(const StandingQuery&) = delete;

    //Partial matches (some attributes matched, but not all)
    //Partial matches are stored so that when a complete match is made they
    //can be sent and also so that a match can be quickly rechecked if
    //attributes are deleted or expired.
    world_state partial;
  public:
    /**
     * Update the list of attributes provided by origins.
     */
    static void addOriginAttributes(std::u16string& origin, std::set<std::u16string>& attributes);

    StandingQuery(const world_model::URI& uri,
        const std::vector<std::u16string>& desired_attributes, bool get_data = true);

    ///Free memory from regular expressions
    ~StandingQuery();

    ///r-value copy constructor
    StandingQuery(StandingQuery&& other);

    ///r-value assignment
    StandingQuery& operator=(StandingQuery&& other);

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
    world_state showInterested(world_state& ws, bool multiple_origins = false);

    /**
     * Return a subset of the world state that this query is interested in.
     * This is similar to showInterested but enforced exact string matches
     * for transient names and does not store transients as partial results.
     * This call can * quickly check if any data is interesting by seeing if
     * the origin itself is interesting, but will skip this if the world state
     * contains data from multiple origins.
     */
    world_state showInterestedTransient(world_state& ws, bool multiple_origins = false);

    /**
     * Return a subset of the world state that would be modified if the
     * supplied URI is expired or deleted.
     */
    void expireURI(world_model::URI uri, world_model::grail_time);

    /**
     * Return a subset of the world state that would be modified if the
     * supplied URI attributes are expired or deleted.
     */
    void expireURIAttributes(world_model::URI uri,
        const std::vector<world_model::Attribute>& entries,
        world_model::grail_time);

    /**
     * Insert data in a thread safe way
     * Data is not checked to see if it matches the given
     * query first so the caller must check that first, on their own
     * or with the showInterested function call.
     */
    void insertData(world_state& ws);

    ///Clear the current data and return what it stored. Thread safe.
    world_state getData();
};

//TODO Merge into standing query class, make standing query functions static
class QueryAccessor {
  private:
    ///Incoming data from solvers, disseminated by the pushThread function
    static boost::lockfree::queue<world_state> incoming_data;

    std::mutex subscription_mutex;
    static std::set<QueryAccessor*> subscriptions;

    /**
     * Need to remember the source list to remove the iterator
     * when this object is destroyed.
     */
    std::list<StandingQuery>* source;
    std::mutex* list_mutex;
    ///List iterators are safe to changes in the underlying list
    std::list<StandingQuery>::iterator data;

    //No copying or assignment. Deleting the copy constructor prevents passing
    //by value would cause trouble when the destructor removes this iterator
    //from the source list.
    QueryAccessor& operator=(const QueryAccessor&) = delete;
    QueryAccessor(const QueryAccessor&) = delete;

  public:
    /**
     * Gets updates and clears the current state
     * This should be thread safe in the StandingQuery class
     */
    StandingQuery::world_state getUpdates();

    /**
     * TODO Create a new standing query, adding itself to the list of
     * subscriptions.
     */
    QueryAccessor(const world_model::URI& uri,
        const std::vector<std::u16string>& desired_attributes, bool get_data = true);

    ///Remove this accessor from the subscription list
    ~QueryAccessor();

    ///r-value copy constructor
    QueryAccessor(QueryAccessor&& other);

    ///r-value assignment
    QueryAccessor& operator=(QueryAccessor&& other);

    /**
     * Function that moves incoming data to the queues of class instances
     * from the incoming data queue.
     */ 
    static void pushThread();

    /**
     * Expire the supplied URI at the given time.
     */
    static bool expireURI(world_model::URI uri, world_model::grail_time);

    /**
     * Expire the given URI's specified attributes at the given time.
     */
    static bool expireURIAttributes(world_model::URI uri,
        const std::vector<world_model::Attribute>& entries,
        world_model::grail_time);

    /**
     * Push new data from a solver to any interested standing queries.
     */
    static bool pushData(world_state& ws);
};

#endif //ifndef __STANDING_QUERY_HPP__


