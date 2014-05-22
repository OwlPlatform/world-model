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
 * An abstract class for data storage for the world model.
 * Supports adding data into and extracting data from the current state.
 * Also supports historic queries about the world model's state.
 * Uses condition variables to notify other threads when the world model is
 * updated.
 ******************************************************************************/

#ifndef __WORLD_MODEL_HPP__
#define __WORLD_MODEL_HPP__

#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include <sqlite3.h>

#include <owl/world_model_protocol.hpp>

#include "semaphore.hpp"
#include "standing_query.hpp"

///Representation of storage and search functionality for the world model
class WorldModel {
  public:
    typedef std::map<world_model::URI, std::vector<world_model::Attribute>> world_state;

  private:

    WorldModel& operator=(const WorldModel&) = delete;
    WorldModel(const WorldModel&) = delete;
    
  protected:
    //Do not store transient types in a database.
    //Recognize types by a unique attribute name and origin pair
    std::mutex transient_lock;
    std::set<std::pair<std::u16string, std::u16string>> transient;

    //The current state of the world model.
    world_state cur_state;

    //Read operations can be done simultaneously but a write operation requires
    //exclusive access to the current state map.
    Semaphore access_control;
    
  public:

    /*
     * Empty constructor and destructor -- inherited classes must
     * provide their own.
     * In derived class the constructor should take in a database name. If
     * the name is empty then no persistent storage should be used.
     */
    WorldModel() {};
    ///Destructor is virtual to ensure destructors of derived classes are called.
    virtual ~WorldModel();

    /*
     * Create a new URI in the world model. Returns true if the URI is created
     * and false if the URI could not be created because it already exists.
     * This call should not block. If inserting cannot be completed immediately
     * then it must be deferred to a thread so that this call can return.
     */
    virtual bool createURI(world_model::URI uri, std::u16string origin, world_model::grail_time creation) = 0;

    /*
     * Block access to the world model until this new information is added to it.
     * Data is not inserted into the world model if the given URI does not exist.
     * URIs must be created with the createURI function first.
     * If autocreate is set to true then this function will call createURI
     * automatically to create any URIs that do not alrady exist.
     * Returns true if the data is inserted, false otherwise.
     * This call should not block. If inserting cannot be completed within a
     * a reasonable time then it must be deferred to a thread so that this call
     * can return.
     */
    virtual bool insertData(std::vector<std::pair<world_model::URI, std::vector<world_model::Attribute>>> new_data, bool autocreate = false) = 0;

    /*
     * Set an expiration time for a URI or attribute.
     * Snapshots after the expiration time will not see expired URIs or attributes.
     * These URIs and attributes may appear again later if they are re-inserted
     * through a call to insertData.
     */
    virtual void expireURI(world_model::URI uri, world_model::grail_time) = 0;
    virtual void expireURIAttributes(world_model::URI uri, std::vector<world_model::Attribute>& entries, world_model::grail_time) = 0;

    /*
     * Delete a URI or URI attributes.
     * All data concerning the given URI or attributes is removed from the world model.
     * These URIs and attributes can be later recereated by subsequent
     * calls to the insertData function.
     */
    virtual void deleteURI(world_model::URI uri) = 0;
    virtual void deleteURIAttributes(world_model::URI uri, std::vector<world_model::Attribute> entries) = 0;

    /**
     * The URI search function returns any URIs in the world model
     * that match the provided regex URI.
     */
    std::vector<world_model::URI> searchURI(const std::u16string& regexp);

    /**
     * Get the current state of the world model.
     * Any number of read requests can be simultaneously serviced.
     * The provided URI is treated as a regex string and results for
     * any URIs that match will be returned.
     * The attributes variable indicates which attribute values should
     * be returned and the get_data variable indicates if the data fields
     * of those attributes should be filled in.
     * If no attributes are specified then all attributes are returned.
     */
    virtual world_state currentSnapshot(const world_model::URI& uri,
                                        std::vector<std::u16string>& desired_attributes,
                                        bool get_data = true);

    /**
     * Get the state of the world model after the data from the given time range.
     * Any number of read requests can be simultaneously serviced.
     */
    virtual world_state historicSnapshot(const world_model::URI& uri,
                                         std::vector<std::u16string> desired_attributes,
                                         world_model::grail_time start, world_model::grail_time stop) = 0;

    /**
     * Get stored data that occurs in a time range.
     * Any number of read requests can be simultaneously serviced.
     */
    virtual world_state historicDataInRange(const world_model::URI& uri,
                                            std::vector<std::u16string>& desired_attributes,
                                            world_model::grail_time start, world_model::grail_time stop) = 0;
    
    /**
     * Register an attribute name as a transient type. Transient types are not
     * permanently stored on disk but are retrieveable through currentSnapshot requests.
     */
    virtual void registerTransient(std::u16string& attr_name, std::u16string& origin);
    
    /**
     * When this request is called the query object is immediately populated.
     * Afterwards any updates that arrive that match the query criteria are
     * added into the standing query.
     */
    virtual StandingQuery requestStandingQuery(const world_model::URI& uri,
                                               std::vector<std::u16string>& desired_attributes,
                                               bool get_data = true);
};

#endif

