/*******************************************************************************
 * Data storage for the world model.
 * Supports adding data into and extracting data from the current state.
 * Also supports historic queries about the world model's state.
 * Uses condition variables to notify other threads when the world model is
 * updated.
 ******************************************************************************/

#ifndef __SQLITE3_WORLD_MODEL_HPP__
#define __SQLITE3_WORLD_MODEL_HPP__

#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>
#include <thread>

#include <sqlite3.h>

#include <semaphore.hpp>
#include <world_model.hpp>
#include <standing_query.hpp>

#include <owl/world_model_protocol.hpp>

///Implements abstract WorldModel class
class SQLite3WorldModel : public WorldModel {
  private:
    //Remember the number of insertions since the last analyze command was issued.
    //Occasionally the system should reanalyze the database.
    uint32_t inserts_since_analyze;

    //We need a semaphore so that writing threads don't get choked out.
    //Semaphore db_access_control;
    sqlite3 *db_handle;


    //Update expiration dates in the database.
    void databaseUpdate(world_model::URI uri, std::vector<world_model::Attribute>& entries);

    //Update the current table
    void currentUpdate(world_model::URI uri, std::vector<world_model::Attribute>& entries);

    //Store attributes in the database.
    void databaseStore(world_model::URI uri, std::vector<world_model::Attribute>& entries);

    //Issue a select request to the database
    world_state fetchWorldData(sqlite3_stmt* statement_p);

    SQLite3WorldModel& operator=(const SQLite3WorldModel&) = delete;
    SQLite3WorldModel(const SQLite3WorldModel&) = delete;

  public:

    /*
     * Create an instance of the world model and open the database
     * with the given database name. If the name is an empty string then
     * this world model will operate without storage.
     */
    SQLite3WorldModel(std::string db_name = "");
    ~SQLite3WorldModel();

    /*
     * Block access to the world model until this new URI is added.
     */
    bool createURI(world_model::URI uri, std::u16string origin, world_model::grail_time creation);

    /*
     * Block access to the world model until this new information is added to it
     * Data is not inserted into the world model if the given URI does not exist.
     * URIs must be created with the createURI function first.
     * If autocreate is set to true then this function will call createURI
     * automatically to create any URIs that do not alrady exist.
     */
    bool insertData(std::vector<std::pair<world_model::URI, std::vector<world_model::Attribute>>> new_data, bool autocreate = false);

    /*
     * Set an expiration time for a URI or attribute.
     * Snapshots after the expiration time will not see expired URIs or attributes.
     * These URIs and attributes may appear again later they are re-inserted
     * through a call to insertData.
     */
    void expireURI(world_model::URI uri, world_model::grail_time);
    void expireURIAttributes(world_model::URI uri, std::vector<world_model::Attribute>& entries, world_model::grail_time);

    /*
     * Delete a URI or URI attributes.
     * All data concerting the given URI or attributes is removed from the world model.
     * These URIs and attributes can be later recereated by subsequent
     * calls to the insertData function.
     */
    void deleteURI(world_model::URI uri);
    void deleteURIAttributes(world_model::URI uri, std::vector<world_model::Attribute> entries);

    /**
     * The URI search function returns any URIs in the world model
     * that match the provided GLOB URI.
     */
    std::vector<world_model::URI> searchURI(const std::u16string& glob);

    /**
     * Get the state of the world model after the data from the given time range.
     * Any number of read requests can be simultaneously serviced.
     */
    world_state historicSnapshot(const world_model::URI& uri,
                                 std::vector<std::u16string> desired_attributes,
                                 world_model::grail_time start, world_model::grail_time stop);

    /**
     * Get stored data that occurs in a time range.
     * Any number of read requests can be simultaneously serviced.
     */
    world_state historicDataInRange(const world_model::URI& uri,
                                    std::vector<std::u16string>& desired_attributes,
                                    world_model::grail_time start, world_model::grail_time stop);

};

#endif

