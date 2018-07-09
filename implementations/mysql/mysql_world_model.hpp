/*******************************************************************************
 * Data storage for the world model using mysql.
 * Supports adding data into and extracting data from the current state.
 * Also supports historic queries about the world model's state.
 * Uses condition variables to notify other threads when the world model is
 * updated.
 ******************************************************************************/

#ifndef __MYSQL_WORLD_MODEL_HPP__
#define __MYSQL_WORLD_MODEL_HPP__

#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

#include "task_pool.hpp"

#include <mysql/mysql.h>

#include <semaphore.hpp>
#include <world_model.hpp>
#include <standing_query.hpp>

#include <owl/world_model_protocol.hpp>

///Implements abstract WorldModel class
class MysqlWorldModel : public WorldModel {
  private:
    MYSQL* db_handle;

    //User name and password to connect to the mysql server
    std::string db_name;
    std::string user;
    std::string password;

    //Update expiration dates in the database.
    WorldModel::world_state databaseUpdate(world_model::URI& uri,
        std::vector<world_model::Attribute>& to_update, MYSQL* handle);

    //Store attributes in the database.
    WorldModel::world_state databaseStore(world_model::URI& uri,
        std::vector<world_model::Attribute>& entries, MYSQL* handle);

    //Issue a select request to the database
    world_state fetchWorldData(MYSQL_STMT* stmt, MYSQL* handle);

    MysqlWorldModel& operator=(const MysqlWorldModel&) = delete;
    MysqlWorldModel(const MysqlWorldModel&) = delete;

    //Convenience function to get a free connection to the mysql server
    MYSQL* getConnection();

    //Convenience function to return a connection to the mysql server to the connection pool
    void returnConnection(MYSQL* conn);

    /**
     * These functions shadow the similarly public named functions.
     * The reason for this is to provide function to bind so that
     * these can be executed inside of threads from the thread pool
     */
    WorldModel::world_state _deleteURI(world_model::URI& uri, MYSQL* handle);
    WorldModel::world_state _deleteURIAttributes(world_model::URI& uri,
        std::vector<world_model::Attribute>& entries, MYSQL* handle);

    WorldModel::world_state _historicSnapshot(const world_model::URI& uri,
        std::vector<std::u16string>& desired_attributes,
        world_model::grail_time start, world_model::grail_time stop, MYSQL* handle);

    WorldModel::world_state _historicDataInRange(const world_model::URI& uri,
        std::vector<std::u16string>& desired_attributes,
        world_model::grail_time start, world_model::grail_time stop, MYSQL* handle);

  public:

    static void setupMySQL(std::string directory, MYSQL* db_handle);

    /*
     * Create an instance of the world model and open the database
     * with the given database name logging in with the given user
     * name and password.
     */
    MysqlWorldModel(std::string db_name, std::string user, std::string password);
    ~MysqlWorldModel();

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

