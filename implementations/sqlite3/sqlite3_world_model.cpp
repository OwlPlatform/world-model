/*******************************************************************************
 * Data storage for the world model.
 * Supports adding data into and extracting data from the current state.
 * Also supports historic queries about the world model's state.
 * Uses condition variables to notify other threads when the world model is
 * updated.
 ******************************************************************************/

#include <algorithm>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <semaphore.hpp>
#include "sqlite3_world_model.hpp"
#include "sqlite_regexp_module.hpp"

#include <owl/world_model_protocol.hpp>

//TODO In the future C++11 support for regex should be used over these POSIX
//regex c headers.
#include <sys/types.h>
#include <regex.h>

using namespace world_model;
using std::vector;
using std::u16string;

using world_model::WorldState;

#define DEBUG

struct Debug {
};
template<typename T>
Debug& operator<<(Debug& dbg, T arg) {
  //Only print if DEBUG was defined during compilation
#ifdef DEBUG
  std::cerr<<arg;
#endif
  return dbg;
}

Debug debug;

//Used to update the creation_date and expiration_date fields of uri attributes in the current db
void SQLite3WorldModel::currentUpdate(world_model::URI uri, std::vector<world_model::Attribute>& entries) {
  if (db_handle != NULL) {
    //SemaphoreLock lck(db_access_control);
    for (auto entry = entries.begin(); entry != entries.end(); ++entry) {
      std::string name(entry->name.begin(), entry->name.end());
      std::string origin(entry->origin.begin(), entry->origin.end());
      std::ostringstream insert_stream;

      insert_stream << "INSERT or REPLACE into 'current' "<<
        "(creation_date, expiration_date, uri, name, origin) values (?1, ?2, ?3, ?4, ?5);";
      sqlite3_stmt* statement_p;
      //Prepare the statement
      std::string insert_string = insert_stream.str();
      sqlite3_prepare_v2(db_handle, insert_string.c_str(), -1, &statement_p, NULL);
      //Bind this attribute's parameters.
      sqlite3_bind_int64(statement_p, 1, entry->creation_date);
      sqlite3_bind_int64(statement_p, 2, entry->expiration_date);
      sqlite3_bind_text16(statement_p, 3, uri.data(), 2*uri.size(), SQLITE_STATIC);
      sqlite3_bind_text16(statement_p, 4, entry->name.data(), 2*entry->name.size(), SQLITE_STATIC);
      sqlite3_bind_text16(statement_p, 5, entry->origin.data(), 2*entry->origin.size(), SQLITE_STATIC);

      //Call sqlite with the statement
      if (SQLITE_DONE != sqlite3_step(statement_p)) {
        //TODO This should be better at handling an error.
        std::cerr<<"Error updating field in database.\n";
      }

      //Delete the statement
      sqlite3_finalize(statement_p);
    }
  }
}

//Used to update the expiration_date field of uri attributes.
void SQLite3WorldModel::databaseUpdate(world_model::URI uri, std::vector<world_model::Attribute>& entries) {
  if (db_handle != NULL) {
    //SemaphoreLock lck(db_access_control);
    for (auto entry = entries.begin(); entry != entries.end(); ++entry) {
      std::string name(entry->name.begin(), entry->name.end());
      std::string origin(entry->origin.begin(), entry->origin.end());
      std::ostringstream insert_stream;

      insert_stream << "UPDATE 'attributes' SET expiration_date = ?1 WHERE "<<
       "uri = ?2 AND name = ?3 AND creation_date = ?4 AND origin = ?5;";
      sqlite3_stmt* statement_p;
      //Prepare the statement
      std::string insert_string = insert_stream.str();
      sqlite3_prepare_v2(db_handle, insert_string.c_str(), -1, &statement_p, NULL);
      //Bind this attribute's parameters.
      sqlite3_bind_int64(statement_p, 1, entry->expiration_date);
      sqlite3_bind_text16(statement_p, 2, uri.data(), 2*uri.size(), SQLITE_STATIC);
      sqlite3_bind_text16(statement_p, 3, entry->name.data(), 2*entry->name.size(), SQLITE_STATIC);
      sqlite3_bind_int64(statement_p, 4, entry->creation_date);
      sqlite3_bind_text16(statement_p, 5, entry->origin.data(), 2*entry->origin.size(), SQLITE_STATIC);

      //Call sqlite with the statement
      if (SQLITE_DONE != sqlite3_step(statement_p)) {
        //TODO This should be better at handling an error.
        std::cerr<<"Error updating field in database.\n";
      }

      //Delete the statement
      sqlite3_finalize(statement_p);
    }
  }
}

void SQLite3WorldModel::databaseStore(world_model::URI uri, std::vector<world_model::Attribute>& entries) {
  if (db_handle != NULL) {
    //SemaphoreLock lck(db_access_control);

    //Create a statement
    std::string statement_str = "INSERT OR IGNORE INTO 'attributes' VALUES (?1, ?2, ?3, ?4, ?5, ?6);";
    sqlite3_stmt* statement_p;
    //Prepare the statement
    sqlite3_prepare_v2(db_handle, statement_str.c_str(), -1, &statement_p, NULL);

    for (auto entry = entries.begin(); entry != entries.end(); ++entry) {
      //Increment the insertion count.
      ++inserts_since_analyze;
      //Bind this attribute's parameters
      sqlite3_bind_text16(statement_p, 1, uri.data(), 2*uri.size(), SQLITE_STATIC);
      sqlite3_bind_text16(statement_p, 2, entry->name.data(), 2*entry->name.size(), SQLITE_STATIC);
      sqlite3_bind_int64(statement_p, 3, entry->creation_date);
      sqlite3_bind_int64(statement_p, 4, entry->expiration_date);
      sqlite3_bind_text16(statement_p, 5, entry->origin.data(), 2*entry->origin.size(), SQLITE_STATIC);
      //The blob's memory is static during this transaction.
      //Otherwise it would be proper to use SQLITE_TRANSIENT to force sqlite to make a copy.
      sqlite3_bind_blob(statement_p, 6, entry->data.data(), entry->data.size(), SQLITE_STATIC);

      //Call sqlite with the statement
      if (SQLITE_DONE != sqlite3_step(statement_p)) {
        //TODO This should be better at handling an error.
        std::cerr<<"Error inserting field into database.\n";
      }
      //Ready the statement for its next use.
      sqlite3_clear_bindings(statement_p);
      sqlite3_reset(statement_p);
    }
    //Delete the statement
    sqlite3_finalize(statement_p);

    //If the number of inserts since the last analyze command is over 9000 then
    //reanalyze the attributes table to possibly reduce SELECT times.
    if (inserts_since_analyze > 9000) {
      //sqlite3_exec(db_handle, "ANALYZE attributes;", NULL, NULL, NULL);
      inserts_since_analyze = 0;
    }
  }
}

//Callback for a sqlite3 query.
int existCallback(void* found, int /*n_col*/, char** /*entries*/, char** /*col_names*/) {
  *(bool *)found = true;
  return 0;
}

SQLite3WorldModel::SQLite3WorldModel(std::string db_name) {
  if ("" == db_name) {
    db_handle = NULL;
    std::cerr<<"World model will operate without persistent storage.\n";
  }
  else {
    std::cerr<<"Opening sqlite3 database in filename '"<<db_name<<"' for data storage.\n";
    //Use the FULLMUTEX mode to open the database in serialized multi-threaded mode.
    int sql_succ = sqlite3_open_v2(db_name.c_str(), &db_handle,
        SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_SHAREDCACHE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    if (SQLITE_OK != sql_succ) {
      std::cerr<<"Error opening sqlite3 database: "<<sqlite3_errmsg(db_handle)<<'\n';
      sqlite3_close(db_handle);
      db_handle = NULL;
      std::cerr<<"World model will operate without persistent storage.\n";
    }
    sql_succ = initializeRegex(db_handle);
    if (SQLITE_OK != sql_succ) {
      std::cerr<<"Error opening using REGEX: "<<sqlite3_errmsg(db_handle)<<'\n';
      sqlite3_close(db_handle);
      db_handle = NULL;
      std::cerr<<"World model will operate without persistent storage.\n";
    }
    //Speed up database execution by turning off synchronous, increasing the cache size,
    //and changing the journal mode.
    //This makes the database less safe in the event of an OS crash but only by a
    //small amount and can give very large apparent speed improvements by allowing
    //function calls to return while the transaction is waiting to be written to disk.
    sqlite3_exec(db_handle, "PRAGMA synchronous = 0", NULL, 0, NULL);
    sqlite3_exec(db_handle, "PRAGMA cache_size = 10000", NULL, 0, NULL);
    sqlite3_exec(db_handle, "PRAGMA journal_mode = WAL", NULL, 0, NULL);
    //TODO Fall back to persist if write-ahead logging (WAL) is not supported
    //sqlite3_exec(db_handle, "PRAGMA journal_mode = PERSIST", NULL, 0, NULL);
    //sqlite3_exec(db_handle, "PRAGMA journal_mode = TRUNCATE", NULL, 0, NULL);

    //Check to see if the attributes table exists and create it if it does not.
    bool fresh_db = false;
    {
      //Check if the 'attributes' table exists.
      bool found = false;
      char* err;
      sqlite3_exec(db_handle, "SELECT name FROM sqlite_master WHERE type='table' AND name='attributes';",
          existCallback, &found, &err);
      if (NULL != err) {
        std::cerr<<"Error querying database: "<<err<<'\n';
        sqlite3_free(err);
        sqlite3_close(db_handle);
        db_handle = NULL;
        std::cerr<<"World model will operate without persistent storage.\n";
      }
      //Create an attributes table if one didn't exist
      if (not found) {
        fresh_db = true;
        sqlite3_exec(db_handle, "CREATE TABLE 'attributes' ('uri' TEXT, 'name' TEXT, creation_date INTEGER, expiration_date INTEGER, 'origin' TEXT, 'data' BLOB);",
            NULL, NULL, &err);
        if (NULL != err) {
          std::cerr<<"Error creating URIs table: "<<err<<'\n';
          sqlite3_free(err);
          sqlite3_close(db_handle);
          db_handle = NULL;
          std::cerr<<"World model will operate without persistent storage.\n";
        }
        else {
          //Create an index on times to make this table faster
          sqlite3_exec(db_handle, "create index create_expire ON attributes (creation_date, expiration_date);", NULL, NULL, &err);
          if (NULL != err) {
            std::cerr<<"Error creating index: "<<err<<'\n';
            sqlite3_free(err);
            sqlite3_close(db_handle);
            db_handle = NULL;
            std::cerr<<"World model will operate without persistent storage.\n";
          }
          //Create another index on the uri, name, and origin to speed up GROUP by requests
          else {
            sqlite3_exec(db_handle, "create index uri_name_orig_index ON attributes (uri, name, origin);", NULL, NULL, &err);
            if (NULL != err) {
              std::cerr<<"Error creating index: "<<err<<'\n';
              sqlite3_free(err);
              sqlite3_close(db_handle);
              db_handle = NULL;
              std::cerr<<"World model will operate without persistent storage.\n";
            }
          }
        }
      }
    }
    //Check to see if the current table exists and create it if it does not.
    {
      //Check if the 'current' table exists.
      bool found = false;
      char* err;
      sqlite3_exec(db_handle, "SELECT name FROM sqlite_master WHERE type='table' AND name='current';",
          existCallback, &found, &err);
      if (NULL != err) {
        std::cerr<<"Error querying database: "<<err<<'\n';
        sqlite3_free(err);
        sqlite3_close(db_handle);
        db_handle = NULL;
        std::cerr<<"World model will operate without persistent storage.\n";
      }
      //Create an attributes table if one didn't exist
      if (not found) {
        //Use uri, name, and origin as primary key. Don't store data in this table.
        sqlite3_exec(db_handle, "CREATE TABLE 'current' ('uri' TEXT not null, 'name' TEXT not null, creation_date INTEGER, expiration_date INTEGER, 'origin' TEXT not null, PRIMARY KEY('uri', 'name', 'origin'));",
            NULL, NULL, &err);
        if (NULL != err) {
          std::cerr<<"Error creating current table: "<<err<<'\n';
          sqlite3_free(err);
          sqlite3_close(db_handle);
          db_handle = NULL;
          std::cerr<<"World model will operate without persistent storage.\n";
        }
        //Check if we are updating from an old database. If so then we need to populate this
        //table.
        if (not fresh_db) {
          std::string request = std::string("INSERT INTO current (uri, name, creation_date, expiration_date, origin) ") +
            "SELECT uri, name, MAX(creation_date), expiration_date, origin FROM attributes GROUP BY uri, name, origin;";

          //Prepare the statement
          sqlite3_stmt* statement_p;
          sqlite3_prepare_v2(db_handle, request.c_str(), -1, &statement_p, NULL);
          while (SQLITE_ROW == sqlite3_step(statement_p)) {
          }
          //Delete the statement
          sqlite3_finalize(statement_p);
        }
      }
    }
  }
  //Analyze the database to remember aggregate statistics and speed up SELECTs
  if (db_handle != NULL) {
    sqlite3_exec(db_handle, "ANALYZE attributes;", NULL, NULL, NULL);
    inserts_since_analyze = 0;
  }
  //Load existing values using the current table.
  //Load existing values using the current table.
  {
    //Current request is around 1 million, the group by gets around 24000, the single request is around 4000
    //select uri, name, MAX(creation_date), expiration_date, origin, HEX(data)  from attributes where uri = "winlab.anchor.pipsqueak.receiver.161" GROUP BY name, origin;
    //select uri, name, MAX(creation_date), expiration_date, origin, HEX(data)  from attributes where uri = "winlab.anchor.pipsqueak.receiver.161" and name = "percent packets received.double";

    //First find all of the URIs and names we need to load
    std::string request = "select attributes.uri, attributes.name, attributes.creation_date, attributes.expiration_date, attributes.origin, attributes.data from current inner join attributes on (attributes.uri = current.uri and attributes.origin = current.origin and attributes.name = current.name and attributes.creation_date = current.creation_date and attributes.expiration_date = current.expiration_date);";
    //Prepare the statement
    sqlite3_stmt* statement_p;
    sqlite3_prepare_v2(db_handle, request.c_str(), -1, &statement_p, NULL);
    cur_state = fetchWorldData(statement_p);
  }
  //Set a timeout for slow operations
  if (db_handle != NULL) {
    //Time out after 30,000 ms (30 seconds)
    sqlite3_busy_timeout(db_handle, 30000);
  }
  //std::vector<std::u16string> all_attribs{u".*"};
  //cur_state = historicSnapshot(u".*", all_attribs, 0, MAX_GRAIL_TIME);
  std::cerr<<"World model loaded.\n";
}

SQLite3WorldModel::~SQLite3WorldModel() {
  if (NULL != db_handle) {
    sqlite3_close(db_handle);
  }
}

bool SQLite3WorldModel::createURI(world_model::URI uri,
                           std::u16string origin,
                           world_model::grail_time creation) {
  //Add a creation attribute to this URI to denote a creation time and the
  //origin of this URI.
  std::vector<world_model::Attribute> to_store{Attribute{u"creation", creation, 0, origin, {}}};
  //Lock the access control to get unique access to the world state.
  {
    SemaphoreLock lck(access_control);

    //The URI cannot be created twice - just return if this already exists.
    if (cur_state.find(uri) != cur_state.end()) {
      return false;
    }

    //Create this URI and push on a creation attribute
    cur_state[uri].push_back(to_store[0]);
  }

  //Put this URI into the database
  databaseStore(uri, to_store);
  //Also update the current table
  currentUpdate(uri, to_store);
  return true;
}

//Block access to the world model until this new information is added to it
bool SQLite3WorldModel::insertData(std::vector<std::pair<world_model::URI, std::vector<world_model::Attribute>>> new_data, bool autocreate) {
  //Handle the map first, then push data to the database.

  //Use these timers to check the memory, db, and standing query delays
  //auto time_start = world_model::getGRAILTime();

  //First check if there are any transient values here and process them separately
  std::map<URI, std::vector<world_model::Attribute>> transients;
  {
    std::unique_lock<std::mutex> lck(transient_lock);
    //Loop through the new data and remove transient attributes from new_data.
    //If all of a URI's updated attributes are transient then remove that URI as well
    auto I = new_data.begin();
    while (I != new_data.end()) {

      world_model::URI& uri = I->first;
      std::vector<world_model::Attribute>& entries = I->second;

      //Check if any entries match a transient type
      auto entry = entries.begin();
      while (entry != entries.end()) {
        //Do not process transient types normally.
        bool is_transient = 0 != transient.count(make_pair(entry->name, entry->origin));
        if (is_transient){
          transients[uri].push_back(*entry);
          entry = entries.erase(entry);
        }
        else {
          ++entry;
        }
      }
      //If all of this URI's attributes were transient then remove it from the vector
      if (I->second.empty()) {
        I = new_data.erase(I);
      }
      //Otherwise go to the next entry leaving this one in place
      else {
        ++I;
      }
    }
  }

  //Remember any attributes that need to be expired
  //and any updates to the current table caused by the
  //new data in this insert command.
  //Also set the expiration times of previously inserted
  //data and automatically create new URIs if autocreate
  //is specified.
  std::map<URI, vector<world_model::Attribute>> to_expire;
  std::map<URI, vector<world_model::Attribute>> current_update;
  //Remember new URIs so that they can be stored after
  //the locks are released
  std::vector<world_model::Attribute> to_store;
  for (auto I = new_data.begin(); I != new_data.end(); ++I) {
    world_model::URI& uri = I->first;
    std::vector<world_model::Attribute>& entries = I->second;

    if (not entries.empty()) {
      SemaphoreLock access_lock(access_control);

      //The URI cannot be created through this message unless
      //autocreate was set to true.
      if (cur_state.find(uri) == cur_state.end()) {
        //Make the URI if it doesn't exist and autocreate is specified
        if (autocreate) {
          world_model::Attribute creation_attr{u"creation", entries.front().creation_date, 0, entries.front().origin, {}};
          //Create this URI and push on a creation attribute
          cur_state[uri].push_back(creation_attr);
          //Remember this attribute and push it into the db once we have
          //released the locks so that we don't block other threads
          entries.push_back(creation_attr);
          current_update[uri].push_back(creation_attr);
        }
        else {
          //Don't insert anything from this URI
          entries.clear();
        }
      }

      //Get a reference to this URI's attributes for easy access
      std::vector<world_model::Attribute>& attributes = cur_state[uri];

      //Update the world model with each entry
      for (auto entry = entries.begin(); entry != entries.end(); ++entry) {
        //Check if there is already an entry with the same name and origin
        auto same_attribute = [&](Attribute& attr) {
          return (attr.name == entry->name) and (attr.origin == entry->origin);};
        auto slot = std::find_if(attributes.begin(), attributes.end(), same_attribute);
        //If no matching solution exists then just insert this new one.
        if (slot == attributes.end()) {
          attributes.push_back(*entry);
          //And update the current db as well
          current_update[uri].push_back(*entry);
        }
        else {
          //If this entry is newer than what is currently in the model update the model
          if (slot->creation_date < entry->creation_date) {
            //Remember the current slot and its expiration time
            slot->expiration_date = entry->creation_date;
            to_expire[uri].push_back(*slot);
            //Now overwrite the slot's value with the new entry
            *slot = *entry;
            //And update the current db as well
            current_update[uri].push_back(*slot);
          }
          else {
            //Check the database for the previous entry by creation date
            //Update that entry's expiration date to the new entry's creation date
            //and set the new entry's expiration to the other entry's expiration
            //Prepare the statement
            sqlite3_stmt* statement_p;
            std::string statement_str = std::string("SELECT uri, name, creation_date, ")+
              "expiration_date, origin, data FROM attributes WHERE creation_date <= ?1 "+
              "AND uri = ?2 AND name = ?3 AND origin = ?4 ORDER BY creation_date DESC limit 1;";
            sqlite3_prepare_v2(db_handle, statement_str.c_str(), -1, &statement_p, NULL);
            //Bind this attribute's parameters.
            sqlite3_bind_int64(statement_p, 1, entry->creation_date);
            sqlite3_bind_text16(statement_p, 2, uri.data(), 2*uri.size(), SQLITE_STATIC);
            sqlite3_bind_text16(statement_p, 3, entry->name.data(), 2*entry->name.size(), SQLITE_STATIC);
            sqlite3_bind_text16(statement_p, 4, entry->origin.data(), 2*entry->origin.size(), SQLITE_STATIC);
            world_state result = fetchWorldData(statement_p);

            //No result? then the expiration is equal to the earliest creation date of this attribute
            if (result.size() == 0) {
              std::string statement_str2 = std::string("SELECT uri, name, creation_date, ")+
                "expiration_date, origin, data FROM attributes WHERE creation_date >= ?1 "+
                "AND uri = ?2 AND name = ?3 AND origin = ?4 ORDER BY creation_date ASC limit 1;";
              sqlite3_prepare_v2(db_handle, statement_str2.c_str(), -1, &statement_p, NULL);
              sqlite3_bind_int64(statement_p, 1, entry->creation_date);
              sqlite3_bind_text16(statement_p, 2, uri.data(), 2*uri.size(), SQLITE_STATIC);
              sqlite3_bind_text16(statement_p, 3, entry->name.data(), 2*entry->name.size(), SQLITE_STATIC);
              sqlite3_bind_text16(statement_p, 4, entry->origin.data(), 2*entry->origin.size(), SQLITE_STATIC);
              world_state result = fetchWorldData(statement_p);
              if (result[uri].size() == 1) {
                entry->expiration_date = result[uri].front().creation_date;
              }
              //Shouldn't ever hit the else case (would imply the attribute
              //exists but isn't in the database)
            }
            else {
              //Update the entry and the attribute from the database
              entry->expiration_date = result[uri].front().expiration_date;
              result[uri].front().expiration_date = entry->creation_date;
              to_expire[uri].push_back(result[uri].front());
            }
          }
        }
      }
    }
  }
  //auto time_diff = world_model::getGRAILTime() - time_start;
  //std::cerr<<"Memory insertion time was "<<time_diff<<'\n';
  //time_start = world_model::getGRAILTime();

  //Store all of the entries that were not transient types
  sqlite3_exec(db_handle, "BEGIN TRANSACTION;", NULL, 0, NULL);

  for (auto I = new_data.begin(); I != new_data.end(); ++I) {
    if (not I->second.empty()) {
      databaseStore(I->first, I->second);
    }
  }
  //Update expiration times
  for (auto I = to_expire.begin(); I != to_expire.end(); ++I) {
    if (not I->second.empty()) {
      //Update values in the attributes database
      databaseUpdate(I->first, I->second);
    }
  }
  for (auto I = current_update.begin(); I != current_update.end(); ++I) {
    if (not I->first.empty()) {
      //Also update the current table
      currentUpdate(I->first, I->second);
    }
  }
  sqlite3_exec(db_handle, "COMMIT TRANSACTION;", NULL, 0, NULL);

  //time_diff = world_model::getGRAILTime() - time_start;
  //std::cerr<<"DB insertion time was "<<time_diff<<'\n';
  //time_start = world_model::getGRAILTime();

  auto push = [&](StandingQuery* sq) {
    //First see what items are of interest. This also tells the standing
    //query to remember partial matches so we do not need to keep feeding
    //it the current state, only the updates.
    auto ws = sq->showInterested(current_update);
    //Insert the data.
    if (not ws.empty()) {
      std::cerr<<"Inserting "<<ws.size()<<" entries for the standing query.\n";
      sq->insertData(ws);
    }
    //Insert transients separately from normal data to enforce exact string matching
    ws = sq->showInterestedTransient(transients);
    if (not ws.empty()) {
      std::cerr<<"Inserting "<<ws.size()<<" transient entries for the standing query.\n";
      sq->insertData(ws);
    }
  };
  StandingQuery::for_each(push);
  //TODO FIXME Shorter if StandingQuery::offerData handled transient data.
  //Send data to the standing queries
  //StandingQuery::offerData(current_update, false, false);
  //time_diff = world_model::getGRAILTime() - time_start;
  //std::cerr<<"Standing query insertion time was "<<time_diff<<'\n';

  return true;
}

void SQLite3WorldModel::expireURI(world_model::URI uri, world_model::grail_time expires) {
  //Update the creation field of this URI in the database to expire at the
  //given time and remove this entry from the live cur_state.
  std::vector<world_model::Attribute> to_expire;

  //Now remove the URI and its attributes from the current world model
  //Lock the access control to get unique access to the world state.
  {
    SemaphoreLock lck(access_control);
    //The URI cannot be created through this message
    if (cur_state.find(uri) == cur_state.end()) {
      return;
    }

    //Copy over all of the attributes and expire them, then remove this
    //uri from the in-memory world memory.
    for (auto I = cur_state[uri].begin(); I != cur_state[uri].end(); ++I) {
      I->expiration_date = expires;
      if (I->name == u"creation") {
        to_expire.push_back(*I);
      }
    }
    cur_state.erase(uri);
  }
  sqlite3_exec(db_handle, "BEGIN TRANSACTION;", NULL, 0, NULL);
  databaseUpdate(uri, to_expire);
  currentUpdate(uri, to_expire);
  sqlite3_exec(db_handle, "COMMIT TRANSACTION;", NULL, 0, NULL);

  //Offer a world state with the expiration date set to indicate expiration.
  WorldState changed_entry;
  changed_entry[uri] = to_expire;
  StandingQuery::offerData(changed_entry, false, true);
}

void SQLite3WorldModel::expireURIAttributes(world_model::URI uri, std::vector<world_model::Attribute>& entries, world_model::grail_time expires) {
  //Keep a vector of items that need to be placed in the database.
  //Handle this after releasing the access control lock on the world model map.
  std::vector<world_model::Attribute> to_update;

  //Lock the access control to get unique access to the world state.
  {
    SemaphoreLock lck(access_control);

    //Nothing to do if there isn't a URI
    if (cur_state.find(uri) == cur_state.end()) {
      return;
    }

    //Get a reference to this URI's attributes for easy access
    std::vector<world_model::Attribute>& attributes = cur_state[uri];

    //Update the world model with each entry
    for (auto entry = entries.begin(); entry != entries.end(); ++entry) {
      //Check if there is an entry that matches this one
      auto same_attribute = [&](Attribute& attr) {
        return (attr.name == entry->name) and
          (attr.origin == entry->origin) and
          (attr.creation_date == entry->creation_date);};
      auto slot = std::find_if(attributes.begin(), attributes.end(), same_attribute);
      //If a matching solution exists then update the database and erase this
      //from the current model.
      if (slot != attributes.end()) {
        slot->expiration_date = expires;
        to_update.push_back(*slot);
        attributes.erase(slot);
      }
    }
  }
  sqlite3_exec(db_handle, "BEGIN TRANSACTION;", NULL, 0, NULL);
  databaseUpdate(uri, to_update);
  currentUpdate(uri, to_update);
  sqlite3_exec(db_handle, "COMMIT TRANSACTION;", NULL, 0, NULL);

  //Offer a world state with the expiration date of attributes set to indicate
  //their expiration.
  WorldState changed_entry;
  changed_entry[uri] = entries;
  StandingQuery::offerData(changed_entry, true, false);
}

void SQLite3WorldModel::deleteURI(world_model::URI uri) {
  //Remove the URI and its attributes from the current world model
  //Lock the access control to get unique access to the world state.
  {
    SemaphoreLock lck(access_control);
    //The URI cannot be created through this message
    if (cur_state.find(uri) == cur_state.end()) {
      return;
    }

    //Delete this URI from the world model
    cur_state.erase(uri);
  }
  //Remove this URI from the database
  //If the database is not being used then just return here.
  if (db_handle == NULL) {
    return;
  }

  //SemaphoreLock lck(db_access_control);
  sqlite3_exec(db_handle, "BEGIN TRANSACTION;", NULL, 0, NULL);
  std::vector<std::string> db_names{"attributes", "current"};
  for (auto I = db_names.begin(); I != db_names.end(); ++I) {
    //Access the database for this information
    std::ostringstream request_stream;
    request_stream << "DELETE FROM "+(*I)+" WHERE uri = ?1;";
    //Prepare the statement
    sqlite3_stmt* statement_p;
    sqlite3_prepare_v2(db_handle, request_stream.str().c_str(), -1, &statement_p, NULL);
    //Bind this attribute's parameters.
    sqlite3_bind_text16(statement_p, 1, uri.data(), 2*uri.size(), SQLITE_STATIC);
    //for (int idx = 0; idx < desired_attributes.size(); ++idx) {
    //sqlite3_bind_text16(statement_p, 1+idx, desired_attributes[idx].data(), 2*desired_attributes[idx].size(), SQLITE_STATIC);
    //}
    //Execute the delete statement
    while (SQLITE_ROW == sqlite3_step(statement_p)) {;
    }
    //Delete the statement
    sqlite3_finalize(statement_p);
  }
  sqlite3_exec(db_handle, "COMMIT TRANSACTION;", NULL, 0, NULL);
  
  //Deletions are the same as expirations from the standing query's perspective
  //Offer a world state with the expiration date set to indicate expiration.
  WorldState changed_entry;
  world_model::Attribute expiration{u"creation", -1, -1, u"", {}};
  changed_entry[uri].push_back(expiration);
  StandingQuery::offerData(changed_entry, false, true);
}

void SQLite3WorldModel::deleteURIAttributes(world_model::URI uri, std::vector<world_model::Attribute> entries) {
  //Don't allow deleting the creation attribute.
  auto creation = std::find_if(entries.begin(), entries.end(),
      [&](Attribute& attr){ return attr.name == u"creation";});
  if (creation != entries.end()) {
    entries.erase(creation);
  }

  //If there is nothing to remove then return.
  if (entries.size() == 0) {
    return;
  }

  //Lock the access control to get unique access to the world state.
  //After cleaning up the world state remove these attributes from the database as well
  {
    SemaphoreLock lck(access_control);

    //Nothing to do if there isn't a matching URI
    if (cur_state.find(uri) == cur_state.end()) {
      return;
    }

    //Get a reference to this URI's attributes for easy access
    std::vector<world_model::Attribute>& attributes = cur_state[uri];

    //Update the world model with each entry
    for (auto entry = entries.begin(); entry != entries.end(); ++entry) {
      //Check if there is an entry that matches this one
      auto same_attribute = [&](Attribute& attr) {
        return (attr.name == entry->name) and (attr.origin == entry->origin);};
      auto slot = std::find_if(attributes.begin(), attributes.end(), same_attribute);
      if (slot != attributes.end()) {
        attributes.erase(slot);
      }
    }
  }
  //Delete these attributes from the database
  //If the database is not being used then just return here.
  if (db_handle == NULL) {
    return;
  }

  std::ostringstream request_stream;
  std::string att_request = "";
  if (entries.size() > 0 ) {
    att_request += " AND (";
  }
  for (int idx = 0; idx < entries.size(); ++idx) {
    if (idx == 0) {
      att_request += " (name = ?"+std::to_string(2*idx+2)+" AND origin = ?"+ std::to_string(2*idx+3)+") ";
    }
    else {
      att_request += " OR (name = ?"+std::to_string(2*idx+2)+" AND origin = ?"+ std::to_string(2*idx+3)+") ";
    }
  }
  if (entries.size() > 0 ) {
    att_request += ") ";
  }
  //SemaphoreLock lck(db_access_control);
  sqlite3_exec(db_handle, "BEGIN TRANSACTION;", NULL, 0, NULL);
  std::vector<std::string> db_names{"attributes", "current"};
  for (auto I = db_names.begin(); I != db_names.end(); ++I) {
    request_stream << "DELETE FROM "+(*I)+" WHERE uri = ?1" << att_request << ";";
    //std::cerr<<"Giving delete request: "<<request_stream.str()<<'\n';
    //Prepare the statement
    sqlite3_stmt* statement_p;
    sqlite3_prepare_v2(db_handle, request_stream.str().c_str(), -1, &statement_p, NULL);
    //Bind this attribute's parameters.
    sqlite3_bind_text16(statement_p, 1, uri.data(), 2*uri.size(), SQLITE_STATIC);
    for (int idx = 0; idx < entries.size(); ++idx) {
      sqlite3_bind_text16(statement_p, 2*idx + 2, entries[idx].name.data(), 2*entries[idx].name.size(), SQLITE_STATIC);
      sqlite3_bind_text16(statement_p, 2*idx + 3, entries[idx].origin.data(), 2*entries[idx].origin.size(), SQLITE_STATIC);
    }
    //Execute the delete statement
    while (SQLITE_ROW == sqlite3_step(statement_p)) {;
    }
    //Delete the statement
    sqlite3_finalize(statement_p);
  }
  sqlite3_exec(db_handle, "COMMIT TRANSACTION;", NULL, 0, NULL);
  
  //Deletions are the same as expirations from the standing query's perspective
  //Offer a world state with the expiration date of attributes set to indicate
  //their expiration.
  WorldState changed_entry;
  changed_entry[uri] = entries;
  StandingQuery::offerData(changed_entry, true, false);
}


//Uses the given SQL query and uri to fetch create a WorldData object and return it.
WorldModel::world_state SQLite3WorldModel::fetchWorldData(sqlite3_stmt* statement_p) {
  WorldModel::world_state ws;
  //SemaphoreFlag db_flag(db_access_control);
  
  //Call sqlite with the statement
  while (SQLITE_ROW == sqlite3_step(statement_p)) {
    //TODO This should be better at handling an error.
    //Each row is one or more fields of a uri's world data.
    u16string uri = u16string((const char16_t*)sqlite3_column_text16(statement_p, 0));
    //Ready this column and indicate that a URI was found
    //(this uri is in the map after this statement.).
    std::vector<world_model::Attribute>& cur_vec = ws[uri];
    uint32_t num_columns = sqlite3_column_count(statement_p);
    //Five columns per attribute requested.
    for (int cur_col = 1; cur_col < num_columns; cur_col += 5) {
      Attribute attr;
      attr.name = u16string((const char16_t*)sqlite3_column_text16(statement_p, cur_col));
      attr.creation_date = sqlite3_column_int64(statement_p, cur_col+1);
      attr.expiration_date = sqlite3_column_int64(statement_p, cur_col+2);
      attr.origin = u16string((const char16_t*)sqlite3_column_text16(statement_p, cur_col+3));
      //Data is always fetched
      //Pull out the data blob here - first get the size, then copy the bytes.
      int blob_size = sqlite3_column_bytes(statement_p, cur_col+4);
      attr.data = Buffer(blob_size);
      uint8_t* blob_p = (uint8_t*)sqlite3_column_blob(statement_p, cur_col+4);
      std::copy(blob_p, blob_p+blob_size, attr.data.begin());
      cur_vec.push_back(attr);
    }
  }

  //Delete the statement
  sqlite3_finalize(statement_p);
  return ws;
}

/**
 * Get the state of the world model after the data from the given time range.
 * Any number of read requests can be simultaneously serviced.
 */
WorldModel::world_state SQLite3WorldModel::historicSnapshot(const world_model::URI& uri,
                                    std::vector<std::u16string> desired_attributes,
                                    world_model::grail_time start, world_model::grail_time stop) {
  //Return an empty result if there is no database access
  if (db_handle == NULL or desired_attributes.empty()) {
    return WorldModel::world_state();
  }

  //Combine all of the requests into a single regular expression to speed up the search.
  std::u16string single_expression = u"(" + desired_attributes[0];
  for (auto I = desired_attributes.begin()+1; I != desired_attributes.end(); ++I) {
    single_expression += u"|" + *I;
  }
  single_expression += u")";

  //Access the database for this information
  //Start assembling the query
  //TODO FIXME There are a lot of different kinds of requests here but all have
  //about the same results. DB access is still distressingly slow, but probably
  //for some reason other than access time so we can probably clean this up.
  std::ostringstream request_stream;
  //In this request ?1 is the start time, ?2 is the end time, ?3 is the URI and
  //all other SQLITE variables are attribute name expressions
  //request_stream << "SELECT A2.uri, A2.name, MAX(A2.creation_date), A2.expiration_date, A2.origin, A2.data "<<
    //"FROM attributes as A2, attributes as A1 WHERE A1.creation_date < ?2 AND A1.name = 'creation' "<<
    //"AND (A1.expiration_date == 0 OR A1.expiration_date > ?2) AND A1.uri REGEXP ?3 AND A2.uri = A1.uri "<<
    //"AND A2.creation_date BETWEEN ?1 and ?2 AND (A2.expiration_date == 0 OR A2.expiration_date > ?2) AND "<<
    //"A2.name REGEXP ?4 GROUP BY A2.uri, A2.name, A2.origin;";
    //
  //request_stream << "SELECT A2.uri, A2.name, MAX(A2.creation_date), A2.expiration_date, A2.origin, A2.data "<<
    //"FROM attributes as A2, attributes as A1 WHERE A1.creation_date < ?2 AND A1.name = 'creation' "<<
    //"AND NOT (A1.expiration_date BETWEEN 1 and ?2) AND A1.uri REGEXP ?3 AND A2.uri = A1.uri "<<
    //"AND A2.creation_date BETWEEN ?1 and ?2 AND NOT (A2.expiration_date BETWEEN 1 and ?2) AND "<<
    //"A2.name REGEXP ?4 GROUP BY A2.uri, A2.name, A2.origin;";

  //This is a bit faster than the self join. This is safe to use if we expire all of a URI's
  //attributes when the URI is expired.
  request_stream << "SELECT uri, name, MAX(creation_date), expiration_date, origin, data "<<
    "FROM attributes WHERE creation_date <= ?2 AND NOT (expiration_date BETWEEN 1 AND ?2) "<<
    "AND uri REGEXP ?3 AND name REGEXP ?4 GROUP BY uri, name, origin;";

  //request_stream << "SELECT uri, name, MAX(creation_date), expiration_date, origin, data FROM "<<
    //"( select * from attributes where uri REGEXP ?3 AND name REGEXP ?4) where NOT expiration_date BETWEEN 1 AND ?2 "<<
    //"GROUP BY uri, name, origin;";

  //Another alternative is to use the current table to determine which triples of uri,name, and origin
  //should be fetched and then fetch them individually.
  //request_stream << "select attributes.uri, attributes.name, MAX(attributes.creation_date), attributes.expiration_date, attributes.origin, attributes.data from attributes inner join current on (current.uri = attributes.uri AND current.name = attributes.name AND current.origin = attributes.origin) where attributes.creation_date <= ?2 and (attributes.expiration_date == 0 OR attributes.expiration_date > ?2) and current.uri REGEXP ?3 and current.name REGEXP ?4 GROUP BY current.uri, current.name, current.origin;";
  //request_stream << "select attributes.uri, attributes.name, MAX(attributes.creation_date), attributes.expiration_date, attributes.origin, attributes.data from attributes inner join current on (current.uri = attributes.uri AND current.name = attributes.name AND current.origin = attributes.origin) where attributes.creation_date <= ?2 and NOT (attributes.expiration_date BETWEEN 1 and ?2) and current.uri REGEXP ?3 and current.name REGEXP ?4 GROUP BY current.uri, current.name, current.origin;";

  //Filter first on times and then filter again on names
  //request_stream << "select * from ( SELECT uri, name, MAX(creation_date), expiration_date, origin, data from attributes where creation_date <= ?2 and NOT (expiration_date BETWEEN 1 and ?2) GROUP BY uri, name, origin) A WHERE uri REGEXP ?3 and name REGEXP ?4;";

  //request_stream << "select attributes.uri, attributes.name, MAX(attributes.creation_date), attributes.expiration_date, attributes.origin, attributes.data from attributes inner join current on ( SELECT uri, name, origin from current where current.uri REGEXP ?3 and current.name REGEXP ?4) A where (A.uri = attributes.uri AND A.name = attributes.name AND A.origin = attributes.origin) where attributes.creation_date <= ?2 and (attributes.expiration_date == 0 OR attributes.expiration_date > ?2);";

  //std::cerr<<"Historic request is:\n"<<request_stream.str()<<'\n';
  //Prepare the statement
  sqlite3_stmt* statement_p;
  sqlite3_prepare_v2(db_handle, request_stream.str().c_str(), -1, &statement_p, NULL);
  //Bind this attribute's parameters.
  sqlite3_bind_int64(statement_p, 1, start);
  sqlite3_bind_int64(statement_p, 2, stop);
  sqlite3_bind_text16(statement_p, 3, uri.data(), 2*uri.size(), SQLITE_STATIC);
  sqlite3_bind_text16(statement_p, 4, single_expression.data(), 2*single_expression.size(), SQLITE_STATIC);
  world_state result = fetchWorldData(statement_p);

  //Check the returned URIs to make sure they satisfy all of the attribute requirements
  for (auto I = desired_attributes.begin(); I != desired_attributes.end(); ++I) {
    regex_t exp;
    int err = regcomp(&exp, std::string(I->begin(), I->end()).c_str(), REG_EXTENDED);
    if (0 != err) {
      debug<<"Error compiling regular expression "<<std::string(I->begin(), I->end())<<" in historic snapshot request.\n";
    }
    else {
      auto attr_match = [&](const world_model::Attribute& attr) {
        regmatch_t pmatch;
        int match = regexec(&exp, std::string(attr.name.begin(), attr.name.end()).c_str(), 1, &pmatch, 0);
        if (0 == match and 0 == pmatch.rm_so and attr.name.size() == pmatch.rm_eo) {
          return true;
        }
        return false;
      };
      auto URI = result.begin();
      while (URI != result.end()) {
        //If none of this URI's attributes match this expression then drop this URI
        if (std::none_of(URI->second.begin(), URI->second.end(), attr_match)) {
          URI = result.erase(URI);
        }
        else {
          ++URI;
        }
      }
      regfree(&exp);
    }
  }
  return result;
}


/**
 * Get stored data that occurs in a time range.
 * Any number of read requests can be simultaneously serviced.
 */
WorldModel::world_state SQLite3WorldModel::historicDataInRange(const world_model::URI& uri,
                                    std::vector<std::u16string>& desired_attributes,
                                    world_model::grail_time start, world_model::grail_time stop) {
  //Return an empty result if there is no database access
  if (db_handle == NULL) {
    return WorldModel::world_state();
  }
  //Access the database for this information
  std::ostringstream request_stream;
  std::string att_request = "";

  if (desired_attributes.size() > 0 ) {
    att_request += " AND (";
  }
  for (int idx = 0; idx < desired_attributes.size(); ++idx) {
    if (idx == 0) {
      att_request += " (name REGEXP ?"+std::to_string(idx+4)+") ";
    }
    else {
      att_request += " OR (name REGEXP ?"+std::to_string(idx+4)+") ";
    }
  }
  if (desired_attributes.size() > 0 ) {
    att_request += ") ";
  }
  request_stream << "SELECT * from attributes WHERE uri REGEXP ?1 "<<att_request<<
    " AND creation_date BETWEEN ?2 AND ?3 order by creation_date asc;";
  //Prepare the statement
  sqlite3_stmt* statement_p;
  sqlite3_prepare_v2(db_handle, request_stream.str().c_str(), -1, &statement_p, NULL);
  //Bind this attribute's parameters.
  sqlite3_bind_text16(statement_p, 1, uri.data(), 2*uri.size(), SQLITE_STATIC);
  sqlite3_bind_int64(statement_p, 2, start);
  sqlite3_bind_int64(statement_p, 3, stop);
  for (int idx = 0; idx < desired_attributes.size(); ++idx) {
    sqlite3_bind_text16(statement_p, 4+idx, desired_attributes[idx].data(), 2*desired_attributes[idx].size(), SQLITE_STATIC);
  }

  WorldModel::world_state result = fetchWorldData(statement_p);
  return result;

  /*
  //Now iterate through the attributes of each URI to enfore the
  //AND relationship in the request.
  //First build the expressions
  std::vector<regex_t> expressions;
  for (auto exp_str = desired_attributes.begin(); exp_str != desired_attributes.end(); ++exp_str) {
    regex_t exp;
    int err = regcomp(&exp, std::string(exp_str->begin(), exp_str->end()).c_str(), REG_EXTENDED);
    if (0 != err) {
      debug<<"Error compiling regular expression in attribute of window request.\n";
    }
    else {
      expressions.push_back(exp);
    }
  }
  WorldModel::world_state anded_result;
  for (auto URI = result.begin(); URI != result.end(); ++URI) {
    std::vector<Attribute> anded_attributes;
    std::vector<Attribute>& these_attributes = URI->second;
    std::vector<std::vector<Attribute>::iterator> cur_slots(desired_attributes.size(), these_attributes.end());
    auto in_range = [&](const std::vector<Attribute>::iterator& a) { return a != these_attributes.end();};
    auto add_if_new = [&](std::vector<Attribute>::iterator& a) {
      auto end = anded_attributes.rbegin();
      if (end == anded_attributes.rend() or
          (a->name != end->name or a->creation_date != end->creation_date or
          a->origin != end->origin)) {
        anded_attributes.push_back(*a);
      }};
    //Sweep through in a single pass removing stranded attributes
    //Always advance to the attribute with the smallest
    //creation date (they are happily already in that order).
    for (auto attr = these_attributes.begin(); attr != these_attributes.end(); ++attr) {
      for (size_t search_ind = 0; search_ind < expressions.size(); ++search_ind) {
        regmatch_t pmatch;
        int match = regexec(&expressions[search_ind], std::string(attr->name.begin(), attr->name.end()).c_str(), 1, &pmatch, 0);
        if (0 == match and 0 == pmatch.rm_so and attr->name.size() == pmatch.rm_eo) {
          //Now remove any expired attributes (expiration < attr->creation)
          for (size_t clear = 0; clear < cur_slots.size(); ++clear) {
            if (in_range(cur_slots[clear]) and
                0 != cur_slots[clear]->expiration_date and
                cur_slots[clear]->expiration_date < attr->creation_date) {
              cur_slots[clear] == these_attributes.end();
            }
          }
          bool prev_all_of = std::all_of(cur_slots.begin(), cur_slots.end(), in_range);
          //Finally go to the next position and add this attribute if all are active
          cur_slots[search_ind] = attr;
          if (std::all_of(cur_slots.begin(), cur_slots.end(), in_range)) {
            //TODO It is possible that attributes can be added multiple times from this
            //Add in the previous slots because previously everything wasn't active.
            if (not prev_all_of) {
              std::for_each(cur_slots.begin(), cur_slots.end(), add_if_new);
            }
            else {
              add_if_new(cur_slots[search_ind]);
            }
          }
        }
      }
    }
    //Check everything left in the cur_slots.
    if (std::all_of(cur_slots.begin(), cur_slots.end(), in_range)) {
      std::for_each(cur_slots.begin(), cur_slots.end(), add_if_new);
    }

    if (not anded_attributes.empty()) {
      anded_result[URI->first] = anded_attributes;
    }
  }
  //Free the memory used in the regex
  std::for_each(expressions.begin(), expressions.end(), [&](regex_t& exp) { regfree(&exp);});
  return anded_result;
  */
}


