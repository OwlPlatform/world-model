/*******************************************************************************
 * Data storage for the world model using a mysql database.
 * Supports adding data into and extracting data from the current state.
 * Also supports historic queries about the world model's state.
 * Uses condition variables to notify other threads when the world model is
 * updated.
 ******************************************************************************/

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <vector>
#include <string.h>
#include <thread>
#include <future>
#include <functional>

#include "task_pool.hpp"
#include "mysql_world_model.hpp"
#include <semaphore.hpp>

#include <mysql/mysql.h>
#include <owl/world_model_protocol.hpp>


//TODO In the future C++11 support for regex should be used over these POSIX
//regex c headers.
#include <sys/types.h>
#include <regex.h>

using namespace world_model;
using std::vector;
using std::u16string;

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

//Make sure that the compiler places this instantiation into the object file.
template class QueryThread<WorldModel::world_state>;

//Search for URIs in the world model using a glob expression
std::vector<world_model::URI> MysqlWorldModel::searchURI(const std::u16string& glob) {
  //debug<<"Searching for "<<std::string(glob.begin(), glob.end())<<'\n';
  std::vector<world_model::URI> result;
  //Build a regular expression from the glob and search for matches in the
  //keys of the world_state map.
  regex_t exp;
  int err = regcomp(&exp, std::string(glob.begin(), glob.end()).c_str(), REG_EXTENDED);
  //Return no results if the expression did not compile.
  //TODO Should indicate error but throwing an exception might be overboard.
  if (0 != err) {
    std::cerr<<"Error compiling regular expression: "<<std::string(glob.begin(), glob.end())<<".\n";
    return result;
  }

  //Flag the access control so that this read does not conflict with a write.
  SemaphoreFlag flag(access_control);

  //Check for a matchs in the URIs and remember any URIs that match
  for (auto I = cur_state.begin(); I != cur_state.end(); ++I) {
    //Check each match to make sure it consumes the whole string
    regmatch_t pmatch;
    int match = regexec(&exp, std::string(I->first.begin(), I->first.end()).c_str(), 1, &pmatch, 0);
    if (0 == match and 0 == pmatch.rm_so and I->first.size() == pmatch.rm_eo) {
      //debug<<"Matched "<<std::string(I->first.begin(), I->first.end())<<'\n';
      result.push_back(I->first);
    }
  }
  regfree(&exp);
  return result;
}

//Used to update the expiration_date field of uri attributes.
WorldModel::world_state MysqlWorldModel::databaseUpdate(world_model::URI& uri,
    std::vector<world_model::Attribute>& to_update, MYSQL* handle) {
  //Return if we cannot get a connection
  if (nullptr == handle) {
    std::cerr<<"Cannot update expiration times -- connection is null\n";
    WorldModel::world_state();
  }
  WorldModel::world_state expired;
  //Call expireUri if there if the whole URI is to be expired, otherwise call expireAttribute
  //CREATE PROCEDURE expireUri(pUri VARCHAR(170) CHARACTER SET UTF16 COLLATE utf16_unicode_ci,
                             //pOrigin VARCHAR(170) CHARACTER SET UTF16 COLLATE utf16_unicode_ci,
                             //pTimestamp BIGINT)
  //CREATE PROCEDURE expireAttribute(pUri VARCHAR(170) CHARACTER SET UTF16 COLLATE utf16_unicode_ci,
                             //pAttribute VARCHAR(170) CHARACTER SET UTF16 COLLATE utf16_unicode_ci,
                             //pOrigin VARCHAR(170) CHARACTER SET UTF16 COLLATE utf16_unicode_ci,
                             //pTimestamp BIGINT)
  if (to_update.size() == 1 and to_update[0].name == u"creation") {
    std::string statement_str = "CALL expireUri(?, ?);";
    MYSQL_STMT* statement_p = mysql_stmt_init(handle);
    if (nullptr == statement_p) {
      //TODO This should be better at handling an error.
      std::cerr<<"Error creating statement expireUri.\n";
    }
    if (mysql_stmt_prepare(statement_p, statement_str.c_str(), statement_str.size())) {
      std::cerr<<"Failed to prepare statement: "<<statement_str<<": "<<mysql_error(handle)<<'\n';
      return WorldModel::world_state();
    }
    MYSQL_BIND parameters[3];
    memset(parameters, 0, sizeof(parameters));
    std::string char8_uri(uri.begin(), uri.end());
    parameters[0].buffer_type = MYSQL_TYPE_STRING;
    parameters[0].buffer = (void*)char8_uri.data();
    unsigned long uri_len = char8_uri.size();
    parameters[0].length = &uri_len;
    parameters[1].buffer_type = MYSQL_TYPE_LONGLONG;
    parameters[1].is_unsigned = false;
    int64_t expires = to_update[0].expiration_date;
    parameters[1].buffer = &(expires);

    WorldModel::world_state expired;
    if (0 != mysql_stmt_bind_param(statement_p, parameters)) {
      parameters[1].buffer = (void*)uri.data();
      //TODO This should be better at handling an error.
      std::cerr<<"Error binding variables for expireUri.\n";
    }
    else {
      //Execute the statement
      if (0 != mysql_stmt_execute(statement_p)) {
        std::cerr<<"Error executing statement for expireUri: "<<mysql_error(handle)<<"\n";
      }
      else {
        //Record which attributes are successfully expired
        expired[uri].push_back(to_update[0]);
      }
    }
    //Delete the statement
    mysql_stmt_close(statement_p);
  }
  else {
    std::string statement_str = "CALL expireAttribute(?, ?, ?, ?);";
    MYSQL_STMT* statement_p = mysql_stmt_init(handle);
    if (nullptr == statement_p) {
      //TODO This should be better at handling an error.
      std::cerr<<"Error creating statement expireAttribute.\n";
    }
    if (mysql_stmt_prepare(statement_p, statement_str.c_str(), statement_str.size())) {
      std::cerr<<"Failed to prepare statement: "<<statement_str<<": "<<mysql_error(handle)<<'\n';
      return WorldModel::world_state();
    }
    MYSQL_BIND parameters[4];
    memset(parameters, 0, sizeof(parameters));
    std::string char8_uri(uri.begin(), uri.end());
    parameters[0].buffer_type = MYSQL_TYPE_STRING;
    parameters[0].buffer = (void*)char8_uri.data();
    unsigned long uri_len = char8_uri.size();
    parameters[0].length = &uri_len;
    parameters[1].buffer_type = MYSQL_TYPE_STRING;
    unsigned long attr_len;
    parameters[1].length = &attr_len;
    parameters[2].buffer_type = MYSQL_TYPE_STRING;
    unsigned long origin_len;
    parameters[2].length = &origin_len;
    parameters[3].buffer_type = MYSQL_TYPE_LONGLONG;
    parameters[3].is_unsigned = false;

    //Expire every matching entry
    for (auto entry = to_update.begin(); entry != to_update.end(); ++entry) {
      //parameters[1].buffer = (void*)entry->name.data();
      //attr_len = entry->name.size()*2;
      std::string char8_name(entry->name.begin(), entry->name.end());
      parameters[1].buffer = (void*)char8_name.data();
      attr_len = char8_name.size();
      std::string char8_origin(entry->origin.begin(), entry->origin.end());
      parameters[2].buffer = (void*)char8_origin.data();
      origin_len = char8_origin.size();
      int64_t expires = entry->expiration_date;
      parameters[3].buffer = &(expires);
      if (0 != mysql_stmt_bind_param(statement_p, parameters)) {
        parameters[1].buffer = (void*)uri.data();
        //TODO This should be better at handling an error.
        std::cerr<<"Error binding variables for expireAttribute.\n";
      }
      else {
        //Execute the statement
        if (0 != mysql_stmt_execute(statement_p)) {
          std::cerr<<"Error executing statement for expireAttribute: "<<mysql_error(handle)<<"\n";
        }
        else {
          //Record which attributes are successfully expired
          expired[uri].push_back(*entry);
        }
      }
      mysql_stmt_reset(statement_p);
    }
    //Delete the statement
    mysql_stmt_close(statement_p);
  }
  //Return the attributes that were successfully expired
  return expired;
}

WorldModel::world_state MysqlWorldModel::databaseStore(world_model::URI& uri, std::vector<world_model::Attribute>& entries, MYSQL* handle) {
  WorldModel::world_state stored;
  //Return if we cannot get a connection
  if (nullptr == handle) {
    std::cerr<<"Cannot call updateAttribute: given a null connection.\n";
    return stored;
  }
  //SemaphoreLock lck(db_access_control);
  //auto from_u16 = [&](std::u16string& str) { return std::string(str.begin(), str.end());};

  //Create a statement
  //std::string statement_str = "INSERT OR IGNORE INTO 'attributes' VALUES (?1, ?2, ?3, ?4, ?5, ?6);";
  std::string statement_str = "CALL updateAttribute(?, ?, ?, ?, ?);";
  MYSQL_STMT* statement_p = mysql_stmt_init(handle);
  if (nullptr == statement_p) {
    //TODO This should be better at handling an error.
    std::cerr<<"Error creating statement for database storage.\n";
  }
  if (mysql_stmt_prepare(statement_p, statement_str.c_str(), statement_str.size())) {
    std::cerr<<"Failed to prepare statement: "<<statement_str<<": "<<mysql_error(handle)<<'\n';
    mysql_stmt_close(statement_p);
    return stored;
  }
  MYSQL_BIND parameters[5];
  memset(parameters, 0, sizeof(parameters));
  std::string char8_uri(uri.begin(), uri.end());
  parameters[0].buffer_type = MYSQL_TYPE_STRING;
  parameters[0].buffer = (void*)char8_uri.data();
  unsigned long uri_len = char8_uri.size();
  parameters[0].length = &uri_len;
  parameters[0].is_unsigned = true;
  parameters[1].buffer_type = MYSQL_TYPE_STRING;
  parameters[1].is_unsigned = true;
  parameters[2].buffer_type = MYSQL_TYPE_STRING;
  parameters[2].is_unsigned = true;
  parameters[3].buffer_type = MYSQL_TYPE_BLOB;
  parameters[3].is_unsigned = true;
  parameters[4].buffer_type = MYSQL_TYPE_LONGLONG;
  parameters[4].is_unsigned = false;
  //Set the parameter structure (uri, attribute, origin, data, timestamp)
  for (auto entry : entries) {
    //Bind this attribute's parameters
    //Name
    std::string char8_name(entry.name.begin(), entry.name.end());
    parameters[1].buffer = (void*)char8_name.data();
    unsigned long name_len = char8_name.size();
    parameters[1].length = &name_len;
    //Origin
    std::string char8_origin(entry.origin.begin(), entry.origin.end());
    parameters[2].buffer = (void*)char8_origin.data();
    unsigned long origin_len = char8_origin.size();
    parameters[2].length = &origin_len;
    //Data
    parameters[3].buffer = entry.data.data();
    unsigned long data_len = entry.data.size();
    parameters[3].length = &data_len;
    //Timestamp
    parameters[4].buffer = &(entry.creation_date);

    if (0 != mysql_stmt_bind_param(statement_p, parameters)) {
      //TODO This should be better at handling an error.
      std::cerr<<"Error binding variables for data insertion.\n";
      continue;
    }

    //Execute the statement
    if (0 != mysql_stmt_execute(statement_p)) {
      std::cerr<<"Error executing statement for data insertion: "<<mysql_error(handle)<<"\n";
    }
    else {
      stored[uri].push_back(entry);
    }
    mysql_stmt_reset(statement_p);
  }
  //Delete the statement
  mysql_stmt_close(statement_p);
  //Return which attributes were successfully stored
  return stored;
}

void MysqlWorldModel::setupMySQL(std::string directory, MYSQL* db_handle) {
  if (directory.empty()) {
    directory = "./";
  }
  //Add a trailing /
  if (directory.back() != '/') {
    directory.push_back('/');
  }
  std::vector<std::string> tables{"AttributeValues.mysql", "Attributes.mysql",
                             "Origins.mysql", "Uris.mysql"};
  for (std::string& tname : tables) {
    if (nullptr == db_handle) { return; }
    std::string fname = directory + "table/" + tname;
    std::ifstream file(fname.c_str());
    std::string cmd;
    //Read the whole file into cmd
    std::getline(file, cmd, '\0');
    //std::cerr<<"Executing commands in file "<<fname<<'\n';
    //Execute the commands in the file
    if (mysql_real_query(db_handle, cmd.c_str(), cmd.size())) {
      std::cerr<<"Error executing commands in file "<<fname<<": "<<mysql_error(db_handle)<<"\n";
      mysql_close(db_handle);
      db_handle = nullptr;
    }
    else {
      int status = 0;
      do {
        MYSQL_RES* result = mysql_store_result(db_handle);
        if (nullptr != result) {
          mysql_free_result(result);
        }
        status = mysql_next_result(db_handle);
      } while (0 == status);
      if (0 < status) {
        std::cerr<<"Error executing commands in file "<<fname<<": "<<mysql_error(db_handle)<<"\n";
        mysql_close(db_handle);
        db_handle = nullptr;
      }
    }
  }
  std::vector<std::string> procs{"deleteAttribute.mysql", "deleteUri.mysql", "expireAttribute.mysql",
    "expireUri.mysql", "getCurrentValue.mysql", "getCurrentValueId.mysql",
    "getIdValueBefore.mysql", "getRangeValues.mysql", "getSnapshotValue.mysql", "getTimestampAfter.mysql",
    "searchAttribute.mysql", "searchOrigin.mysql", "searchUri.mysql", "updateAttribute.mysql"};
  for (std::string& pname : procs) {
    if (nullptr == db_handle) { return; }
    std::string fname = directory + "proc/" + pname;
    //std::cerr<<"Executing commands in file "<<fname<<'\n';
    std::ifstream file(fname.c_str());
    std::string cmd;
    //Read the whole file into cmd
    std::getline(file, cmd, '\0');
    //Now remove any of the delimiter command since they are not needed through the C API
    std::vector<std::string> dels = {"DELIMITER //", "//", "DELIMITER ;"};
    for (auto d : dels) {
      auto del_pos = cmd.find(d);
      if (del_pos != std::string::npos) {
        cmd.erase(del_pos, d.size());
      }
    }
    //Execute the commands in the file
    if (mysql_real_query(db_handle, cmd.c_str(), cmd.size())) {
      std::cerr<<"Error executing commands in file "<<fname<<": "<<mysql_error(db_handle)<<"\n";
      mysql_close(db_handle);
      db_handle = nullptr;
    }
    else {
      int status = 0;
      do {
        MYSQL_RES* result = mysql_store_result(db_handle);
        if (nullptr != result) {
          mysql_free_result(result);
        }
        status = mysql_next_result(db_handle);
      } while (0 == status);
      if (0 < status) {
        std::cerr<<"Error executing commands in file "<<fname<<": "<<mysql_error(db_handle)<<"\n";
        mysql_close(db_handle);
        db_handle = nullptr;
      }
    }
  }
  //std::cerr<<"Done setting up stored tables and procedures.\n";
}

MysqlWorldModel::MysqlWorldModel(std::string db_name, std::string user, std::string password) {
  this->db_name = db_name;
  this->user = user;
  this->password = password;
  db_handle = nullptr;
  if ("" == db_name) {
    std::cerr<<"World model will operate without persistent storage.\n";
  }
  else {
    std::cerr<<"Opening mysql database in database '"<<db_name<<"' for data storage.\n";
    //Initialize mysql -- This automatically calls my_init() and mysql_thread_init()
    //mysql_library_init should be automatically called by mysql_init, but calling it
    //explicitly here
    mysql_library_init(0, NULL, NULL);
    db_handle = mysql_init(NULL);
    if (NULL == db_handle) {
      std::cerr<<"Error connecting to mysql: "<<mysql_error(db_handle)<<'\n';
      std::cerr<<"World model will operate without persistent storage.\n";
    }
    else {
      //Enable multiple statement in a single string sent to mysql
      if (NULL == mysql_real_connect(db_handle,"localhost", user.c_str(), password.c_str(),
            NULL, 0, NULL,CLIENT_MULTI_STATEMENTS)) {
        std::cerr<<"Error connection to database: "<<mysql_error(db_handle)<<'\n';
        std::cerr<<"World model will operate without persistent storage.\n";
        mysql_close(db_handle);
        db_handle = nullptr;
      }
      else {
        //Set the character collation
        {
          std::string statement_str = "set collation_connection = utf16_unicode_ci;";
          if (mysql_query(db_handle, statement_str.c_str())) {
            std::cerr<<"Error setting collate to utf16.\n";
            mysql_close(db_handle);
            db_handle = nullptr;
          }
        }
        //Now try to switch to the database
        if (mysql_select_db(db_handle, db_name.c_str())) {
          //std::cerr<<"Error switching to database for world model: "<<mysql_error(db_handle)<<"\n";
          //std::cerr<<"Trying to create new database...\n";
          std::string statement_str = "CREATE DATABASE IF NOT EXISTS "+db_name+";";
          if (mysql_query(db_handle, statement_str.c_str())) {
            std::cerr<<"Error creating database for world model: "<<mysql_error(db_handle)<<"\n";
            mysql_close(db_handle);
            db_handle = nullptr;
          }
          else {
            int status = 0;
            do {
              MYSQL_RES* result = mysql_store_result(db_handle);
              if (nullptr != result) {
                mysql_free_result(result);
              }
              status = mysql_next_result(db_handle);
            } while (0 == status);
            //Now switch to the database
            if (mysql_select_db(db_handle, db_name.c_str())) {
              std::cerr<<"Error switching to database for world model: "<<mysql_error(db_handle)<<"\n";
              mysql_close(db_handle);
              db_handle = nullptr;
            }
            //And populate it with tables and stored procedures
            //Execute stored SQL procedures and create tables
            std::string path = "./";
            //std::cerr<<"Setting up mysql from files\n";
            setupMySQL(path, db_handle);
          }
        }
      }
    }
  }
  //Disable multiline statements
  if (db_handle != nullptr) {
    if (0 != mysql_set_server_option(db_handle, MYSQL_OPTION_MULTI_STATEMENTS_OFF)) {
      std::cerr<<"Error setting server options: "<<mysql_error(db_handle)<<'\n';
    }
  }

  //Set up the database settings for the query threads
  QueryThread<WorldModel::world_state>::setDBInfo(db_name, user, password, db_handle);

  //Load existing values using the current table.
  {
    std::cerr<<"Loading world model\n";
    /*
    std::u16string everything(u".*");
    std::vector<u16string> all_attrs{u".*"};
    cur_state = currentSnapshot(everything, all_attrs, true);
    */
    std::u16string uri(u".*");
    std::vector<u16string> desired_attributes{u".*"};
    //TODO FIXME Right now the current snapshot doesn't check the db so it
    //cannot be used to load the table. However, this call is faster than
    //the historic snapshot
    //Return if we cannot get a connection
    if (nullptr != db_handle) {
      //CREATE PROCEDURE getCurrentValue(uri VARCHAR(170) CHARACTER SET utf16 COLLATE utf16_unicode_ci,
      //attribute VARCHAR(170) CHARACTER SET utf16 COLLATE utf16_unicode_ci,
      //origin VARCHAR(170) CHARACTER SET utf16 COLLATE utf16_unicode_ci)
      std::string statement_str = "CALL getCurrentValue(?, ?, ?);";
      MYSQL_STMT* statement_p = mysql_stmt_init(db_handle);
      if (nullptr == statement_p) {
        //TODO This should be better at handling an error.
        std::cerr<<"Error creating statement for current snapshot.\n";
      }
      else {
        if (mysql_stmt_prepare(statement_p, statement_str.c_str(), statement_str.size())) {
          std::cerr<<"Failed to prepare statement: "<<statement_str<<": "<<mysql_error(db_handle)<<'\n';
        }
        else {
          MYSQL_BIND parameters[3];
          memset(parameters, 0, sizeof(parameters));
          std::string char8_uri(uri.begin(), uri.end());
          parameters[0].buffer_type = MYSQL_TYPE_STRING;
          parameters[0].buffer = (void*)char8_uri.data();
          unsigned long uri_len = char8_uri.size();
          parameters[0].length = &uri_len;
          parameters[0].is_unsigned = true;
          parameters[1].buffer_type = MYSQL_TYPE_STRING;
          unsigned long attr_len;
          parameters[1].length = &attr_len;
          parameters[1].is_unsigned = true;
          //TODO FIXME Accepting any origin right now
          std::string char8_origin = ".*";
          parameters[2].buffer_type = MYSQL_TYPE_STRING;
          parameters[2].buffer = (void*)char8_origin.data();
          unsigned long origin_len = char8_origin.size();
          parameters[2].length = &origin_len;

          for (std::u16string attr : desired_attributes) {
            //parameters[1].buffer = (void*)attr.data();
            //attr_len = attr.size()*2;
            std::string char8_name(attr.begin(), attr.end());
            parameters[1].buffer = (void*)char8_name.data();
            attr_len = char8_name.size();
            if (0 != mysql_stmt_bind_param(statement_p, parameters)) {
              parameters[1].buffer = (void*)uri.data();
              //TODO This should be better at handling an error.
              std::cerr<<"Error binding variables for getCurrentValue.\n";
            }
            else {
              //Execute the statement
              //std::cerr<<"Executing getCurrentValue for "+std::string(attr.begin(), attr.end())+"\n";
              std::function<WorldModel::world_state(MYSQL*)> bound_fun = [&](MYSQL* myhandle){ return this->fetchWorldData(statement_p, myhandle);};
              WorldModel::world_state partial = QueryThread<WorldModel::world_state>::assignTask(bound_fun);
              for (auto I : partial) {
                //Insert new attributes into the world state
                cur_state[I.first].insert(cur_state[I.first].end(), I.second.begin(), I.second.end());
              }
            }
            mysql_stmt_reset(statement_p);
          }
          //std::cerr<<"Finished fetching world data\n";

          //Delete the statement
          mysql_stmt_close(statement_p);
        }
      }
    }
  }
  std::cerr<<"World model loaded.\n";
}

MysqlWorldModel::~MysqlWorldModel() {
  std::cerr<<"Destroying thread pool...\n";
  QueryThread<WorldModel::world_state>::destroyThreads();
  if (nullptr != db_handle) {
    mysql_close(db_handle);
  }
  mysql_library_end();
}

WorldModel::world_state MysqlWorldModel::_deleteURI(world_model::URI& uri, MYSQL* handle) {
  WorldModel::world_state deleted;
  //Return if we cannot get a connection
  if (handle == nullptr) {
    std::cerr<<"Cannot call deleteURI: given a null connection.\n";
    return deleted;
  }
  std::string statement_str = "CALL deleteUri(?);";
  MYSQL_STMT* statement_p = mysql_stmt_init(handle);
  if (nullptr == statement_p) {
    //TODO This should be better at handling an error.
    std::cerr<<"Error creating statement deleteURI.\n";
    return deleted;
  }
  if (mysql_stmt_prepare(statement_p, statement_str.c_str(), statement_str.size())) {
    std::cerr<<"Failed to prepare statement: "<<statement_str<<": "<<mysql_error(handle)<<'\n';
    mysql_stmt_close(statement_p);
    return deleted;
  }
  MYSQL_BIND parameters[1];
  memset(parameters, 0, sizeof(parameters));
  std::string char8_uri(uri.begin(), uri.end());
  parameters[0].buffer_type = MYSQL_TYPE_STRING;
  parameters[0].buffer = (void*)char8_uri.data();
  unsigned long uri_len = char8_uri.size();
  parameters[0].length = &uri_len;
  if (0 != mysql_stmt_bind_param(statement_p, parameters)) {
    //TODO This should be better at handling an error.
    std::cerr<<"Error binding variables for deleteURI.\n";
    mysql_stmt_close(statement_p);
    return deleted;
  }
  else {
    //Execute the statement
    if (0 != mysql_stmt_execute(statement_p)) {
      std::cerr<<"Error executing statement for deleteURI: "<<mysql_error(handle)<<"\n";
      mysql_stmt_close(statement_p);
      return deleted;
    }
    else {
      deleted[uri].push_back(world_model::Attribute());
    }
  }
  //Delete the statement
  mysql_stmt_close(statement_p);
  return deleted;
}

WorldModel::world_state MysqlWorldModel::_deleteURIAttributes(world_model::URI& uri,
    std::vector<world_model::Attribute>& entries, MYSQL* handle) {
  WorldModel::world_state deleted;
  //Return if we cannot get a connection
  if (handle == nullptr) {
    std::cerr<<"Cannot call deleteURIAttributes: given a null connection.\n";
    return deleted;
  }
  std::string statement_str = "CALL deleteAttribute(?, ?);";
  MYSQL_STMT* statement_p = mysql_stmt_init(handle);
  if (nullptr == statement_p) {
    //TODO This should be better at handling an error.
    std::cerr<<"Error creating statement deleteAttribute.\n";
  }
  if (mysql_stmt_prepare(statement_p, statement_str.c_str(), statement_str.size())) {
    std::cerr<<"Failed to prepare statement: "<<statement_str<<": "<<mysql_error(handle)<<'\n';
    mysql_stmt_close(statement_p);
    return deleted;
  }
  MYSQL_BIND parameters[2];
  memset(parameters, 0, sizeof(parameters));
  std::string char8_uri(uri.begin(), uri.end());
  parameters[0].buffer_type = MYSQL_TYPE_STRING;
  parameters[0].buffer = (void*)char8_uri.data();
  unsigned long uri_len = char8_uri.size();
  parameters[0].length = &uri_len;
  parameters[1].buffer_type = MYSQL_TYPE_STRING;
  unsigned long attr_len;
  parameters[1].length = &attr_len;

  //Delete every matching entry
  for (auto entry = entries.begin(); entry != entries.end(); ++entry) {
    //parameters[1].buffer = (void*)entry->name.data();
    //attr_len = entry->name.size()*2;
    std::string char8_name(entry->name.begin(), entry->name.end());
    parameters[1].buffer = (void*)char8_name.data();
    attr_len = char8_name.size();
    if (0 != mysql_stmt_bind_param(statement_p, parameters)) {
      parameters[1].buffer = (void*)uri.data();
      //TODO This should be better at handling an error.
      std::cerr<<"Error binding variables for deleteAttribute.\n";
    }
    else {
      //Execute the statement
      if (0 != mysql_stmt_execute(statement_p)) {
        std::cerr<<"Error executing statement for deleteAttribute: "<<mysql_error(handle)<<"\n";
      }
      else {
        deleted[uri].push_back(*entry);
      }
    }
    mysql_stmt_reset(statement_p);
  }
  //Delete the statement
  mysql_stmt_close(statement_p);
  return deleted;
}

bool MysqlWorldModel::createURI(world_model::URI uri,
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
  std::function<WorldModel::world_state(MYSQL*)> bound_fun = [&](MYSQL* handle){ return this->databaseStore(uri, to_store, handle);};
  //Send this task to a query thread
  WorldModel::world_state result = QueryThread<WorldModel::world_state>::assignTask(bound_fun);
  return not result.empty();
}

//Block access to the world model until this new information is added to it
bool MysqlWorldModel::insertData(std::vector<std::pair<world_model::URI, std::vector<world_model::Attribute>>> new_data, bool autocreate) {
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
          //Store separately to send to standing queries
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
  //TODO FIXME Send transients to any standing queries

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
          //Don't insert anything from ths URI
          entries.clear();
        }
      }

      //Now update the in-memory storage for the current state of the world model
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
        }
        //If this entry is newer than what is currently in the model update the model
        else if (slot->creation_date < entry->creation_date) {
            //Remember the current slot and its expiration time
            slot->expiration_date = entry->creation_date;
            to_expire[uri].push_back(*slot);
            //Now overwrite the slot's value with the new entry
            *slot = *entry;
        }
        //Always update the db
        current_update[uri].push_back(*entry);
      }
    }
  }


  //Put these new attributes into the database
  //Store all of the entries that were not transient types in the db
  for (auto I = new_data.begin(); I != new_data.end(); ++I) {
    if (not I->second.empty()) {
      std::function<WorldModel::world_state(MYSQL*)> bound_fun = [&](MYSQL* handle){ return this->databaseStore(I->first, I->second, handle);};
      //Send this task to a query thread
      WorldModel::world_state result = QueryThread<WorldModel::world_state>::assignTask(bound_fun);
    }
  }
  //Expiration times are automatically updated by the stored procedure

  //time_diff = world_model::getGRAILTime() - time_start;
  //std::cerr<<"DB insertion time was "<<time_diff<<'\n';
  //time_start = world_model::getGRAILTime();

  //Now service standing queries with anything that has updated the
  //current state of the world model. Re-insert additions to the
  //transient values here.
  for (auto I = transients.begin(); I != transients.end(); ++I) {
    std::vector<world_model::Attribute>& attrs = current_update[I->first];
    attrs.insert(attrs.end(), I->second.begin(), I->second.end());
  }
  //Lock the standing queries so they don't get deleted while we insert data
  std::unique_lock<std::mutex> lck(sq_mutex);
  for (auto sq = standing_queries.begin(); sq != standing_queries.end(); ++sq) {
    //First see what items are of interest. This also tells the standing
    //query to remember partial matches so we do not need to keep feeding
    //it the current state, only the updates.
    auto ws = sq->showInterested(current_update);
    //Insert the data.
    sq->insertData(ws);
  }
  //time_diff = world_model::getGRAILTime() - time_start;
  //std::cerr<<"Standing query insertion time was "<<time_diff<<'\n';

  return true;
}

void MysqlWorldModel::expireURI(world_model::URI uri, world_model::grail_time expires) {
  //Remove the URI and its attributes from the current world model
  //Lock the access control to get unique access to the world state.
  {
    SemaphoreLock lck(access_control);
    //The URI cannot be created through this message
    if (cur_state.find(uri) == cur_state.end()) {
      return;
    }
    cur_state.erase(uri);
  }
  std::vector<world_model::Attribute> to_expire(1);
  to_expire[0].name = u"creation";
  to_expire[0].expiration_date = expires;
  std::function<WorldModel::world_state(MYSQL*)> bound_fun = [&](MYSQL* handle){ return this->databaseUpdate(uri, to_expire, handle);};
  //Send this task to a query thread
  WorldModel::world_state result = QueryThread<WorldModel::world_state>::assignTask(bound_fun);

  //Lock the standing queries so they don't get deleted while we insert data
  std::unique_lock<std::mutex> lck(sq_mutex);
  for (auto sq = standing_queries.begin(); sq != standing_queries.end(); ++sq) {
    //See if the standing query cares about this expiration
    sq->expireURI(uri);
  }
}

void MysqlWorldModel::expireURIAttributes(world_model::URI uri, std::vector<world_model::Attribute>& entries, world_model::grail_time expires) {
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
  std::function<WorldModel::world_state(MYSQL*)> bound_fun = [&](MYSQL* handle){ return this->databaseUpdate(uri, to_update, handle);};
  //Send this task to a query thread
  WorldModel::world_state result = QueryThread<WorldModel::world_state>::assignTask(bound_fun);

  //Lock the standing queries so they don't get deleted while we insert data
  std::unique_lock<std::mutex> lck(sq_mutex);
  for (auto sq = standing_queries.begin(); sq != standing_queries.end(); ++sq) {
    //See if the standing query cares about this expiration
    sq->expireURIAttributes(uri, entries);
  }
}

void MysqlWorldModel::deleteURI(world_model::URI uri) {
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

  std::function<WorldModel::world_state(MYSQL*)> bound_fun = [&](MYSQL* handle){ return this->_deleteURI(uri, handle);};
  //Send this task to a thread in the thread pool
  WorldModel::world_state result = QueryThread<WorldModel::world_state>::assignTask(bound_fun);
}

void MysqlWorldModel::deleteURIAttributes(world_model::URI uri, std::vector<world_model::Attribute> entries) {
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
  std::function<WorldModel::world_state(MYSQL*)> bound_fun = [&](MYSQL* handle){ return this->_deleteURIAttributes(uri, entries, handle);};
  //Send this task to a thread in the thread pool
  WorldModel::world_state result = QueryThread<WorldModel::world_state>::assignTask(bound_fun);

  //Lock the standing queries so they don't get deleted while we insert data
  std::unique_lock<std::mutex> lck(sq_mutex);
  for (auto sq = standing_queries.begin(); sq != standing_queries.end(); ++sq) {
    //See if the standing query cares about this deletion
    //Deletions are the same as expirations from the standing queries perspective
    sq->expireURIAttributes(uri, entries);
  }
}

WorldModel::world_state MysqlWorldModel::currentSnapshot(const URI& uri,
                                                         vector<u16string>& desired_attributes,
                                                         bool get_data) {
  //Return if nothing was requested
  if (desired_attributes.empty()) {
    return WorldModel::world_state();
  }
  //Find which URIs match the given search string
  std::vector<world_model::URI> matches = searchURI(uri);

  world_state result;
  if (0 < matches.size()) {
    //Make a regular expression for each attribute
    //First build the expressions
    std::vector<regex_t> expressions;
    for (auto exp_str = desired_attributes.begin(); exp_str != desired_attributes.end(); ++exp_str) {
      regex_t exp;
      int err = regcomp(&exp, std::string(exp_str->begin(), exp_str->end()).c_str(), REG_EXTENDED);
      if (0 != err) {
        std::cerr<<"Error compiling regular expression "<<std::string(exp_str->begin(), exp_str->end())<<" in attribute of snapshot request.\n";
      }
      else {
        expressions.push_back(exp);
      }
    }
    //Flag the access control so that this read does not conflict with a write.
    SemaphoreFlag flag(access_control);
    
    //Find the attributes of interest for each URI
    //Attributes search have an AND relationship - this URI's results are only
    //returned if all of the attribute search have matches.
    for (auto uri_match = matches.begin(); uri_match != matches.end(); ++uri_match) {
      //Make a reference to the URI's attributes for ease of access
      std::vector<world_model::Attribute>& attributes = cur_state[*uri_match];
      std::vector<world_model::Attribute> matched_attributes;
      std::vector<bool> attr_matched(expressions.size());
      //Check each of this URI's attributes to see if it was requested
      for (auto attr = attributes.begin(); attr != attributes.end(); ++attr) {
        //This is a desired attribute if it appears in the attributes list
        //Also return this attribute if no attributes were specified
        //Check for a match that consumes the entire string
        //TODO Should also check origins here
        //Count which search expressions match
        bool matched = false;
        for (size_t search_ind = 0; search_ind < expressions.size(); ++search_ind) {
          //Use regex matching
          regmatch_t pmatch;
          int match = regexec(&expressions[search_ind], std::string(attr->name.begin(), attr->name.end()).c_str(), 1, &pmatch, 0);
          if (0 == match and 0 == pmatch.rm_so and attr->name.size() == pmatch.rm_eo) {
            attr_matched[search_ind] = true;
            matched = true;
          }
        }
        if (matched) {
          if (get_data) {
            matched_attributes.push_back(*attr);
          }
          else {
            matched_attributes.push_back(
                Attribute{attr->name, attr->creation_date, attr->expiration_date, attr->origin, Buffer{}});
          }
        }
      }
      //If all of the desired attributes were matched then return this URI
      //and its attributes to the user.
      if (std::none_of(attr_matched.begin(), attr_matched.end(), [&](const bool& b) { return not b;})) {
        result[*uri_match] = matched_attributes;
      }
    }
    //Free the memory used in the regex
    std::for_each(expressions.begin(), expressions.end(), [&](regex_t& exp) { regfree(&exp);});
  }
  return result;
  /* TODO FIXME Use the in-memory search from above, or the mysql call below?
  //Obtain a handle from the connection pool
  MYSQL* handle = getConnection();
  //Return if we cannot get a connection
  if (nullptr == handle) {
    return WorldModel::world_state();
  }
  //CREATE PROCEDURE getCurrentValue(uri VARCHAR(170) CHARACTER SET utf16 COLLATE utf16_unicode_ci,
                                   //attribute VARCHAR(170) CHARACTER SET utf16 COLLATE utf16_unicode_ci,
                                   //origin VARCHAR(170) CHARACTER SET utf16 COLLATE utf16_unicode_ci)
  std::string statement_str = "CALL getCurrentValue(?, ?, ?);";
  MYSQL_STMT* statement_p = mysql_stmt_init(handle);
  if (nullptr == statement_p) {
    //TODO This should be better at handling an error.
    std::cerr<<"Error creating statement for current snapshot.\n";
    return WorldModel::world_state();
  }
  if (mysql_stmt_prepare(statement_p, statement_str.c_str(), statement_str.size())) {
    std::cerr<<"Failed to prepare statement: "<<statement_str<<": "<<mysql_error(handle)<<'\n';
    return WorldModel::world_state();
  }
  MYSQL_BIND parameters[3];
  memset(parameters, 0, sizeof(parameters));
  std::string char8_uri(uri.begin(), uri.end());
  parameters[0].buffer_type = MYSQL_TYPE_STRING;
  parameters[0].buffer = (void*)char8_uri.data();
  unsigned long uri_len = char8_uri.size();
  parameters[0].length = &uri_len;
  parameters[0].is_unsigned = true;
  parameters[1].buffer_type = MYSQL_TYPE_STRING;
  unsigned long attr_len;
  parameters[1].length = &attr_len;
  parameters[1].is_unsigned = true;
  //TODO FIXME Accepting any origin right now
  std::string char8_origin = ".*";
  parameters[2].buffer_type = MYSQL_TYPE_STRING;
  parameters[2].buffer = (void*)char8_origin.data();
  unsigned long origin_len = char8_origin.size();
  parameters[2].length = &origin_len;

  WorldModel::world_state result;
  for (std::u16string attr : desired_attributes) {
    //parameters[1].buffer = (void*)attr.data();
    //attr_len = attr.size()*2;
    std::string char8_name(attr.begin(), attr.end());
    parameters[1].buffer = (void*)char8_name.data();
    attr_len = char8_name.size();
    if (0 != mysql_stmt_bind_param(statement_p, parameters)) {
      parameters[1].buffer = (void*)uri.data();
      //TODO This should be better at handling an error.
      std::cerr<<"Error binding variables for getCurrentValue.\n";
    }
    else {
      //Execute the statement
      //std::cerr<<"Executing getCurrentValue for "+std::string(attr.begin(), attr.end())+"\n";
      WorldModel::world_state partial = fetchWorldData(statement_p, handle);
      for (auto I : partial) {
        //Insert new attributes into the world state
        result[I.first].insert(result[I.first].end(), I.second.begin(), I.second.end());
      }
    }
    mysql_stmt_reset(statement_p);
  }
  //std::cerr<<"Finished fetching world data\n";

  //Delete the statement
  mysql_stmt_close(statement_p);

  //Return the connection to the connection pool
  returnConnection(handle);

  //Return all of the results
  return result;
  */
}

void bindSQL(MYSQL_BIND* bind, unsigned long* length, my_bool* error, my_bool* is_null, std::string& str) {
  bind->buffer_type = MYSQL_TYPE_STRING;
  bind->buffer = (void*)str.data();
  bind->buffer_length = str.size() - 1;
  bind->length = length;
  bind->is_null = is_null;
  bind->error = error;
}

void bindSQL(MYSQL_BIND* bind, unsigned long* length, my_bool* error, my_bool* is_null, std::u16string& str) {
  bind->buffer_type = MYSQL_TYPE_STRING;
  bind->buffer = (void*)str.data();
  bind->buffer_length = str.size()*2 - 2;
  bind->length = length;
  bind->is_null = is_null;
  bind->error = error;
}

void bindSQL(MYSQL_BIND* bind, unsigned long* length, my_bool* error, my_bool* is_null, int64_t& num) {
  bind->buffer_type = MYSQL_TYPE_LONGLONG;
  bind->buffer = &num;
  bind->buffer_length = sizeof(num);
  bind->length = length;
  bind->is_null = is_null;
  bind->error = error;
  bind->is_unsigned = false;
}

void bindSQL(MYSQL_BIND* bind, unsigned long* length, my_bool* error, my_bool* is_null, std::vector<unsigned char>& buff) {
  bind->buffer_type = MYSQL_TYPE_BLOB;
  bind->buffer = buff.data();
  bind->buffer_length = buff.size();
  bind->length = length;
  bind->is_null = is_null;
  bind->error = error;
}

template <typename T, typename... R>
void bindSQL(MYSQL_BIND* bind, unsigned long* lengths, my_bool* errors, my_bool* is_nulls, T& val, R&... r) {
  MYSQL_BIND* next_bind = bind+1;
  unsigned long* next_long = lengths == NULL ? NULL : lengths + 1;
  my_bool* next_err = errors == NULL ? NULL : errors + 1;
  my_bool* next_null = is_nulls == NULL ? NULL : is_nulls + 1;
  //Bind this value
  bindSQL(bind, lengths, errors, is_nulls, val);
  //Now bind the rest
  bindSQL(next_bind, next_long, next_err, next_null, r...);
}

//Fetches the world data from a mysql_stmt_execute command (call after mysql_stmt_execute)
WorldModel::world_state MysqlWorldModel::fetchWorldData(MYSQL_STMT* stmt, MYSQL* handle) {
  WorldModel::world_state ws;
  //Expecting a set of these columns:
  //u.uriName AS uri, a.attributeName AS attribute, o.originName AS origin, 
  //  av.data, av.createTimestamp AS created, av.expireTimestamp AS expires 

  if (nullptr == handle) {
    std::cerr<<"Error fetching data -- connection is null\n";
    return ws;
  }

  //Execute the statement
  if (mysql_stmt_execute(stmt)) {
    std::cerr<<"SQL statement failed: "<<mysql_stmt_error(stmt)<<'\n';
    return ws;
  }

  //Get the parameter count from the statement
  int param_count = mysql_stmt_param_count(stmt);

  //Validate parameter count
  //if (param_count != 6) {
    //std::cerr<<"Bad parameter count fetching world data from mysql.\n";
    //return ws;
  //}

  //Fetch result set meta information */
  MYSQL_RES* prepare_meta_result = mysql_stmt_result_metadata(stmt);
  if (!prepare_meta_result) {
    std::cerr<<"Error fetching meta-information to get world data: "<<mysql_stmt_error(stmt)<<'\n';
    return ws;
  }

  //Get total columns in the query
  int column_count = mysql_num_fields(prepare_meta_result);
  //Check for the expected number of columns
  if (column_count != 6) {
    std::cerr<<"Bad column count while fetching world data -- expected 6 got "<<column_count<<'\n';
    return ws;
  }

  MYSQL_BIND bind[6];
  my_bool error[6];
  my_bool is_null[6];

  memset(bind, 0, sizeof(bind));
  //u.uriName AS uri, a.attributeName AS attribute, o.originName AS origin, 
  //  av.data, av.createTimestamp AS created, av.expireTimestamp AS expires 

  //Column 1: UTF16 string
  //Column 2: UTF16 string
  //Column 3: UTF16 string
  //Column 4: Binary blob
  //Column 5: Bigint
  //Column 6: Bigint
  unsigned long lengths[6];
  //std::u16string in_uri(342, '\0');
  //std::u16string in_attr(342, '\0');
  //std::u16string in_origin(342, '\0');
  std::string in_uri(171, '\0');
  std::string in_attr(171, '\0');
  std::string in_origin(171, '\0');
  std::vector<unsigned char> in_data(2000);
  int64_t creation, expiration;
  bindSQL(bind, lengths, error, is_null, in_uri, in_attr, in_origin, in_data, creation, expiration);

  //Bind the result buffers
  if (mysql_stmt_bind_result(stmt, bind)) {
    std::cerr<<"Error binding to result buffers while fetching world data: "<<mysql_stmt_error(stmt)<<'\n';
    return ws;
  }

  //mysql_stmt_fetch() returns zero if a row was fetched successfully,
  //MYSQL_NO_DATA if there are no more rows to fetch, and 1 if an error occurred.
  //After a successful fetch, the column values are available in the MYSQL_BIND
  //structures bound to the result.
  int status = 0;
  int num_rows = 0;
  while (0 == (mysql_stmt_fetch(stmt))) {
    ++num_rows;
    std::u16string uri(in_uri.begin(), in_uri.begin() + lengths[0]);
    std::u16string attr(in_attr.begin(), in_attr.begin() + lengths[1]);
    std::u16string origin(in_origin.begin(), in_origin.begin() + lengths[2]);
    std::vector<unsigned char> data(in_data.begin(), in_data.begin() + lengths[3]);
    ws[uri].push_back(world_model::Attribute{attr, creation, expiration, origin, data});
  }
  {
    int status;
    do {
      status = mysql_next_result(handle);
    } while (0 == status);
    if (0 < status) {
      std::cerr<<"Error fetching world data: "<<mysql_error(handle)<<"\n";
    }
  }
  //mysql_stmt_fetch() to retrieve rows, and mysql_stmt_free_result() to free the result set.
  mysql_stmt_free_result(stmt);

  //Free the prepared result metadata
  mysql_free_result(prepare_meta_result);

  //Let the caller close the statement so that they can reuse it
  //if (mysql_stmt_close(stmt)) {
    //std::cerr<<"Error closing mysql statement: "<<mysql_stmt_error(stmt)<<'\n';
  //}
  return ws;
}

/**
 * Get the state of the world model after the data from the given time range.
 * Any number of read requests can be simultaneously serviced.
 */
WorldModel::world_state MysqlWorldModel::_historicSnapshot(const world_model::URI& uri,
                                    std::vector<std::u16string>& desired_attributes,
                                    world_model::grail_time start, world_model::grail_time stop,
                                    MYSQL* handle) {
  //Return if nothing was requested
  if (desired_attributes.empty()) {
    return WorldModel::world_state();
  }
  //Return if we cannot get a connection
  if (nullptr == handle) {
    std::cerr<<"Cannot call getSnapshotValue -- connection is null\n";
    return WorldModel::world_state();
  }
  //CREATE PROCEDURE getSnapshotValue(uri VARCHAR(170) CHARACTER SET utf16 COLLATE utf16_unicode_ci,
  //                                 attribute VARCHAR(170) CHARACTER SET utf16 COLLATE utf16_unicode_ci,
  //                                 origin VARCHAR(170) CHARACTER SET utf16 COLLATE utf16_unicode_ci,
  //                                 timestamp BIGINT)
  std::string statement_str = "CALL getSnapshotValue(?, ?, ?, ?);";
  MYSQL_STMT* statement_p = mysql_stmt_init(handle);
  if (nullptr == statement_p) {
    //TODO This should be better at handling an error.
    std::cerr<<"Error creating statement for historic snapshot.\n";
    return WorldModel::world_state();
  }
  if (mysql_stmt_prepare(statement_p, statement_str.c_str(), statement_str.size())) {
    std::cerr<<"Failed to prepare statement: "<<statement_str<<": "<<mysql_error(handle)<<'\n';
    mysql_stmt_close(statement_p);
    return WorldModel::world_state();
  }
  MYSQL_BIND parameters[4];
  std::string char8_uri(uri.begin(), uri.end());
  memset(parameters, 0, sizeof(parameters));
  parameters[0].buffer_type = MYSQL_TYPE_STRING;
  //parameters[0].buffer = (void*)uri.data();
  parameters[0].buffer = (void*)char8_uri.data();
  //unsigned long uri_len = uri.size()*2;
  unsigned long uri_len = char8_uri.size();
  parameters[0].length = &uri_len;
  parameters[0].is_unsigned = true;
  parameters[1].buffer_type = MYSQL_TYPE_STRING;
  unsigned long attr_len;
  parameters[1].length = &attr_len;
  parameters[1].is_unsigned = true;
  //TODO FIXME Accepting any origin right now
  std::u16string char8_origin = u".*";
  parameters[2].buffer_type = MYSQL_TYPE_STRING;
  parameters[2].buffer = (void*)char8_origin.data();
  unsigned long origin_len = char8_origin.size();
  parameters[2].length = &origin_len;
  parameters[3].buffer_type = MYSQL_TYPE_LONGLONG;
  parameters[3].is_unsigned = false;
  parameters[3].buffer = &(stop);
  WorldModel::world_state result;
  for (std::u16string attr : desired_attributes) {
    //parameters[1].buffer = (void*)attr.data();
    //attr_len = attr.size()*2;
    std::string char8_attr(attr.begin(), attr.end());
    parameters[1].buffer = (void*)char8_attr.data();
    attr_len = char8_attr.size();
    if (0 != mysql_stmt_bind_param(statement_p, parameters)) {
      //TODO This should be better at handling an error.
      std::cerr<<"Error binding variables for getSnapshotValue.\n";
    }
    else {
      //Execute the statement
      WorldModel::world_state partial = fetchWorldData(statement_p, handle);
      for (auto I : partial) {
        //Insert new attributes into the world state
        result[I.first].insert(result[I.first].end(), I.second.begin(), I.second.end());
      }
    }
    //Ready the statement to bind to the next search strings
    mysql_stmt_reset(statement_p);
  }
  //Delete the statement
  mysql_stmt_close(statement_p);

  //Return all of the results
  return result;
}

/**
 * Get the state of the world model after the data from the given time range.
 * Any number of read requests can be simultaneously serviced.
 */
WorldModel::world_state MysqlWorldModel::historicSnapshot(const world_model::URI& uri,
                                    std::vector<std::u16string> desired_attributes,
                                    world_model::grail_time start, world_model::grail_time stop) {
  std::function<WorldModel::world_state(MYSQL*)> bound_fun = [&](MYSQL* handle){ return this->_historicSnapshot(uri, desired_attributes, start, stop, handle);};

  //Send this task to a query thread
  WorldModel::world_state result = QueryThread<WorldModel::world_state>::assignTask(bound_fun);

  return result;
}


/**
 * Get stored data that occurs in a time range.
 * Any number of read requests can be simultaneously serviced.
 */
WorldModel::world_state MysqlWorldModel::_historicDataInRange(const world_model::URI& uri,
                                    std::vector<std::u16string>& desired_attributes,
                                    world_model::grail_time start, world_model::grail_time stop, MYSQL* handle) {
  //Return if we cannot get a connection
  if (nullptr == handle) {
    std::cerr<<"Cannot call getRangeValues -- connection is null\n";
    return WorldModel::world_state();
  }
  //CREATE PROCEDURE getRangeValues(uri VARCHAR(170) CHARACTER SET utf16 COLLATE utf16_unicode_ci,
                                   //attribute VARCHAR(170) CHARACTER SET utf16 COLLATE utf16_unicode_ci,
                                   //origin VARCHAR(170) CHARACTER SET utf16 COLLATE utf16_unicode_ci,
                                   //beginTs BIGINT,
                                   //endTs BIGINT)
  std::string statement_str = "CALL getRangeValues(?, ?, ?, ?, ?);";
  MYSQL_STMT* statement_p = mysql_stmt_init(handle);
  if (nullptr == statement_p) {
    //TODO This should be better at handling an error.
    std::cerr<<"Error creating statement for historic snapshot.\n";
    return WorldModel::world_state();
  }
  if (mysql_stmt_prepare(statement_p, statement_str.c_str(), statement_str.size())) {
    std::cerr<<"Failed to prepare statement: "<<statement_str<<": "<<mysql_error(handle)<<'\n';
    return WorldModel::world_state();
  }
  MYSQL_BIND parameters[5];
  std::string char8_uri(uri.begin(), uri.end());
  memset(parameters, 0, sizeof(parameters));
  parameters[0].buffer_type = MYSQL_TYPE_STRING;
  //parameters[0].buffer = (void*)uri.data();
  parameters[0].buffer = (void*)char8_uri.data();
  //unsigned long uri_len = uri.size()*2;
  unsigned long uri_len = char8_uri.size();
  parameters[0].length = &uri_len;
  parameters[0].is_unsigned = true;
  parameters[1].buffer_type = MYSQL_TYPE_STRING;
  unsigned long attr_len;
  parameters[1].length = &attr_len;
  parameters[1].is_unsigned = true;
  //TODO FIXME Accepting any origin right now
  std::u16string char8_origin = u".*";
  parameters[2].buffer_type = MYSQL_TYPE_STRING;
  parameters[2].buffer = (void*)char8_origin.data();
  unsigned long origin_len = char8_origin.size();
  parameters[2].length = &origin_len;
  parameters[3].buffer_type = MYSQL_TYPE_LONGLONG;
  parameters[3].is_unsigned = false;
  parameters[3].buffer = &(start);
  parameters[4].buffer_type = MYSQL_TYPE_LONGLONG;
  parameters[4].is_unsigned = false;
  parameters[4].buffer = &(stop);
  WorldModel::world_state result;
  for (std::u16string attr : desired_attributes) {
    //parameters[1].buffer = (void*)attr.data();
    //attr_len = attr.size()*2;
    std::string char8_attr(attr.begin(), attr.end());
    parameters[1].buffer = (void*)char8_attr.data();
    attr_len = char8_attr.size();
    if (0 != mysql_stmt_bind_param(statement_p, parameters)) {
      //TODO This should be better at handling an error.
      std::cerr<<"Error binding variables for getRangeValues.\n";
    }
    else {
      //Execute the statement in a query thread
      WorldModel::world_state partial = fetchWorldData(statement_p, handle);
      for (auto I : partial) {
        //Insert new attributes into the world state
        result[I.first].insert(result[I.first].end(), I.second.begin(), I.second.end());
      }
    }
    //Ready the statement to bind to the next search strings
    mysql_stmt_reset(statement_p);
  }
  //Delete the statement
  mysql_stmt_close(statement_p);

  //Return all of the results
  return result;
}

WorldModel::world_state MysqlWorldModel::historicDataInRange(const world_model::URI& uri,
                                    std::vector<std::u16string>& desired_attributes,
                                    world_model::grail_time start, world_model::grail_time stop) {
  //Return if nothing was requested
  if (desired_attributes.empty()) {
    return WorldModel::world_state();
  }

  std::function<WorldModel::world_state(MYSQL*)> bound_fun = [&](MYSQL* handle){ return this->_historicDataInRange(uri, desired_attributes, start, stop, handle);};

  //Send this task to a query thread
  WorldModel::world_state result = QueryThread<WorldModel::world_state>::assignTask(bound_fun);

  return result;
}

//Register an attribute name as a transient type. Transient types are not
//stored in the SQL table but are stored in the cur_state map.
void MysqlWorldModel::registerTransient(std::u16string& attr_name, std::u16string& origin) {
  std::unique_lock<std::mutex> lck(transient_lock);
  transient.insert(std::make_pair(attr_name, origin));
}

/**
 * When this request is called the query object is immediately populated.
 * Afterwards any updates that arrive that match the query criteria are
 * added into the standing query.
 */
QueryAccessor MysqlWorldModel::requestStandingQuery(const world_model::URI& uri,
    std::vector<std::u16string>& desired_attributes, bool get_data) {
  std::unique_lock<std::mutex> lck(sq_mutex);
  standing_queries.push_front(StandingQuery(uri, desired_attributes, get_data));
  //Populate the query
  //Flag the access control so that this read does not conflict with a write.
  SemaphoreFlag flag(access_control);
  //Indicate that multiple origins are used when checking interest so that
  //the standing query does not try to optimize the check based upon origins.
  world_state ws = standing_queries.front().showInterested(cur_state, true);
  standing_queries.front().insertData(ws);
  return QueryAccessor(&standing_queries, &sq_mutex, standing_queries.begin());
}

