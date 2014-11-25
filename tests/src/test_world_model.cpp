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
 * This program is made to test the world model implementations.
 ******************************************************************************/

#include <world_model.hpp>
#include <sqlite3_world_model.hpp>
#include <mysql_world_model.hpp>
#include <task_pool.hpp>

#include <owl/world_model_protocol.hpp>

#include <algorithm>
#include <iostream>
#include <vector>
#include <unistd.h>

#include <stdlib.h>

#include <time.h>
#include <thread>

#include <chrono>


using namespace world_model;
using namespace std;
using namespace std::chrono;

vector<Attribute> attributes1{
  Attribute{u"att1", 100, 0, u"test_world_model", {0,1,2,3}},
    Attribute{u"att2", 100, 0, u"test_world_model", {0,1,2,3}},
    Attribute{u"att3", 100, 0, u"test_world_model", {0,1,2,3}},
    Attribute{u"att4", 100, 0, u"test_world_model", {0,1,2,3}},
    Attribute{u"att5", 100, 0, u"test_world_model", {0,1,2,3}},
    Attribute{u"att6", 100, 0, u"test_world_model", {0,1,2,3}}};

vector<Attribute> attributes1_transient{
  Attribute{u"att1", 100, 0, u"transient_test", {2,3}},
    Attribute{u"att2", 100, 0, u"transient_test", {2,3}},
    Attribute{u"att3", 100, 0, u"transient_test", {2,3}},
    Attribute{u"att4", 100, 0, u"transient_test", {2,3}},
    Attribute{u"att5", 100, 0, u"transient_test", {2,3}},
    Attribute{u"att6", 100, 0, u"transient_test", {2,3}}};

vector<Attribute> attributes2{
  Attribute{u"att1", 200, 0, u"test_world_model", {1,2,3}},
    Attribute{u"att2", 200, 0, u"test_world_model", {1,2,3}},
    Attribute{u"att3", 200, 0, u"test_world_model", {1,2,3}},
    Attribute{u"att4", 200, 0, u"test_world_model", {1,2,3}},
    Attribute{u"att5", 200, 250, u"test_world_model", {1,2,3}},
    Attribute{u"att6", 200, 0, u"test_world_model", {1,2,3}}};

vector<Attribute> attributes3{
  Attribute{u"att1", 300, 0, u"test_world_model", {2,3}},
    Attribute{u"att2", 300, 0, u"test_world_model", {2,3}},
    Attribute{u"att3", 300, 0, u"test_world_model", {2,3}},
    Attribute{u"att4", 300, 0, u"test_world_model", {2,3}},
    Attribute{u"att6", 300, 0, u"test_world_model", {2,3}}};

vector<Attribute> attributes4{
  Attribute{u"att1", 400, 0, u"test_world_model", {2,3}},
    Attribute{u"att2", 400, 0, u"test_world_model", {2,3}},
    Attribute{u"att3", 400, 0, u"test_world_model", {2,3}},
    Attribute{u"att4", 400, 0, u"test_world_model", {2,3}},
    Attribute{u"att5", 400, 0, u"test_world_model", {2,3}},
    Attribute{u"att6", 400, 0, u"test_world_model", {2,3}}};

URI uri1 = u"test1";
URI uri2 = u"test2";

bool createAndSearchURIs(WorldModel& wm) {
  vector<URI> uris{uri1, uri2};
  for_each(uris.begin(), uris.end(), [&](URI uri) { wm.createURI(uri, u"test_world_model", 1);});
  //Test searching for previously inserted URIs
  vector<URI> found = wm.searchURI(u"test.*");
  if (any_of(found.begin(), found.end(), [&](URI& uri) { return uri == uri1;}) and
      any_of(found.begin(), found.end(), [&](URI& uri) { return uri == uri2;})) {
    return true;
  }
  else {
    return false;
  }
}

bool searchSingleURI(WorldModel& wm) {
  //Test searching for previously inserted URIs
  vector<URI> found = wm.searchURI(u".*1");
  if (any_of(found.begin(), found.end(), [&](URI& uri) { return uri == uri1;}) and
      not any_of(found.begin(), found.end(), [&](URI& uri) { return uri == uri2;})) {
    return true;
  }
  else {
    return false;
  }
}

//Requires URIs to have been previously created in createAndSearchURIs
bool insertHalfAttributes(WorldModel& wm) {
  vector<Attribute> attributes1half{
    Attribute{u"att1", 100, 0, u"test_world_model", {0,1,2,3}},
      Attribute{u"att2", 100, 0, u"test_world_model", {0,1,2,3}},
      Attribute{u"att3", 100, 0, u"test_world_model", {0,1,2,3}}};
  //Test inserting data and then retrieving it
  wm.insertData(vector<pair<URI, vector<Attribute>>>{
      make_pair(uri1, attributes1half), make_pair(uri2, attributes1)});
  return true;
}

//Requires URIs to have been previously created in createAndSearchURIs
bool insertHalfAttributes2(WorldModel& wm) {
vector<Attribute> attributes1rest{
  Attribute{u"att4", 100, 0, u"test_world_model", {0,1,2,3}},
    Attribute{u"att5", 100, 0, u"test_world_model", {0,1,2,3}},
    Attribute{u"att6", 100, 0, u"test_world_model", {0,1,2,3}}};
  //Test inserting data and then retrieving it
  wm.insertData(vector<pair<URI, vector<Attribute>>>{
      make_pair(uri1, attributes1rest), make_pair(uri2, attributes1)});
  return true;
}

//Requires URIs to have been previously created in createAndSearchURIs
bool checkStandingQueryFour(StandingQuery& sq) {

  WorldModel::world_state ws = sq.getData();
  if (ws.end() == ws.find(uri1)) {
    return false;
  }
  else {
    vector<Attribute> found = ws[uri1];
    if (found.size() == 4 and
        any_of(found.begin(), found.end(), [&](Attribute att) { return att.name == u"att1";}) and
        any_of(found.begin(), found.end(), [&](Attribute att) { return att.name == u"att2";}) and
        any_of(found.begin(), found.end(), [&](Attribute att) { return att.name == u"att5";}) and
        any_of(found.begin(), found.end(), [&](Attribute att) { return att.name == u"att6";})) {
      return true;
    }
    else {
      return false;
    }
  }
}

//Requires URIs to have been previously created in createAndSearchURIs
bool insertAndRetrieveAttributes(WorldModel& wm) {
  //Test inserting data and then retrieving it
  wm.insertData(vector<pair<URI, vector<Attribute>>>{make_pair(uri1, attributes1), make_pair(uri2, attributes1)});

  //Test retrieving inserted attributes (without data)
  vector<u16string> search_atts{u"att1", u"att2", u"att5", u"att6"};
  WorldModel::world_state ws = wm.currentSnapshot(uri1, search_atts, false);
  if (ws.end() == ws.find(uri1)) {
    return false;
  }
  else {
    vector<Attribute> found = ws[uri1];
    if (found.size() == 4 and
        any_of(found.begin(), found.end(), [&](Attribute att) { return att.name == u"att1";}) and
        any_of(found.begin(), found.end(), [&](Attribute att) { return att.name == u"att2";}) and
        any_of(found.begin(), found.end(), [&](Attribute att) { return att.name == u"att5";}) and
        any_of(found.begin(), found.end(), [&](Attribute att) { return att.name == u"att6";})) {
      return true;
    }
    else {
      return false;
    }
  }
}

bool insertAndRetrieveAttributesAuto(WorldModel& wm) {
  //Test inserting data and then retrieving it
  wm.insertData(vector<pair<URI, vector<Attribute>>>{make_pair(uri1, attributes1), make_pair(uri2, attributes1)}, true);

  //Test retrieving inserted attributes (without data)
  vector<u16string> search_atts{u"att1", u"att2", u"att5", u"att6"};
  WorldModel::world_state ws = wm.currentSnapshot(uri1, search_atts, false);
  if (ws.end() == ws.find(uri1)) {
    return false;
  }
  else {
    vector<Attribute> found = ws[uri1];
    if (found.size() == 4 and
        any_of(found.begin(), found.end(), [&](Attribute att) { return att.name == u"att1";}) and
        any_of(found.begin(), found.end(), [&](Attribute att) { return att.name == u"att2";}) and
        any_of(found.begin(), found.end(), [&](Attribute att) { return att.name == u"att5";}) and
        any_of(found.begin(), found.end(), [&](Attribute att) { return att.name == u"att6";})) {
      return true;
    }
    else {
      return false;
    }
  }
}

//Requires URIs to have been previously created in createAndSearchURIs
bool insertAndRetrieveData(WorldModel& wm) {
  //Test inserting data and then retrieving it
  wm.insertData(vector<pair<URI, vector<Attribute>>>{make_pair(uri1, attributes1), make_pair(uri2, attributes1)});

  vector<u16string> search_atts{u"att3"};
  WorldModel::world_state ws = wm.currentSnapshot(uri1, search_atts, true);
  if (ws.end() == ws.find(uri1)) {
    std::cerr<<"Result empty\n";
    return false;
  }
  else {
    vector<Attribute> found = ws[uri1];
    if (found.size() == 1 and
        found[0].name == u"att3" and
        found[0].data.size() == attributes1[2].data.size() and
        equal(found[0].data.begin(), found[0].data.end(), attributes1[2].data.begin())) {
      return true;
    }
    else {
      std::cerr<<"Found.size is "<<found.size()<<'\n';
      if (found.size() == 1) {
        if (found[0].name != u"att3") {
          std::cerr<<"Name doesn't match\n";
          std::cerr<<"Bytes are:\n\t";
          for (char16_t c : found[0].name) {
            std::cerr<<(uint32_t)c<<'\t';
          }
          std::cerr<<'\n';
        }
        else if (found[0].data.size() != attributes1[2].data.size()) {
          std::cerr<<"Data size doesn't match\n";
        }
        else {
          std::cerr<<"Data values don't match\n";
        }
      }
      return false;
    }
    //else {
      //return false;
    //}
  }
}

//Should match the results of insertAndRetrieveData
bool checkStandingQuery(StandingQuery& sq) {

  WorldModel::world_state ws = sq.getData();
  if (ws.end() == ws.find(uri1)) {
    std::cerr<<"Result empty\n";
    return false;
  }
  else {
    vector<Attribute> found = ws[uri1];
    if (found.size() == 1 and
        found[0].name == u"att3" and
        found[0].data.size() == attributes1[2].data.size() and
        equal(found[0].data.begin(), found[0].data.end(), attributes1[2].data.begin())) {
      return true;
    }
    else {
      std::cerr<<"Found.size is "<<found.size()<<'\n';
      if (found.size() == 1) {
        if (found[0].name != u"att3") {
          std::cerr<<"Name doesn't match\n";
        }
        else if (found[0].data.size() != attributes1[2].data.size()) {
          std::cerr<<"Data size doesn't match\n";
        }
        else {
          std::cerr<<"Data values don't match\n";
        }
      }
      return false;
    }
  }
}

//Should match the results of insertAndRetrieveData
bool checkStandingQueryPartial(StandingQuery& sq) {

  WorldModel::world_state ws = sq.getData();
  if (ws.end() == ws.find(uri1)) {
    std::cerr<<"Result empty\n";
    return false;
  }
  else {
    vector<Attribute> found = ws[uri1];
    if (found.size() == 2) {
      for (int i = 0; i < 2; ++i) {
        if (found[i].name == u"att3" and
            found[i].data.size() == attributes1[2].data.size() and
            equal(found[i].data.begin(), found[i].data.end(), attributes1[2].data.begin())) {
          return true;
        }
      }
      return false;
    }
    else {
      std::cerr<<"Found.size is "<<found.size()<<'\n';
      if (found.size() == 1) {
        if (found[0].name != u"att3") {
          std::cerr<<"Name doesn't match\n";
        }
        else if (found[0].data.size() != attributes1[2].data.size()) {
          std::cerr<<"Data size doesn't match\n";
        }
        else {
          std::cerr<<"Data values don't match\n";
        }
      }
      return false;
    }
  }
}

//Should have only an update to att3
bool checkStandingQueryPartial2(StandingQuery& sq) {

  WorldModel::world_state ws = sq.getData();
  if (ws.end() == ws.find(uri1)) {
    std::cerr<<"Result empty\n";
    return false;
  }
  else {
    vector<Attribute> found = ws[uri1];
    if (found.size() == 1 and
        found[0].name == u"att3") {
      return true;
    }
    else {
      std::cerr<<"Found.size is "<<found.size()<<'\n';
      if (found.size() == 1) {
        if (found[0].name != u"att3") {
          std::cerr<<"Name doesn't match\n";
        }
        else if (found[0].data.size() != attributes1[2].data.size()) {
          std::cerr<<"Data size doesn't match\n";
        }
        else {
          std::cerr<<"Data values don't match\n";
        }
      }
      return false;
    }
  }
}

//Should match the results of insertAndRetrieveData, but expire time should be non-zero
bool checkExpiredStandingQuery(StandingQuery& sq) {

  WorldModel::world_state ws = sq.getData();
  if (ws.end() == ws.find(uri1)) {
    std::cerr<<"Failed checkExpiredStandingQuery: Result empty\n";
    return false;
  }
  else {
    vector<Attribute> found = ws[uri1];
    if (found.size() == 1 and
        found[0].name == u"att3") {
      if (found[0].expiration_date != 0)
        return true;
      else {
        std::cerr<<"Failed checkExpiredStandingQuery: Attribute was not expired.\n";
        return false;
      }
    }
    else {
      std::cerr<<"Found.size is "<<found.size()<<'\n';
      if (found.size() == 1) {
        if (found[0].name != u"att3") {
          std::cerr<<"Failed checkExpiredStandingQuery: Name doesn't match\n";
        }
      }
      return false;
    }
  }
}

/**
 * Requires URIs to have been previously created in createAndSearchURIs
 * Also assumes that a non-transient entry was also inserted.
 */
bool insertAndRetrieveTransientData(WorldModel& wm) {
  //Test inserting data and then retrieving it
  wm.insertData(vector<pair<URI, vector<Attribute>>>{make_pair(uri1, attributes1_transient)});

  vector<u16string> search_atts{u"att3"};
  WorldModel::world_state ws = wm.currentSnapshot(uri1, search_atts, true);
  if (ws.end() == ws.find(uri1)) {
    return false;
  }
  else {
    vector<Attribute> found = ws[uri1];
    //Should find the non-transient values but not the transient one
    if (found.size() == 1 and
        found[0].name == u"att3") {
      return true;
    }
    else {
      return false;
    }
  }
}

//Requires URIs to have been previously created in createAndSearchURIs
bool insertAndRetrieveData2(WorldModel& wm) {
  //Test inserting data and then retrieving it
  wm.insertData(vector<pair<URI, vector<Attribute>>>{make_pair(uri1, attributes2), make_pair(uri2, attributes2)});

  vector<u16string> search_atts{u"att3"};
  WorldModel::world_state ws = wm.currentSnapshot(uri1, search_atts, true);
  if (ws.end() == ws.find(uri1)) {
    return false;
  }
  else {
    vector<Attribute> found = ws[uri1];
    if (found.size() == 1 and
        found[0].name == u"att3" and
        found[0].data.size() == attributes2[2].data.size() and
        equal(found[0].data.begin(), found[0].data.end(), attributes2[2].data.begin())) {
      return true;
    }
    else {
      return false;
    }
  }
}

//Should match the results of insertAndRetrieveData2
bool checkStandingQuery2(StandingQuery& sq) {

  WorldModel::world_state ws = sq.getData();
  if (ws.end() == ws.find(uri1)) {
    std::cerr<<"Result empty\n";
    return false;
  }
  else {
    vector<Attribute> found = ws[uri1];
    if (found.size() == 1 and
        found[0].name == u"att3" and
        found[0].data.size() == attributes2[2].data.size() and
        equal(found[0].data.begin(), found[0].data.end(), attributes2[2].data.begin())) {
      return true;
    }
    else {
      std::cerr<<"Found.size is "<<found.size()<<'\n';
      if (found.size() == 1) {
        if (found[0].name != u"att3") {
          std::cerr<<"Name doesn't match\n";
        }
        else if (found[0].data.size() != attributes1[2].data.size()) {
          std::cerr<<"Data size doesn't match\n";
        }
        else {
          std::cerr<<"Data values don't match\n";
        }
      }
      return false;
    }
  }
}

//Assumes that the data has already been inserted
bool testHistoricSnapshot1(WorldModel& wm) {
  vector<u16string> search_atts{u"att3"};
  WorldModel::world_state ws = wm.historicSnapshot(uri1, search_atts, 0, 100);
  if (ws.end() == ws.find(uri1)) {
    //std::cerr<<"Testing: URI not found\n";
    return false;
  }
  else {
    vector<Attribute> found = ws[uri1];
    //std::cerr<<"Testing: found.size() is "<<found.size()<<'\n';
    //std::cerr<<"Found names are ";
    //for (auto I = found.begin(); I != found.end(); ++I) {
      //std::cerr<<string(I->name.begin(), I->name.end())<<'\t';
    //}
    //std::cerr<<'\n';
    //std::cerr<<"Testing: found[0].name is "<<string(found[0].name.begin(), found[0].name.end())<<'\n';
    //std::cerr<<"Testing: found[0].data.size() is "<<found[0].data.size()<<'\n';
    //std::cerr<<"Testing: data size should be "<<attributes1[2].data.size()<<'\n';
    //if (found.size() > 0) {
      //std::cerr<<"Data bytes are: ";
      //std::for_each(found[0].data.begin(), found[0].data.end(), [&](uint8_t c) {std::cerr<<(uint32_t)c<<'\t';});
      //std::cerr<<'\n';
    //}
    //if (found.size() > 0) {
      //std::cerr<<"Data bytes should be: ";
      //std::for_each(attributes1[2].data.begin(), attributes1[2].data.end(), [&](uint8_t c) {std::cerr<<(uint32_t)c<<'\t';});
      //std::cerr<<'\n';
    //}
    if (found.size() == 1 and
        found[0].name == u"att3" and
        found[0].data.size() == attributes1[2].data.size() and
        equal(found[0].data.begin(), found[0].data.end(), attributes1[2].data.begin())) {
      return true;
    }
    else {
      return false;
    }
  }
}

//Assumes that the data has already been inserted
bool testHistoricSnapshot2(WorldModel& wm) {
  vector<u16string> search_atts{u"att3"};
  WorldModel::world_state ws = wm.historicSnapshot(uri1, search_atts, 0, 200);
  if (ws.end() == ws.find(uri1)) {
    //std::cerr<<"Testing: URI not found\n";
    return false;
  }
  else {
    vector<Attribute> found = ws[uri1];
    //std::cerr<<"Testing: found.size() is "<<found.size()<<'\n';
    //std::cerr<<"Testing: found[0].name is "<<string(found[0].name.begin(), found[0].name.end())<<'\n';
    //std::cerr<<"Testing: found[0].data.size() is "<<found[0].data.size()<<'\n';
    //std::cerr<<"Testing: data size should be "<<attributes2[2].data.size()<<'\n';
    //if (found.size() > 0) {
      //std::cerr<<"Data bytes are: ";
      //std::for_each(found[0].data.begin(), found[0].data.end(), [&](uint8_t c) {std::cerr<<(uint32_t)c<<'\t';});
      //std::cerr<<'\n';
    //}
    //if (found.size() > 0) {
      //std::cerr<<"Data bytes should be: ";
      //std::for_each(attributes2[2].data.begin(), attributes2[2].data.end(), [&](uint8_t c) {std::cerr<<(uint32_t)c<<'\t';});
      //std::cerr<<'\n';
    //}
    if (found.size() == 1 and
        found[0].name == u"att3" and
        found[0].data.size() == attributes2[2].data.size() and
        equal(found[0].data.begin(), found[0].data.end(), attributes2[2].data.begin())) {
      return true;
    }
    else {
      return false;
    }
  }
}

//Assumes that the data has already been inserted
bool testHistoricRange(WorldModel& wm) {
  vector<u16string> search_atts{u"att3"};
  WorldModel::world_state ws = wm.historicDataInRange(uri1, search_atts, 0, 200);
  if (ws.end() == ws.find(uri1)) {
    return false;
  }
  else {
    vector<Attribute> found = ws[uri1];
    if (found.size() == 2 and
        found[0].name == u"att3" and
        found[1].name == u"att3" and
        found[0].data.size() == attributes1[2].data.size() and
        found[1].data.size() == attributes2[2].data.size() and
        equal(found[0].data.begin(), found[0].data.end(), attributes1[2].data.begin()) and
        equal(found[1].data.begin(), found[1].data.end(), attributes2[2].data.begin())) {
      return true;
    }
    else {
      return false;
    }
  }
}

//Assumes that URI was inserted
bool testExpireURI1(WorldModel& wm) {
  wm.expireURI(uri1, 210);
  //Try searching for this URI now
  vector<URI> found = wm.searchURI(u"test.*");
  if (any_of(found.begin(), found.end(), [&](URI& uri) { return uri == uri1;})) {
    std::cerr<<"testExpireURI still sees uri in searchURI\n";
    return false;
  }
  else {
    //Now check that a historic query still finds the URI
    vector<u16string> search_atts{u"att3"};
    WorldModel::world_state ws = wm.historicSnapshot(uri1, search_atts, 0, 200);
    if (ws.end() == ws.find(uri1)) {
      std::cerr<<"testExpireURI did not see uri in historicSnapshot\n";
      return false;
    }
    else {
      vector<Attribute> found = ws[uri1];
      if (found.size() == 1 and
          found[0].name == u"att3" and
          found[0].data.size() == attributes1[2].data.size() and
          equal(found[0].data.begin(), found[0].data.end(), attributes1[2].data.begin())) {
        return true;
      }
      else {
        std::cerr<<"testExpireURI did not see matching data in the historicSnapshot\n";
        std::cerr<<"Found.size is "<<found.size()<<'\n';
        if (found.size() == 1) {
          if (found[0].name != u"att3") {
            std::cerr<<"Name doesn't match\n";
            std::cerr<<"Bytes are:\n\t";
            for (char16_t c : found[0].name) {
              std::cerr<<(uint32_t)c<<'\t';
            }
            std::cerr<<'\n';
          }
          else if (found[0].data.size() != attributes1[2].data.size()) {
            std::cerr<<"Data size doesn't match ("<<found[0].data.size()<<" instead of "<<attributes1[2].data.size()<<")\n";
          }
          else {
            std::string data1 = std::accumulate(found[0].data.begin(), found[0].data.end(), std::string(""), [&](std::string s, uint8_t d) {
                return s + std::string(":") + std::to_string(d);});
            std::string data2 = std::accumulate(attributes1[2].data.begin(), attributes1[2].data.end(), std::string(""), [&](std::string s, uint8_t d) {
                return s + std::string(":") + std::to_string(d);});
            std::cerr<<"Data values don't match ("<<data1<<" instead of "<<data2<<")\n";
          }
        }
        return false;
      }
    }
  }
}

//Assumes that URI was inserted
bool testExpireURI(WorldModel& wm) {
  wm.expireURI(uri1, 210);
  //Try searching for this URI now
  vector<URI> found = wm.searchURI(u"test.*");
  if (any_of(found.begin(), found.end(), [&](URI& uri) { return uri == uri1;})) {
    std::cerr<<"testExpireURI still sees uri in searchURI\n";
    return false;
  }
  else {
    //Now check that a historic query still finds the URI
    vector<u16string> search_atts{u"att3"};
    WorldModel::world_state ws = wm.historicSnapshot(uri1, search_atts, 0, 200);
    if (ws.end() == ws.find(uri1)) {
      std::cerr<<"testExpireURI did not see uri in historicSnapshot\n";
      return false;
    }
    else {
      vector<Attribute> found = ws[uri1];
      if (found.size() == 1 and
          found[0].name == u"att3" and
          found[0].data.size() == attributes2[2].data.size() and
          equal(found[0].data.begin(), found[0].data.end(), attributes2[2].data.begin())) {
        return true;
      }
      else {
        std::cerr<<"testExpireURI did not see matching data in the historicSnapshot\n";
        std::cerr<<"Found.size is "<<found.size()<<'\n';
        if (found.size() == 1) {
          if (found[0].name != u"att3") {
            std::cerr<<"Name doesn't match\n";
            std::cerr<<"Bytes are:\n\t";
            for (char16_t c : found[0].name) {
              std::cerr<<(uint32_t)c<<'\t';
            }
            std::cerr<<'\n';
          }
          else if (found[0].data.size() != attributes2[2].data.size()) {
            std::cerr<<"Data size doesn't match ("<<found[0].data.size()<<" instead of "<<attributes2[2].data.size()<<")\n";
          }
          else {
            std::string data1 = std::accumulate(found[0].data.begin(), found[0].data.end(), std::string(""), [&](std::string s, uint8_t d) {
                return s + std::string(":") + std::to_string(d);});
            std::string data2 = std::accumulate(attributes2[2].data.begin(), attributes2[2].data.end(), std::string(""), [&](std::string s, uint8_t d) {
                return s + std::string(":") + std::to_string(d);});
            std::cerr<<"Data values don't match ("<<data1<<" instead of "<<data2<<")\n";
          }
        }
        return false;
      }
    }
  }
}

bool testExpireAttributes(WorldModel& wm) {
  wm.expireURIAttributes(uri1, attributes2, 210);
  //Try searching for this URI now to verify is has been expired
  vector<u16string> search_atts{u"att3"};
  WorldModel::world_state ws = wm.currentSnapshot(uri1, search_atts, true);
  //We should fail to match anything because the attribute was not found
  if (ws.end() != ws.find(uri1)) {
    return false;
  }
  else {
    //Now check that a historic query still finds the attribute
    vector<u16string> search_atts{u"att3"};
    WorldModel::world_state ws = wm.historicSnapshot(uri1, search_atts, 0, 200);
    if (ws.end() == ws.find(uri1)) {
      return false;
    }
    else {
      vector<Attribute> found = ws[uri1];
      if (found.size() == 1 and
          found[0].name == u"att3" and
          found[0].data.size() == attributes2[2].data.size() and
          equal(found[0].data.begin(), found[0].data.end(), attributes2[2].data.begin())) {
        return true;
      }
      else {
        return false;
      }
    }
  }
}

bool testDeleteAttributes(WorldModel& wm) {
  wm.deleteURIAttributes(uri1, attributes2);
  //Try searching for this URI now to verity is has been expired
  vector<u16string> search_atts{u"att3"};
  WorldModel::world_state ws = wm.currentSnapshot(uri1, search_atts, true);
  vector<URI> found = wm.searchURI(uri1);
  //Should not find the URI because the attribute search does not match
  //but URI search should find it
  if (none_of(found.begin(), found.end(), [&](URI& uri) { return uri == uri1;}) or
      (ws.end() != ws.find(uri1))) {
    cerr<<"URI search not working correctly after deleting attribute.\n";
    return false;
  }
  else {
    //Now check that a historic query also does not find this attribute
    vector<u16string> search_atts{u"att3"};
    WorldModel::world_state ws = wm.historicSnapshot(uri1, search_atts, 0, 200);
    if (ws.end() == ws.find(uri1)) {
      return true;
    }
    else {
      return false;
    }
  }
}

//Assumes that URI was inserted
bool testDeleteURI(WorldModel& wm) {
  wm.deleteURI(uri1);
  //Try searching for this URI now
  vector<URI> found = wm.searchURI(u"test.*");
  if (any_of(found.begin(), found.end(), [&](URI& uri) { return uri == uri1;})) {
    return false;
  }
  else {
    //Now check that a historic query does not find the URI
    vector<u16string> search_atts{u"att3"};
    WorldModel::world_state ws = wm.historicSnapshot(uri1, search_atts, 0, 200);
    if (ws.end() == ws.find(uri1)) {
      return true;
    }
    else {
      return false;
    }
  }
}

//Not testing the validity of anything, just testing that nothing crashes.
//Assumes that uri1 was already created.
void insertingThread(WorldModel* wm_p, u16string att_name, size_t num_insertions) {
  WorldModel& wm = *wm_p;
  vector<Attribute> attributes{
    Attribute{att_name, 0, 0, u"test_world_model", {2,3}}};

  for (size_t insertion = 1; insertion <= num_insertions; ++insertion) {
    attributes[0].creation_date = insertion;
    //wm.insertData(uri1, attributes);
    wm.insertData(vector<pair<URI, vector<Attribute>>>{make_pair(uri1, attributes)});
  }
}

//Not testing the validity of anything, just testing that nothing crashes.
void readingThread(WorldModel* wm_p, u16string att_name, size_t num_reads) {
  WorldModel& wm = *wm_p;
  vector<u16string> search_atts{att_name};

  for (size_t read = 1; read <= num_reads; ++read) {
    WorldModel::world_state ws = wm.currentSnapshot(uri1, search_atts, true);
  }
}

//Sets success to false on failure and leaves it alone otherwise
void readWriteThread(WorldModel* wm_p, u16string att_name, size_t num_read_write, bool* success) {
  WorldModel& wm = *wm_p;
  vector<u16string> search_atts{att_name};
  vector<Attribute> attributes{
    Attribute{att_name, 0, 0, u"test_world_model", {2,3}}};

  for (size_t cycle = 1; cycle <= num_read_write; ++cycle) {
    //Insert data
    attributes.at(0).creation_date = cycle;
    //grail_time before = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    //wm.insertData(uri1, attributes);
    wm.insertData(vector<pair<URI, vector<Attribute>>>{make_pair(uri1, attributes)});
    //std::cerr<<"Time to insert: "<<duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count() - before<<'\n';
    //before = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    //Alternate between taking snapshots and taking historic queries
    if (cycle % 2 == 1) {
      WorldModel::world_state ws = wm.currentSnapshot(uri1, search_atts, true);
      if (ws.end() == ws.find(uri1)) {
        std::cerr<<"Thread failed current snapshot\n";
        *success = false;
        //return;
      }
      else {
        WorldModel::world_state::const_iterator found = ws.find(uri1);
        if (ws.count(uri1) == 1 and
            found->second.at(0).name == att_name and
            found->second.at(0).data.size() == attributes.at(0).data.size() and
            equal(found->second.at(0).data.begin(), found->second.at(0).data.end(), attributes.at(0).data.begin())) {
          //std::cerr<<"Time to current snapshot: "<<duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count() - before<<'\n';
          ;
        }
        else {
          std::cerr<<"Thread failed current snapshot\n";
          *success = false;
          //return;
        }
      }
    }
    else {
      if (cycle % 3 == 1) {
        WorldModel::world_state ws = wm.historicDataInRange(uri1, search_atts, 0, cycle);
        if (ws.end() == ws.find(uri1)) {
          std::cerr<<"Thread failed range request: uri not found\n";
          *success = false;
          //return;
        }
        else {
          vector<Attribute> found = ws[uri1];
          //Should find as many as we've inserted in this thread
          if (found.size() != cycle) {
						std::cerr<<"Thread failed range request: wrong number of attributes returned\n";
						std::cout<<"Found "<<found.size()<<" but expected "<<cycle<<'\n';
						*success = false;
					}
					else if (not all_of(found.begin(), found.end(), [&](Attribute& att) { return att.name == att_name;})) {

						std::cerr<<"Thread failed range request: non-matching attributes returned\n";
						*success = false;
            //return;
          }
          //std::cerr<<"Time to data in range: "<<duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count() - before<<'\n';
        }
      }
      else {
        WorldModel::world_state ws = wm.historicSnapshot(uri1, search_atts, 0, cycle);
        if (ws.end() == ws.find(uri1)) {
          std::cerr<<"historic snapshot failed to find uri!\n";
          *success = false;
          //return;
        }
        else {
          vector<Attribute> found = ws[uri1];
          //std::cerr<<"Testing: found.size() is "<<found.size()<<'\n';
          //std::cerr<<"Testing: found[0].name is "<<string(found[0].name.begin(), found[0].name.end())<<'\n';
          //std::cerr<<"Testing: found[0].data.size() is "<<found[0].data.size()<<'\n';
          //std::cerr<<"Testing: data size should be "<<attributes[0].data.size()<<'\n';
          //if (found.size() > 0) {
          //std::cerr<<"Data bytes are: ";
          //std::for_each(found[0].data.begin(), found[0].data.end(), [&](uint8_t c) {std::cerr<<(uint32_t)c<<'\t';});
          //std::cerr<<'\n';
          //}
          //if (found.size() > 0) {
          //std::cerr<<"Data bytes should be: ";
          //std::for_each(attributes[0].data.begin(), attributes[0].data.end(), [&](uint8_t c) {std::cerr<<(uint32_t)c<<'\t';});
          //std::cerr<<'\n';
          //}
          if (found.size() == 1 and
              found[0].name == att_name and
              found[0].data.size() == attributes[0].data.size() and
              equal(found[0].data.begin(), found[0].data.end(), attributes[0].data.begin())) {
            ;
            //std::cerr<<"Time to historic snapshot: "<<duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count() - before<<'\n';
          }
          else {
            std::cerr<<"Thread failed historic snapshot\n";
            *success = false;
            //return;
          }
        }
      }
    }
  }
}

void readAfterWriteThread(WorldModel* wm_p, u16string att_name, size_t num_read_write, bool* success) {
  WorldModel& wm = *wm_p;
  vector<u16string> search_atts{att_name};
  vector<Attribute> attributes{
    Attribute{att_name, 0, 0, u"test_world_model", {2,3}}};

  for (size_t cycle = 1; cycle <= num_read_write; ++cycle) {
    //Alternate between taking snapshots and taking historic queries
    if (cycle % 2 == 1) {
      WorldModel::world_state ws = wm.currentSnapshot(uri1, search_atts, true);
      if (ws.end() == ws.find(uri1)) {
        std::cerr<<"Thread failed current snapshot\n";
        *success = false;
        //return;
      }
      else {
        WorldModel::world_state::const_iterator found = ws.find(uri1);
        if (ws.count(uri1) == 1 and
            found->second.at(0).name == att_name and
            found->second.at(0).data.size() == attributes.at(0).data.size() and
            equal(found->second.at(0).data.begin(), found->second.at(0).data.end(), attributes.at(0).data.begin())) {
          //std::cerr<<"Time to current snapshot: "<<duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count() - before<<'\n';
          ;
        }
        else {
          std::cerr<<"Thread failed current snapshot\n";
          *success = false;
          //return;
        }
      }
    }
    else {
      if (cycle % 3 == 1) {
        WorldModel::world_state ws = wm.historicDataInRange(uri1, search_atts, 0, cycle);
        if (ws.end() == ws.find(uri1)) {
          std::cerr<<"Thread failed range request: uri not found\n";
          *success = false;
          //return;
        }
        else {
          vector<Attribute> found = ws[uri1];
          //Should find as many as we've inserted in this thread
          if (found.size() != cycle) {
						std::cerr<<"Thread failed range request: wrong number of attributes returned\n";
						//std::cout<<"Found "<<found.size()<<" but expected "<<cycle<<'\n';
						*success = false;
					}
					else if (not all_of(found.begin(), found.end(), [&](Attribute& att) { return att.name == att_name;})) {

						std::cerr<<"Thread failed range request: non-matching attributes returned\n";
						*success = false;
            //return;
          }
          //std::cerr<<"Time to data in range: "<<duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count() - before<<'\n';
        }
      }
      else {
        WorldModel::world_state ws = wm.historicSnapshot(uri1, search_atts, 0, cycle);
        if (ws.end() == ws.find(uri1)) {
          std::cerr<<"historic snapshot failed to find uri!\n";
          *success = false;
          //return;
        }
        else {
          vector<Attribute> found = ws[uri1];
          //std::cerr<<"Testing: found.size() is "<<found.size()<<'\n';
          //std::cerr<<"Testing: found[0].name is "<<string(found[0].name.begin(), found[0].name.end())<<'\n';
          //std::cerr<<"Testing: found[0].data.size() is "<<found[0].data.size()<<'\n';
          //std::cerr<<"Testing: data size should be "<<attributes[0].data.size()<<'\n';
          //if (found.size() > 0) {
          //std::cerr<<"Data bytes are: ";
          //std::for_each(found[0].data.begin(), found[0].data.end(), [&](uint8_t c) {std::cerr<<(uint32_t)c<<'\t';});
          //std::cerr<<'\n';
          //}
          //if (found.size() > 0) {
          //std::cerr<<"Data bytes should be: ";
          //std::for_each(attributes[0].data.begin(), attributes[0].data.end(), [&](uint8_t c) {std::cerr<<(uint32_t)c<<'\t';});
          //std::cerr<<'\n';
          //}
          if (found.size() == 1 and
              found[0].name == att_name and
              found[0].data.size() == attributes[0].data.size() and
              equal(found[0].data.begin(), found[0].data.end(), attributes[0].data.begin())) {
            ;
            //std::cerr<<"Time to historic snapshot: "<<duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count() - before<<'\n';
          }
          else {
            std::cerr<<"Thread failed historic snapshot\n";
            *success = false;
            //return;
          }
        }
      }
    }
  }
}

string makeFilename() {
  return string("testdb_") + to_string(random()) + string("_db");
}

int main(int argc, char** argv) {
  uint32_t num_cycles = 100;
  std::function<WorldModel* (std::string)> makeWM;
	//Make sure that different random names are generated each time this is run
	srand(time(NULL));

  std::string username("username");
  std::string password("password");

  //Define different factories for world models here
  std::function<WorldModel* (std::string)> make_sqlite_wm = [](std::string dbname) {
    return new SQLite3WorldModel(dbname);
  };
  std::function<WorldModel* (std::string)> make_mysql_wm = [&](std::string dbname) {
    //Use the existing username and password variables by reference
    return new MysqlWorldModel(dbname, username, password);
  };

  //Set the selected world model factory (default to sqlite3)
  makeWM = make_sqlite_wm;

  if (argc >= 3) {
    for (size_t cur_arg = 1; cur_arg+1 < argc; ++cur_arg) {
      if (string(argv[cur_arg]) == "-c") {
        try {
          num_cycles = stoi(string(argv[cur_arg+1]));
          ++cur_arg;
        }
        catch (std::exception& e) {
          std::cerr<<"Error parsing -c argument as a number: "<<argv[cur_arg+1]<<'\n';
          return 0;
        }
      }
      //Select the type of world model to use
      else if (string(argv[cur_arg]) == "-wm") {
        string wmtype = string(argv[cur_arg+1]);
        ++cur_arg;
        if ("sqlite" == wmtype) {
          makeWM = make_sqlite_wm;
        }
        else if ("mysql" == wmtype) {
          makeWM = make_mysql_wm;
        }
        else {
          std::cerr<<"Unrecognized -wm argument: "<<argv[cur_arg+1]<<'\n';
          std::cerr<<"Expected 'sqlite' or 'mysql'\n";
          return 0;
        }
      }
      //Select the type of world model to use
      else if (string(argv[cur_arg]) == "-u") {
        username = string(argv[cur_arg+1]);
        ++cur_arg;
      }
      //Select the type of world model to use
      else if (string(argv[cur_arg]) == "-p") {
        password = string(argv[cur_arg+1]);
        ++cur_arg;
      }
      else {
        std::cout<<"This program will test the world model program.\n";
        std::cout<<"Optional arguments specify how many cycles of various "<<
          "read and write operations should be performed in the final threaded test,\n";
        std::cout<<"which type of storage to use in the world model, or\n";
        std::cout<<"the user name and password to use in the world model.\n";

        std::cout<<"Usage is: "<<argv[0]<<" [-c #cycles] [-u username] [-p password] [-wm <world model type(sqlite|mysql)>]\n";
        return 0;
      }
    }
  }
  
  //Use this for debugging specific tests (move the tests_start label)
  //goto tests_start;
  goto tests_slow;
  
tests_start:
  
  //Use the random number generator to make filenames for databases
  srandom(time(NULL));
  cerr<<"Testing URI search...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    if (createAndSearchURIs(*wm)) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  cerr<<"Testing that URI pattern search does not match everything...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    if (createAndSearchURIs(*wm) and
        searchSingleURI(*wm)) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  cerr<<"Testing data insertion cannot create URIs...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    if (not insertAndRetrieveAttributes(*wm)) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  cerr<<"Testing data insertion creates URIs when autocreate is set...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    if (insertAndRetrieveAttributesAuto(*wm)) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  //Test retrieving inserted attributes (without data)
  cerr<<"Testing attribute retrieval...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    if (createAndSearchURIs(*wm) and
        insertAndRetrieveAttributes(*wm)) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  //Test retrieving inserted attribute data
  cerr<<"Testing attribute data retrieval...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    if (createAndSearchURIs(*wm) and
        insertAndRetrieveData(*wm)) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  //Test updating a value and verifying that the state changes.
  cerr<<"Testing attribute data updating...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    if (createAndSearchURIs(*wm) and
        insertAndRetrieveData(*wm) and
        insertAndRetrieveData2(*wm)) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  //Test historic snapshot with old value and then up to current values.
  cerr<<"Testing stop/start database reloading...\t";
  {
    std::string fname = makeFilename();
    {
      WorldModel* wm = makeWM(fname);
      createAndSearchURIs(*wm);
      delete wm;
    }
    WorldModel* wm = makeWM(fname);
    if (insertAndRetrieveData(*wm)) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  //Test historic snapshot with old values and then up to current values.
  cerr<<"Testing historic snapshots...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    if (createAndSearchURIs(*wm) and
        insertAndRetrieveData(*wm) and
        insertAndRetrieveData2(*wm) and
        testHistoricSnapshot1(*wm) and
        testHistoricSnapshot2(*wm)) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  //Test range requests
  cerr<<"Testing historic range...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    if (createAndSearchURIs(*wm) and
        insertAndRetrieveData(*wm) and
        insertAndRetrieveData2(*wm) and
        testHistoricRange(*wm)) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  /*
  //Test noncontiguous expired attributes in a historic range requests
  cerr<<"Testing AND query in historic range...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    if (createAndSearchURIs(*wm) and
        insertAndRetrieveData(*wm) and
        insertAndRetrieveData2(*wm) and
        testHistoricRange(*wm)) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }
  */

  //Test expiring URIs from the current world model
  cerr<<"Testing expiring URIs...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    if (createAndSearchURIs(*wm) and
        insertAndRetrieveData(*wm) and
        insertAndRetrieveData2(*wm) and
        testExpireURI(*wm)) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  //Test expiring attributes from the current world model
  cerr<<"Testing expiring URI attributes...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    if (createAndSearchURIs(*wm) and
        insertAndRetrieveData(*wm) and
        insertAndRetrieveData2(*wm) and
        testExpireAttributes(*wm)) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  //Test deleting data from the current world model and database
  cerr<<"Testing deleting URIs...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    if (createAndSearchURIs(*wm) and
        insertAndRetrieveData(*wm) and
        insertAndRetrieveData2(*wm) and
        testDeleteURI(*wm)) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  //Test deleting data from the current world model and database
  cerr<<"Testing deleting attributes...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    if (createAndSearchURIs(*wm) and
        insertAndRetrieveData(*wm) and
        insertAndRetrieveData2(*wm) and
        testDeleteAttributes(*wm)) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  //Test that transient values are not stored
  cerr<<"Testing that transient values are not stored...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    wm->registerTransient(attributes1[2].name, attributes1[2].origin);
    if (createAndSearchURIs(*wm) and
        not insertAndRetrieveData(*wm) and
        not insertAndRetrieveData2(*wm) and //Transient should not show up in the current snapshot
        not testHistoricRange(*wm)) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  //Test that transient values from one origin
  //and non-transient values from another origin
  //co-exist properly (transient are not stored)
  cerr<<"Testing transient/non-transient coexistence...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    //Inserting transient values and non-transient values together.
    //Transient values should not be retrievable while non-transient ones should be.
    wm->registerTransient(attributes1_transient[2].name, attributes1_transient[2].origin);
    if (createAndSearchURIs(*wm) and
        insertAndRetrieveData(*wm) and
        insertAndRetrieveTransientData(*wm) and
        insertAndRetrieveData2(*wm) and //Transient data should not show up in snapshot
        testHistoricRange(*wm)) { //Should pass becaus the transient does not show up in history
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  //Testing that we can subscribe to data using a standing query
  cerr<<"Testing standing queries with transient values...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    vector<u16string> search_atts{u"att3"};
    wm->registerTransient(attributes1[2].name, attributes1[2].origin);
    {
      StandingQuery sq = wm->requestStandingQuery(uri1, search_atts, true);

      if (createAndSearchURIs(*wm) and
          not insertAndRetrieveData(*wm) and  //att3 is transient so won't show up
          checkStandingQuery(sq) and          //att3 should show up in the standing query
          not insertAndRetrieveData2(*wm) and //again, att3 shouldn't show up
          checkStandingQuery2(sq)) {          //att3 should appear again
        cerr<<"Pass\n";
      }
      else {
        cerr<<"Fail\n";
      }
    }
    
    delete wm;
  }

  //Testing that we can subscribe to data using a standing query
  cerr<<"Testing that standing queries update on partial updates...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    vector<u16string> search_atts{u"att3", u"att4"};

    vector<Attribute> attr_update1{
      Attribute{u"att1", 100, 0, u"test_world_model", {0,1,2,3}},
        Attribute{u"att2", 100, 0, u"test_world_model", {0,1,2,3}},
        Attribute{u"att3", 100, 0, u"test_world_model", {0,1,2,3}}};
    vector<Attribute> attr_update2{
        Attribute{u"att4", 110, 0, u"test_world_model", {0,1,2,3}},
        Attribute{u"att5", 110, 0, u"test_world_model", {0,1,2,3}},
        Attribute{u"att6", 110, 0, u"test_world_model", {0,1,2,3}}};
    vector<Attribute> attr_update3{
        Attribute{u"att3", 120, 0, u"test_world_model", {0,1,2,3}}};
    {
      StandingQuery sq = wm->requestStandingQuery(uri1, search_atts, true);

      if (createAndSearchURIs(*wm) and
          wm->insertData(vector<pair<URI, vector<Attribute>>>{make_pair(uri1, attr_update1)}) and
          not checkStandingQueryPartial(sq) and
          wm->insertData(vector<pair<URI, vector<Attribute>>>{make_pair(uri1, attr_update2)}) and
          checkStandingQueryPartial(sq) and
          wm->insertData(vector<pair<URI, vector<Attribute>>>{make_pair(uri1, attr_update3)}) and
          checkStandingQueryPartial2(sq)) {
        cerr<<"Pass\n";
      }
      else {
        cerr<<"Fail\n";
      }
    }
    
    delete wm;
  }

  //Testing that we can subscribe to data using a standing query
  //and that data is overwritten as it is updated
  cerr<<"Testing that standing queries only store current values...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    vector<u16string> search_atts{u"att3"};
    {
      StandingQuery sq = wm->requestStandingQuery(uri1, search_atts, true);

      if (createAndSearchURIs(*wm) and
          insertAndRetrieveData(*wm) and
          insertAndRetrieveData2(*wm) and
          checkStandingQuery2(sq)) {
        cerr<<"Pass\n";
      }
      else {
        cerr<<"Fail\n";
      }
    }
    
    delete wm;
  }

  //Testing that when a standing query is issued it is immediately filled.
  cerr<<"Testing that standing queries immediately update...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    vector<u16string> search_atts{u"att3"};
    {
      if (createAndSearchURIs(*wm) and
          insertAndRetrieveData(*wm)) {
        StandingQuery sq = wm->requestStandingQuery(uri1, search_atts, true);
        if (checkStandingQuery(sq)) {
          cerr<<"Pass\n";
        }
        else {
          cerr<<"Fail\n";
        }
      }
      else {
        cerr<<"Fail\n";
      }
    }
    
    delete wm;
  }

  //Test that standing queries will match a query even if the match
  //is inserted in parts
  cerr<<"Testing that standing queries find matches that are inserted in parts...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    vector<u16string> search_atts{u"att1", u"att2", u"att5", u"att6"};
    {
      StandingQuery sq = wm->requestStandingQuery(uri1, search_atts, true);

      if (createAndSearchURIs(*wm) and
          insertHalfAttributes(*wm) and
          not checkStandingQueryFour(sq) and //Not done yet so should fail
          insertHalfAttributes2(*wm) and
          checkStandingQueryFour(sq)) {
        cerr<<"Pass\n";
      }
      else {
        cerr<<"Fail\n";
      }
    }
    
    delete wm;
  }

  //Test that standing queries will get an update when a match is expired
  cerr<<"Testing that standing queries find updates when items are expired...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    vector<u16string> search_atts{u"att3"};
    {
      StandingQuery sq = wm->requestStandingQuery(uri1, search_atts, true);
      //The threaded standing query sleeps for 5ms when there is no data, so sleep longer
      if (createAndSearchURIs(*wm) and
          insertAndRetrieveData(*wm) and
          checkStandingQuery(sq) and
          testExpireURI1(*wm) and
					not usleep(6000) and
          checkExpiredStandingQuery(sq)) {
        cerr<<"Pass\n";
      }
      else {
        cerr<<"Fail\n";
      }
    }
    
    delete wm;
  }

  //Test that standing queries will get an update when a match is deleted
  cerr<<"Testing that standing queries find updates when items are deleted...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    vector<u16string> search_atts{u"att3"};
    {
      StandingQuery sq = wm->requestStandingQuery(uri1, search_atts, true);
      //The threaded standing query sleeps for 5ms when there is no data, so sleep longer
      if (createAndSearchURIs(*wm) and
          insertAndRetrieveData(*wm) and
          checkStandingQuery(sq) and
          testDeleteURI(*wm) and
					not usleep(6000) and
          checkExpiredStandingQuery(sq)) {
        cerr<<"Pass\n";
      }
      else {
        cerr<<"Fail\n";
      }
    }
    
    delete wm;
  }

  //Test multiple threads inserting values
  cerr<<"Testing threaded insertion...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    wm->createURI(uri1, u"test_world_model", 0);
    vector<thread> test_threads;
    for (size_t i = 0; i < 10; ++i) {
      string num = to_string(i);
      test_threads.push_back(thread(insertingThread, &(*wm), u"att" + u16string(num.begin(), num.end()), num_cycles));
    }
    for_each(test_threads.begin(), test_threads.end(), [&](thread& t) { t.join();});
    delete wm;
  }
  cerr<<"Pass\n";

  //Test multiple threads retrieving values.
  cerr<<"Testing threaded retrieval...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    vector<thread> test_threads;
    createAndSearchURIs(*wm);
    insertAndRetrieveData(*wm);
    insertAndRetrieveData2(*wm);
    for (size_t i = 0; i < 10; ++i) {
      string num = to_string(i);
      test_threads.push_back(thread(readingThread, &(*wm), u"att" + u16string(num.begin(), num.end()), num_cycles));
    }
    for_each(test_threads.begin(), test_threads.end(), [&](thread& t) { t.join();});
    delete wm;
  }
  cerr<<"Pass\n";

tests_slow:
  //Test multiple threads simultaneously inserting and retrieving values.
  cerr<<"Testing simultaneous threaded read/write...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    vector<thread> test_threads;
    wm->createURI(uri1, u"test_world_model", 0);
    bool success = true;
    for (size_t i = 0; i < 10; ++i) {
      string num = to_string(i);
      u16string u_num = u"att" + u16string(num.begin(), num.end());
      test_threads.push_back(thread(readWriteThread, &(*wm), u_num, num_cycles, &success));
    }
    for_each(test_threads.begin(), test_threads.end(), [&](thread& t) { t.join();});
    if (success) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }

  goto FINISH;
  //Test multiple threads simultaneously inserting and after that multiple threads simultaneously reading
  cerr<<"Testing simultaneous threaded write and then multiple threads reading...\t";
  {
    WorldModel* wm = makeWM(makeFilename());
    vector<thread> test_threads;
    wm->createURI(uri1, u"test_world_model", 0);
    bool success = true;
    //First the writing
    {
      vector<thread> test_threads;
      for (size_t i = 0; i < 10; ++i) {
        string num = to_string(i);
        test_threads.push_back(thread(insertingThread, &(*wm), u"att" + u16string(num.begin(), num.end()), num_cycles));
      }
      for_each(test_threads.begin(), test_threads.end(), [&](thread& t) { t.join();});
    }
    //Now the reading
    for (size_t i = 0; i < 10; ++i) {
      string num = to_string(i);
      u16string u_num = u"att" + u16string(num.begin(), num.end());
      test_threads.push_back(thread(readAfterWriteThread, &(*wm), u_num, num_cycles, &success));
    }
    for_each(test_threads.begin(), test_threads.end(), [&](thread& t) { t.join();});
    if (success) {
      cerr<<"Pass\n";
    }
    else {
      cerr<<"Fail\n";
    }
    delete wm;
  }


FINISH:
  return 0;
}

