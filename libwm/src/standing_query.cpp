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

#include <set>
#include <string>
#include <utility>

#include <iostream>

#include <standing_query.hpp>

using std::u16string;

///Mutex for the origin_attributes map
std::mutex StandingQuery::origin_attr_mutex;
///Attributes that different origin's will offer
std::map<std::u16string, std::set<std::u16string>> StandingQuery::origin_attributes;

void StandingQuery::addOriginAttributes(std::u16string& origin, std::set<std::u16string>& attributes) {
  std::unique_lock<std::mutex> lck(origin_attr_mutex);
  origin_attributes[origin].insert(attributes.begin(), attributes.end());
}

StandingQuery::StandingQuery(const world_model::URI& uri, const std::vector<std::u16string>& desired_attributes,
    bool get_data) : uri_pattern(uri), desired_attributes(desired_attributes), get_data(get_data) {
  //Set this to true only after all regex patterns have compiled
  regex_valid = false;
  std::string u8_pattern(uri_pattern.begin(), uri_pattern.end());
  int err = regcomp(&uri_regex, u8_pattern.c_str(), REG_EXTENDED);
  if (0 != err) {
    return;
  }
  for (auto I = desired_attributes.begin(); I != desired_attributes.end(); ++I) {
    regex_t re;
    std::string u8_attr_pattern(I->begin(), I->end());
    int err = regcomp(&re, u8_attr_pattern.c_str(), REG_EXTENDED);
    if (0 != err) {
      regfree(&uri_regex);
      for (auto J = attr_regex.begin(); J != attr_regex.end(); ++J) {
        regfree(&(J->second));
      }
      return;
    }
    else {
      attr_regex[*I] = re;
    }
  }
  regex_valid = true;
}

///Free memory from regular expressions
StandingQuery::~StandingQuery() {
  if (regex_valid) {
    regfree(&uri_regex);
    for (auto J = attr_regex.begin(); J != attr_regex.end(); ++J) {
      regfree(&(J->second));
    }
  }
}

///r-value copy constructor
StandingQuery::StandingQuery(StandingQuery&& other) {
  uri_pattern = other.uri_pattern;
  desired_attributes = other.desired_attributes;
  regex_valid = other.regex_valid;
  std::swap(uri_regex, other.uri_regex);
  attr_regex = other.attr_regex;
  other.attr_regex.clear();
  other.regex_valid = false;
  get_data = other.get_data;
}

///r-value assignment
StandingQuery& StandingQuery::operator=(StandingQuery&& other) {
  uri_pattern = other.uri_pattern;
  desired_attributes = other.desired_attributes;
  regex_valid = other.regex_valid;
  std::swap(uri_regex, other.uri_regex);
  attr_regex = other.attr_regex;
  other.attr_regex.clear();
  other.regex_valid = false;
  get_data = other.get_data;
  return *this;
}

/**
 * Return true if this origin has data that this standing query might
 * be interested in and false otherwise.
 */
bool StandingQuery::interestingOrigin(std::u16string& origin) {
  //Fetch the attributes that this origin provides
  std::set<std::u16string> attrs;
  {
    std::unique_lock<std::mutex> lck(origin_attr_mutex);
    //If this wasn't entered into the list then it must be checked
    //This could happen for historic data in the world model for
    //origins that were not connected to the world model since it
    //was started. Since these will not be sending active updates
    //going through the origin tagging process here is probably not
    //worth it so just check the attributes.
    if (origin_attributes.end() == origin_attributes.find(origin)) {
      return true;
    }
    attrs = origin_attributes[origin];
  }
  //Return true if any attributes are of interest
  for (const std::u16string& attr : attrs) {
    //See if we need to check this attribute string against regexes or if the
    //results was already computed
    auto attr_store = attribute_accepted.find(attr);
    if (attribute_accepted.end() == attr_store) {
      std::set<size_t> patt_match;
      std::string name_str = std::string(attr.begin(), attr.end());
      for (size_t search_ind = 0; search_ind < desired_attributes.size(); ++search_ind) {
        //Use regex matching
        regmatch_t pmatch;
        int match = regexec(&attr_regex[desired_attributes[search_ind]],
            name_str.c_str(), 1, &pmatch, 0);
        if (0 == match and 0 == pmatch.rm_so and name_str.size() == pmatch.rm_eo) {
          //Remember that this attribute was matched
          patt_match.insert(search_ind);
        }
      }
      //Now remember which desired attributes this pattern matched.
      attribute_accepted[attr] = patt_match;
      //Stop here if this origin is of interest
      if (not patt_match.empty()) {
        return true;
      }
    }
    //Otherwise checked the cached values
    else {
      //If the map of matches is not empty then there was a match
      if (not attr_store->second.empty()) {
        return true;
      }
    }
  }
  //If no match was found then data from this origin is not interesting
  return false;
}

///Return a subset of the world state that this query is interested in.
StandingQuery::world_state StandingQuery::showInterested(world_state& ws, bool multiple_origins) {
  //Optimize the search if every value in this state comes from the same origin.
  //If this origin is not interesting then don't bother checking its data.
  //This is to avoid checking large numbers of attributes against the uri
  //and attribute regular expressions when this world state contains many entries.
  if (not multiple_origins and attr_regex.size() < ws.size()) {
    //Assume here that the world state does not have any empty vectors
    try {
      if (not interestingOrigin(ws.begin()->second.at(0).origin)) {
        return StandingQuery::world_state();
      }
    }
    //Catch an out of range exception from at()
    catch (std::exception& e) {
    }
    //Just match normally, don't waste time trying to find IDs with attributes
    //if some of them do not have any attributes
  }

  std::vector<world_model::URI> matches;
  //Find matching URIs and remember if they match after
  //doing regexp searches.
  for (auto I = ws.begin(); I != ws.end(); ++I) {
    //First check the cached results
    auto uriI = uri_accepted.find(I->first);
    if (uriI != uri_accepted.end()) {
      if (uriI->second) {
        matches.push_back(I->first);
      }
    }
    //Do a regex and update uri_accepted if no cached result was found
    else {
      regmatch_t pmatch;
      std::string search_id(I->first.begin(), I->first.end());
      int match = regexec(&uri_regex, search_id.c_str(), 1, &pmatch, 0);
      if (0 == match and 0 == pmatch.rm_so and I->first.size() == pmatch.rm_eo) {
        uri_accepted[I->first] = true;
        {
          std::unique_lock<std::mutex> lck(data_mutex);
          current_matches[I->first] = std::set<std::u16string>();
        }
        matches.push_back(I->first);
        //Start off with no matching attributes
        uri_matches[I->first] = std::set<size_t>();
      }
      else {
        uri_accepted[I->first] = false;
      }
    }
  }

  //Now find the attributes of interest for each URI
  //Attribute searches have AND relationships - this URI's results are only
  //matched if all of the attribute search patterns have matches.
  world_state result;
  for (auto uri_match = matches.begin(); uri_match != matches.end(); ++uri_match) {
    std::vector<world_model::Attribute>& uri_partial = partial[*uri_match];
    //Make a vector of attributes to search through
    std::vector<world_model::Attribute> attributes = ws[*uri_match];
    //Make a place to put results for this uri
    std::vector<world_model::Attribute> uri_attributes;
    //Fill in the attribute_accepted map for any unknown attributes
    size_t prev_match_count = uri_matches[*uri_match].size();
    for (auto I = attributes.begin(); I != attributes.end(); ++I) {
      //See if we need to check this attribute string against regexes or if the
      //results was already computed
      auto attr_store = attribute_accepted.find(I->name);
      if (attribute_accepted.end() == attr_store) {
        std::set<size_t> patt_match;
        std::string name_str = std::string(I->name.begin(), I->name.end());
        for (size_t search_ind = 0; search_ind < desired_attributes.size(); ++search_ind) {
          //Use regex matching
          regmatch_t pmatch;
          int match = regexec(&attr_regex[desired_attributes[search_ind]],
              name_str.c_str(), 1, &pmatch, 0);
          if (0 == match and 0 == pmatch.rm_so and name_str.size() == pmatch.rm_eo) {
            //Remember that this attribute was matched
            patt_match.insert(search_ind);
            //Remember that this index was matched
            uri_matches[*uri_match].insert(search_ind);
          }
        }
        //Now remember which desired attributes this pattern matched.
        attribute_accepted[I->name] = patt_match;
      }
      //Otherwise add this attribute's matches to the URI's match results
      else {
        for (auto matched_index = attr_store->second.begin(); matched_index != attr_store->second.end(); ++matched_index) {
          uri_matches[*uri_match].insert(*matched_index);
        }
      }

      //Store this if the attribute matched
      if (not attribute_accepted[I->name].empty()) {
        //Add the attribute to the list of accepted attributes for this insert
        uri_attributes.push_back(*I);
        auto same_attr = std::find_if(uri_partial.begin(), uri_partial.end(), [&](world_model::Attribute& wma) {
            return wma.name == I->name and wma.origin == I->origin;});
        //Update the attribute
        if (same_attr != uri_partial.end()) {
          *same_attr = *I;
        }
        //Or insert the attribute as a new value
        else {
          uri_partial.push_back(*I);
        }
      }
    }
    //See if we matched all attributes
    if (uri_matches[*uri_match].size() == desired_attributes.size()) {
      //If any attributes matched and this is the first time this URI matched all
      //desired attributes then send all attributes stored in partial (which also
      //has the new matches).
      //Otherwise just send the new matches.
      if (desired_attributes.size() == prev_match_count) {
        result[*uri_match] = uri_attributes;
      }
      else {
        result[*uri_match] = uri_partial;
      }
    }
  }
  return result;
}

StandingQuery::world_state StandingQuery::showInterestedTransient(world_state& ws, bool multiple_origins) {
  //Optimize the search if every value in this state comes from the same origin.
  //If this origin is not interesting then don't bother checking its data.
  //This is to avoid checking large numbers of attributes against the uri
  //and attribute regular expressions when this world state contains many entries.
  if (not multiple_origins and attr_regex.size() < ws.size()) {
    //Assume here that the world state does not have any empty vectors
    try {
      if (not interestingOrigin(ws.begin()->second.at(0).origin)) {
        return StandingQuery::world_state();
      }
    }
    //Catch an out of range exception from at()
    catch (std::exception& e) {
    }
    //Just match normally, don't waste time trying to find IDs with attributes
    //if some of them do not have any attributes
  }

  std::vector<world_model::URI> matches;
  //Find matching URIs and remember if they match after
  //doing regexp searches.
  for (auto I = ws.begin(); I != ws.end(); ++I) {
    //First check the cached results
    auto uriI = uri_accepted.find(I->first);
    if (uriI != uri_accepted.end()) {
      if (uriI->second) {
        matches.push_back(I->first);
      }
    }
    //Do a regex and update uri_accepted if no cached result was found
    else {
      regmatch_t pmatch;
      std::string search_id(I->first.begin(), I->first.end());
      int match = regexec(&uri_regex, search_id.c_str(), 1, &pmatch, 0);
      if (0 == match and 0 == pmatch.rm_so and I->first.size() == pmatch.rm_eo) {
        uri_accepted[I->first] = true;
        {
          std::unique_lock<std::mutex> lck(data_mutex);
          current_matches[I->first] = std::set<std::u16string>();
        }
        matches.push_back(I->first);
        //Start off with no matching attributes
        uri_matches[I->first] = std::set<size_t>();
      }
      else {
        uri_accepted[I->first] = false;
      }
    }
  }

  //Now find the attributes of interest for each URI
  //Attribute searches have AND relationships - this URI's results are only
  //matched if all of the attribute search patterns have matches.
  //We don't cache transient matches since they are direct string comparisons
  //Check directly if attribute was requested
  world_state result;
  for (auto uri_match = matches.begin(); uri_match != matches.end(); ++uri_match) {
    //TODO FIXME For transient attributes do not use the uri_partial structure
    //since transient values should not be stored. This also means that the
    //cached uri_matches map should not store matches to transient attributes either.
    std::vector<world_model::Attribute>& uri_partial = partial[*uri_match];
    //Make a vector of attributes to search through
    std::vector<world_model::Attribute> attributes = ws[*uri_match];
    //Make a place to put results for this uri
    std::vector<world_model::Attribute> uri_attributes;
    //Fill in the attribute_accepted map for any unknown attributes
    size_t prev_match_count = uri_matches[*uri_match].size();
    for (auto I = attributes.begin(); I != attributes.end(); ++I) {
      //Use direct string comparison for transients. Don't use the cached
      //map since that just using string comparison again
      std::set<size_t> patt_match;
      std::string name_str = std::string(I->name.begin(), I->name.end());
      for (size_t search_ind = 0; search_ind < desired_attributes.size(); ++search_ind) {
        if (I->name == desired_attributes[search_ind]) {
          //Remember that this attribute was matched
          patt_match.insert(search_ind);
          //Remember that this search index was matched for this URI
          uri_matches[*uri_match].insert(search_ind);
        }
      }
      //Now remember which desired attributes this pattern matched.
      attribute_accepted[I->name] = patt_match;

      //Store this if any attributes matched
      if (not attribute_accepted[I->name].empty()) {
        //Add the attribute to the list of accepted attributes for this insert
        uri_attributes.push_back(*I);
        auto same_attr = std::find_if(uri_partial.begin(), uri_partial.end(), [&](world_model::Attribute& wma) {
            return wma.name == I->name and wma.origin == I->origin;});
        //Update the attribute
        if (same_attr != uri_partial.end()) {
          *same_attr = *I;
        }
        //Or insert the attribute as a new value
        else {
          uri_partial.push_back(*I);
        }
      }
    }
    //See if we matched all attributes for any URIs
    if (uri_matches[*uri_match].size() == desired_attributes.size()) {
      //If any attributes matched and this is the first time this URI matched all
      //desired attributes then send all attributes stored in partial (which also
      //has the new matches).
      //Otherwise just send the new matches.
      if (desired_attributes.size() == prev_match_count) {
        result[*uri_match] = uri_attributes;
      }
      else {
        result[*uri_match] = uri_partial;
      }
    }
  }
  return result;
}

void StandingQuery::expireURI(world_model::URI uri, world_model::grail_time expires) {
  //Make sure we don't store a partial for this if it is expired or deleted.
  partial.erase(uri);
  uri_accepted.erase(uri);
  uri_matches.erase(uri);
  std::unique_lock<std::mutex> lck(data_mutex);
  auto state = cur_state.find(uri);
  //If this data is in the current state then expire all of the attributes
  if (state != cur_state.end()) {
    std::for_each(state->second.begin(), state->second.end(), [&](world_model::Attribute& attr) {
        current_matches[uri].erase(attr.name);
        attr.expiration_date = expires; });
  }
  //If the current state does not have values for some attributes that were
  //previously sent then make sure to expire these as well
  if (current_matches.end() != current_matches.find(uri)) {
    std::set<std::u16string>& attr_names = current_matches[uri];
    for (const std::u16string& attr_name : attr_names) {
      //Push an attribute with the expired attribute's name and no data
      cur_state[uri].push_back(world_model::Attribute{attr_name, expires, expires, u"", {}});
    }
    current_matches.erase(uri);
  }
}

void StandingQuery::expireURIAttributes(world_model::URI uri,
    const std::vector<world_model::Attribute>& entries) {
  using world_model::Attribute;
  std::set<std::pair<u16string, u16string>> is_expired;
  std::for_each(entries.begin(), entries.end(), [&](const Attribute& a) {
      is_expired.insert(std::make_pair(a.name, a.origin));});
  //Make sure we don't store a partial for this if it is expired or deleted.
  {
    auto state = partial.find(uri);
    if (state != partial.end()) {
      std::vector<Attribute>& attr = state->second;
      attr.erase(std::remove_if(attr.begin(), attr.end(), [&](Attribute& a) {
            return 0 < is_expired.count(std::make_pair(a.name, a.origin));}), attr.end());
    }
  }
  {
    std::unique_lock<std::mutex> lck(data_mutex);
    auto state = cur_state.find(uri);
    //If this data is in the current state then expire all of the attributes
    if (state != cur_state.end()) {
      //Function to quickly find to be done entries
      auto tbd = [&](const std::u16string& name) {
        return entries.end() != std::find_if(entries.begin(), entries.end(),
            [&](const Attribute& attr) { return attr.name == name;});};
      std::for_each(state->second.begin(), state->second.end(), [&](Attribute& attr) {
          if (tbd(attr.name)) {
std::cerr<<"Trying to set expiration date of attributes "<<std::string(attr.name.begin(), attr.name.end())<<"\n";
            //TODO FIXME Maybe the expire and delete should be different function
            //calls so that the true expiration date can be represented during expiration?
            //TODO FIXME HERE set expired attributes to expired
            attr.expiration_date = attr.creation_date;
          }});
    }
  }
}

///Insert data in a thread safe way
void StandingQuery::insertData(world_state& ws) {
  std::unique_lock<std::mutex> lck(data_mutex);
  //std::cerr<<"AAAAAAAAAAAAAA INSERTING DATA AAAAAAAAAAAAAA\n";
  for (auto I = ws.begin(); I != ws.end(); ++I) {
    //Update the state with each entry
    std::vector<world_model::Attribute>& state = cur_state[I->first];
    for (auto entry = I->second.begin(); entry != I->second.end(); ++entry) {

      //std::cerr<<"Checking URI, name "<<std::string(I->first.begin(), I->first.end())<<
      //  ", "<<std::string(entry->name.begin(), entry->name.end())<<'\n';
      //Check if there is already an entry with the same name and origin
      auto same_attribute = [&](world_model::Attribute& attr) {
        return (attr.name == entry->name) and (attr.origin == entry->origin);};
      auto slot = std::find_if(state.begin(), state.end(), same_attribute);
      //Update
      if (slot != state.end()) {
        //std::cerr<<"Assigning existing slot\n";
        *slot = *entry;
      }
      //Insert
      else {
        //std::cerr<<"Pushing back new entry\n";
        state.push_back(*entry);
        //Remember that this attribute was stored for this identifier
        current_matches[I->first].insert(entry->name);
      }
    }
  }
  //std::cerr<<"Size is now "<<cur_state.size()<<'\n';
}

///Clear the current data and return what it stored. Thread safe.
StandingQuery::world_state StandingQuery::getData() {
  std::unique_lock<std::mutex> lck(data_mutex);
  world_state data = cur_state;
  cur_state.clear();
  return data;
}

/**
 * Gets updates and clears the current state
 * This should be thread safe in the StandingQuery class
 */
StandingQuery::world_state QueryAccessor::getUpdates() {
  return data->getData();
}

QueryAccessor::QueryAccessor(std::list<StandingQuery>* source, std::mutex* list_mutex,
        std::list<StandingQuery>::iterator data) : source(source), list_mutex(list_mutex), data(data) {;}

///Remove the iterator from the source list.
QueryAccessor::~QueryAccessor() {
  std::unique_lock<std::mutex> lck(*list_mutex);
  if (data != source->end()) {
    source->erase(data);
  }
}

///r-value copy constructor
QueryAccessor::QueryAccessor(QueryAccessor&& other) :
  source(other.source), list_mutex(other.list_mutex) {
  //Lock the list and get its "end"
  std::unique_lock<std::mutex> lck(*list_mutex);
  data = other.data;
  other.data = source->end();
}

///r-value copy constructor
QueryAccessor& QueryAccessor::operator=(QueryAccessor&& other) {
  //Lock the list and get its "end"
  source = other.source;
  list_mutex = other.list_mutex;
  std::unique_lock<std::mutex> lck(*list_mutex);
  data = other.data;
  other.data = source->end();
  return *this;
}

