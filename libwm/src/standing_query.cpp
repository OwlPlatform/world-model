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

#include <iostream>
#include <set>
#include <string>
#include <utility>

#include <standing_query.hpp>
#include <owl/world_model_protocol.hpp>

using std::u16string;
using world_model::WorldState;

//Input/output queue. Input from solver threads, output to standing query thread
std::mutex StandingQuery::solver_data_mutex;
std::queue<StandingQuery::Update> StandingQuery::solver_data;

//Thread that runs the dataProcessingLoop.
std::thread StandingQuery::data_processing_thread;

///Mutex that ensures that only one data_processing_thread runs.
std::mutex StandingQuery::data_processing_mutex;

/**
 * The set of all current standing queries, used to find all of the
 * existing StandingQuery objects so that data can be offered to them.
 */
ThreadsafeSet<StandingQuery*> StandingQuery::subscriptions;

/**
 * A mutex to protect access to the @subscriptions set.
 */
std::mutex StandingQuery::subscription_mutex;

/**
 * The origin_attributes map is used to quickly check if any data from an
 * origin is interesting.
 */
std::map<std::u16string, std::set<std::u16string>> StandingQuery::origin_attributes;

/**
 * Mutex for the origin_attributes map.
 */
std::mutex StandingQuery::origin_attr_mutex;

/**
 * Loop that moves data from the internal data queue to interested client
 * threads.
 */
void StandingQuery::dataProcessingLoop() {
	bool running = true;
	static timespec sleep_interval;
	sleep_interval.tv_sec = 0;
	//Sleep for 5 milliseconds by default, tune according to the rate of new data.
	sleep_interval.tv_nsec = 5000000;

	try {
		while (running) {
			//TODO Put in a timer and fail with an error after too long?
			solver_data_mutex.lock();
			if (not solver_data.empty()) {
				//TODO Move data from @solver_data into individual queries.
				//TODO If there is no more data then sleep for a brief period
				//TODO If there has not been any new data for a long period of time exit the loop
				Update update;

				auto push = [&](StandingQuery* sq) {
					//Check for invalidation from expiration/deletion
					if (update.invalidate_attributes) {
						for (auto& I : update.state) {
							sq->invalidateAttributes(I.first, I.second);
						}
					}
					else if (update.invalidate_objects) {
						for (auto& I : update.state) {
							//Invalidating an ID requires an update to the creation attribute
							if (not I.second.empty() and I.second[0].name == u"creation") {
								sq->invalidateObject(I.first, I.second[0]);
							}
						}
					}
					else {
						//First see what items are of interest. This also tells the standing
						//query to remember partial matches so we do not need to keep feeding
						//it the current state, only the updates.
						auto ws = sq->showInterested(update.state);
						//Insert the data.
						if (not ws.empty()) {
							sq->insertData(ws);
						}
						/* TODO FIXME Handle transients in the queue -- have a separate transient queue?
						//Insert transients separately from normal data to enforce exact string matching
						ws = sq->showInterestedTransient(transients);
						if (not ws.empty()) {
						sq->insertData(ws);
						}
						*/
					}
				};
				while (not solver_data.empty()) {
					update = solver_data.front();
					solver_data.pop();
					StandingQuery::for_each(push);
				}
				solver_data_mutex.unlock();
			}
			else {
				solver_data_mutex.unlock();
				timespec remaining;
				nanosleep(&sleep_interval, &remaining);
				//TODO See if sleep was interrupted
				//TODO Tune the sleep interval to anticipate data arrival
			}
		}
	}
	catch (std::exception err) {
		std::cerr<<"Error in streaming data thread: "<<err.what()<<'\n';
	}

	//Unlock the mutex so that a new data processing thread can spawn.
	data_processing_mutex.unlock();
}

/**
 * Offer data from the input queue for every StandingQuery
 */
void StandingQuery::offerData(WorldState& ws, bool invalidate_attributes, bool invalidate_objects) {
	//TODO Put in a timer and fail with an error after too long?
	{
		std::unique_lock<std::mutex> lck(solver_data_mutex);
		solver_data.push(Update{ws, invalidate_attributes, invalidate_objects});
	}
	//Spawn the dataProcessingLoop thread if it is not running
	{
		if (data_processing_mutex.try_lock()) {
			//The data processing thread will unlock the mutex at exit, allowing a
			//new thread to spawn if it exits.
			data_processing_thread = std::thread(dataProcessingLoop);
			data_processing_thread.detach();
		}
	}
}

void StandingQuery::addOriginAttributes(std::u16string& origin, std::set<std::u16string>& attributes) {
  std::unique_lock<std::mutex> lck(origin_attr_mutex);
  origin_attributes[origin].insert(attributes.begin(), attributes.end());
}

void StandingQuery::for_each(std::function<void(StandingQuery*)> f) {
	subscriptions.for_each(f);
}

StandingQuery::StandingQuery(WorldState& cur_state, const world_model::URI& uri,
		const std::vector<std::u16string>& desired_attributes, bool get_data) :
	uri_pattern(uri), desired_attributes(desired_attributes), get_data(get_data) {
	//Add this standing query into the subscriptions set so that it receives
	//updates from the @data_processing_thread
	subscriptions.insert(this);

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
	//Set up initial data from the current state.
  WorldState ws = this->showInterested(cur_state, true);
  this->insertData(ws);
}

///Free memory from regular expressions
StandingQuery::~StandingQuery() {
  //Delete all of the regex used in the standing standing queries
  if (regex_valid) {
    //Delete the identifier regex
    regfree(&uri_regex);
    //Now delete the attribute regex patterns
    for (auto J = attr_regex.begin(); J != attr_regex.end(); ++J) {
      regfree(&(J->second));
    }
  }
  //Remove this standing query into the subscriptions set so that it no longer
  //receives updates from the @data_processing_thread
  subscriptions.erase(this);
}

///Copy constructor
StandingQuery::StandingQuery(const StandingQuery& other) {
  uri_pattern = other.uri_pattern;
  desired_attributes = other.desired_attributes;

	//Set this to true only after all regex patterns have compiled
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

  get_data = other.get_data;

	//Lock other query and copy its data
	{
		//TODO FIXME Is this lock required? Can't do it in a const constructor
		//std::unique_lock<std::mutex> lck(other.data_mutex);
		cur_state = other.cur_state;
		partial = other.partial;
	}
  //Add this standing query into the subscriptions set so that it receives
  //updates from the @data_processing_thread
  subscriptions.insert(this);
}

///Assignment
StandingQuery& StandingQuery::operator=(const StandingQuery& other) {
	//Clear old regex if there was one
  if (regex_valid) {
    regfree(&uri_regex);
    for (auto J = attr_regex.begin(); J != attr_regex.end(); ++J) {
      regfree(&(J->second));
    }
  }
  uri_pattern = other.uri_pattern;
  desired_attributes = other.desired_attributes;

	//Set this to true only after all regex patterns have compiled
	regex_valid = false;
	std::string u8_pattern(uri_pattern.begin(), uri_pattern.end());
	int err = regcomp(&uri_regex, u8_pattern.c_str(), REG_EXTENDED);
	if (0 != err) {
		return *this;
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
			return *this;
		}
		else {
			attr_regex[*I] = re;
		}
	}
	regex_valid = true;

  get_data = other.get_data;

	//Lock other query and copy its data
	{
		//TODO FIXME Is this lock required? Can't do it in a const constructor
		//std::unique_lock<std::mutex> lck(other.data_mutex);
		cur_state = other.cur_state;
		partial = other.partial;
	}
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
    //Otherwise check the cached values
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
WorldState StandingQuery::showInterested(WorldState& ws, bool multiple_origins) {
  //Optimize the search if every value in this state comes from the same origin.
  //If this origin is not interesting then don't bother checking its data.
  //This is to avoid checking large numbers of attributes against the uri
  //and attribute regular expressions when this world state contains many entries.
  if (not multiple_origins and attr_regex.size() < ws.size()) {
    //Assume here that the world state does not have any empty vectors
    try {
      if (not interestingOrigin(ws.begin()->second.at(0).origin)) {
        return WorldState();
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
  WorldState result;
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

WorldState StandingQuery::showInterestedTransient(WorldState& ws, bool multiple_origins) {
  //Optimize the search if every value in this state comes from the same origin.
  //If this origin is not interesting then don't bother checking its data.
  //This is to avoid checking large numbers of attributes against the uri
  //and attribute regular expressions when this world state contains many entries.
  if (not multiple_origins and attr_regex.size() < ws.size()) {
    //Assume here that the world state does not have any empty vectors
    try {
      if (not interestingOrigin(ws.begin()->second.at(0).origin)) {
        return WorldState();
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
  WorldState result;
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

void StandingQuery::invalidateObject(world_model::URI name, world_model::Attribute creation) {
  //Make sure we don't store a partial for this if it is expired or deleted.
  partial.erase(name);
  uri_accepted.erase(name);
  uri_matches.erase(name);
  std::unique_lock<std::mutex> lck(data_mutex);
  auto state = cur_state.find(name);
  //If this data is in the current state then expire all of the attributes
  if (state != cur_state.end()) {
    std::for_each(state->second.begin(), state->second.end(), [&](world_model::Attribute& attr) {
				//Remove this from the cached matches of this identifier and set an
				//expiration date in the current state
        current_matches[name].erase(attr.name);
        attr.expiration_date = creation.expiration_date; });
  }
	//The attributes of the expired identifier that were in the current state were
	//expired, but now make sure that all attributes ever sent from this request
	//are also expired.
  if (current_matches.end() != current_matches.find(name)) {
    std::set<std::u16string>& attr_names = current_matches[name];
    for (const std::u16string& attr_name : attr_names) {
      //Push an attribute with the expired attribute's name and no data
			cur_state[name].push_back(world_model::Attribute{attr_name,
					creation.expiration_date, creation.expiration_date, u"", {}});
    }
		//Finally remove this object name from the matches list.
    current_matches.erase(name);
  }
}

void StandingQuery::invalidateAttributes(world_model::URI name,
    std::vector<world_model::Attribute>& attrs_to_remove) {
  using world_model::Attribute;
  std::set<std::pair<u16string, u16string>> is_expired;
  std::for_each(attrs_to_remove.begin(), attrs_to_remove.end(), [&](const Attribute& a) {
      is_expired.insert(std::make_pair(a.name, a.origin));});
  //Make sure we don't store a partial for this if it is expired or deleted.
  {
    auto state = partial.find(name);
    if (state != partial.end()) {
      std::vector<Attribute>& attrs = state->second;
      attrs.erase(std::remove_if(attrs.begin(), attrs.end(), [&](Attribute& a) {
            return 0 < is_expired.count(std::make_pair(a.name, a.origin));}), attrs.end());
    }
  }
	//Function to quickly find to be deleted entries
	auto tbd = [&](const std::u16string& attr_name) {
		auto check = [&](const Attribute& attr) { return attr.name == attr_name;};
		return std::find_if(attrs_to_remove.begin(), attrs_to_remove.end(), check);
	};
  {
    std::unique_lock<std::mutex> lck(data_mutex);
		//If this object is in the current state then those updated attributes
		//should receive an expiration date.
    auto state = cur_state.find(name);
    //If this data is in the current state then expire it
    if (state != cur_state.end()) {
			//Expire each attribute that is to be deleted
      std::for_each(state->second.begin(), state->second.end(), [&](Attribute& attr) {
					std::vector<world_model::Attribute>::iterator match = tbd(attr.name);
          if (attrs_to_remove.end() != match) {
            //Set expired attributes to expired
            attr.expiration_date = match->expiration_date;
            //Remove the attribute from the current matches set
            current_matches[name].erase(attr.name);
          }});
    }
    //The current state may not have every attribute ever sent (if they haven't
		//been udpated since the last time data was read). We need to expire older
		//attributes here.
    if (current_matches.end() != current_matches.find(name)) {
			//Find the transmitted attributes of the object with this name
      std::set<std::u16string>& attr_names = current_matches[name];
      for (const std::u16string& attr_name : attr_names) {
				std::vector<world_model::Attribute>::iterator match = tbd(attr_name);
				if (attrs_to_remove.end() != match) {
					//Push an attribute with the expired attribute's name and no data
					cur_state[name].push_back(world_model::Attribute{attr_name, match->expiration_date, match->expiration_date, u"", {}});
				}
      }
    }
  }
}

///Insert data in a thread safe way
void StandingQuery::insertData(WorldState& ws) {
  std::unique_lock<std::mutex> lck(data_mutex);
  for (auto I = ws.begin(); I != ws.end(); ++I) {
    //Update the state with each entry
    std::vector<world_model::Attribute>& state = cur_state[I->first];
    for (auto entry = I->second.begin(); entry != I->second.end(); ++entry) {
      //Check if there is already an entry with the same name and origin
      auto same_attribute = [&](world_model::Attribute& attr) {
        return (attr.name == entry->name) and (attr.origin == entry->origin);};
      auto slot = std::find_if(state.begin(), state.end(), same_attribute);
      //Update
      if (slot != state.end()) {
        *slot = *entry;
      }
      //Insert
      else {
        state.push_back(*entry);
        //Remember that this attribute was stored for this identifier
        current_matches[I->first].insert(entry->name);
      }
    }
  }
}

///Clear the current data and return what it stored. Thread safe.
WorldState StandingQuery::getData() {
  std::unique_lock<std::mutex> lck(data_mutex);
  WorldState data = cur_state;
  cur_state.clear();
  return data;
}

