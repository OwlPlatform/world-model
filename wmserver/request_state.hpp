/*
 * Copyright (c) 2014 Bernhard Firner
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
 * RequestState class
 * Class with the internal state of streaming requests.
 *
 * @author Bernhard Firner
 ******************************************************************************/

#ifndef __REQUEST_STATE_HPP__
#define __REQUEST_STATE_HPP__

#include <owl/world_model_protocol.hpp>
using world_model::grail_time;
using world_model::URI;

#include <world_model.hpp>

#include <string>
#include <vector>

//A structure used to track the state of streaming requests
class RequestState {
	private:
		RequestState& operator=(const RequestState&) = delete;
		RequestState(const RequestState&) = delete;
	public:
		grail_time last_serviced;
		grail_time interval;
		URI search_uri;
		std::vector<std::u16string> desired_attributes;
		WorldModel::world_state last_state;
		uint32_t ticket_number;
		//Standing query for streaming requests
		StandingQuery sq;
		RequestState(grail_time interval, URI& uri, std::vector<std::u16string>& attributes, uint32_t ticket, StandingQuery sq);
		RequestState(RequestState&& other);
		RequestState& operator=(RequestState&& other);
};

#endif //__REQUEST_STATE_HPP__

