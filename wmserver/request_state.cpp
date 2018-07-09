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

#include "request_state.hpp"

#include <owl/world_model_protocol.hpp>
using world_model::grail_time;
using world_model::URI;

#include <string>
#include <vector>

RequestState::RequestState(grail_time interval, URI& uri, std::vector<std::u16string>& attributes, uint32_t ticket, StandingQuery sq) : sq(sq) {
	this->interval = interval;
	search_uri = uri;
	desired_attributes = attributes;
	ticket_number = ticket;
	last_serviced = 0;
}

RequestState::RequestState(RequestState&& other) : sq(other.sq) {
	last_serviced = other.last_serviced;
	interval = other.interval;
	search_uri = other.search_uri;
	desired_attributes = other.desired_attributes;
	last_state = other.last_state;
	ticket_number = other.ticket_number;
}

RequestState& RequestState::operator=(RequestState&& other) {
	sq = other.sq;
	last_serviced = other.last_serviced;
	interval = other.interval;
	search_uri = other.search_uri;
	desired_attributes = other.desired_attributes;
	last_state = other.last_state;
	ticket_number = other.ticket_number;
	return *this;
}
