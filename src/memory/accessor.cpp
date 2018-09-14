/**
 * Copyright (c) 2011-2017 libbitcoin developers (see AUTHORS)
 *
 * This file is part of libbitcoin.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#include <bitcoin/database/memory/accessor.hpp>

#include <cstdint>
#include <cstddef>
#include <bitcoin/bitcoin.hpp>
#include <bitcoin/database/define.hpp>

namespace libbitcoin {
namespace database {

accessor::accessor(upgrade_mutex& mutex)
  : mutex_(mutex), data_(nullptr)
{
    ///////////////////////////////////////////////////////////////////////////
    // Begin Critical Section
//    const auto this_id = boost::this_thread::get_id();
//    LOG_DEBUG(LOG_DATABASE)
//    << this_id
//    << " accessor::accessor() calling lock_upgrade() for mutex_ of "
//    << &mutex_;

    mutex_.lock_upgrade();

//    LOG_DEBUG(LOG_DATABASE)
//    << this_id
//    << " accessor::accessor() called lock_upgrade() successfully for mutex_ of "
//    << &mutex_;
}

uint8_t* accessor::buffer()
{
    return data_;
}

// Assign a buffer to this upgradable allocator.
void accessor::assign(uint8_t* data)
{
    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//    const auto this_id = boost::this_thread::get_id();
//    LOG_DEBUG(LOG_DATABASE)
//    << this_id
//    << " accessor::assign() calling unlock_upgrade_and_lock_shared() for mutex_ of "
//    << &mutex_;
    mutex_.unlock_upgrade_and_lock_shared();
//    LOG_DEBUG(LOG_DATABASE)
//    << this_id
//    << " accessor::assign() called unlock_upgrade_and_lock_shared() for mutex_ of "
//    << &mutex_;

    data_ = data;
}

void accessor::increment(size_t value)
{
    BITCOIN_ASSERT_MSG(data_ != nullptr, "Buffer not assigned.");
    BITCOIN_ASSERT((size_t)data_ <= bc::max_size_t - value);

    data_ += value;
}

accessor::~accessor()
{
//    const auto this_id = boost::this_thread::get_id();
/*    LOG_DEBUG(LOG_DATABASE)
    << this_id
    << " accessor::~accessor() calling unlock_upgrade_and_lock_shared() for mutex_ of "
    << &mutex_;

    mutex_.unlock_upgrade_and_lock_shared();

    LOG_DEBUG(LOG_DATABASE)
    << this_id
    << " accessor::~accessor() called unlock_upgrade_and_lock_shared() for mutex_ of "
    << &mutex_;
*/
//    LOG_DEBUG(LOG_DATABASE)
//    << this_id
//    << " accessor::~accessor() calling unlock_shared() for mutex_ of "
//    << &mutex_;

    mutex_.unlock_shared();

//    LOG_DEBUG(LOG_DATABASE)
//    << this_id
//    << " accessor::~accessor() called unlock_shared() successfully for mutex_ of "
//    << &mutex_;
    // End Critical Section
    ///////////////////////////////////////////////////////////////////////////
}

} // namespace database
} // namespace libbitcoin
