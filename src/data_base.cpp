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
#include <bitcoin/database/data_base.hpp>

#include <algorithm>
#include <cstdint>
#include <cstddef>
#include <functional>
#include <memory>
#include <utility>
#include <boost/filesystem.hpp>
#include <bitcoin/bitcoin.hpp>
#include <bitcoin/database/define.hpp>
#include <bitcoin/database/result/block_result.hpp>
#include <bitcoin/database/settings.hpp>
#include <bitcoin/database/store.hpp>
#include <bitcoin/database/verify.hpp>

namespace libbitcoin {
namespace database {

using namespace std::placeholders;
using namespace boost::filesystem;
using namespace bc::chain;
using namespace bc::machine;
using namespace bc::wallet;

#define NAME "data_base"

// TODO: replace spends with complex query, output gets inpoint:
// (1) transactions_.get(outpoint, require_confirmed)->spender_height.
// (2) blocks_.get(spender_height)->transactions().
// (3) (transactions()->inputs()->previous_output() == outpoint)->inpoint.
// This has the same average cost as 1 output-query + 1/2 block-query.
// This will reduce server indexing by 30% (address indexing only).
// Could make index optional, redirecting queries if not present.

// A failure after begin_write is returned without calling end_write.
// This leaves the local flush lock enabled, preventing usage after restart.

// Construct.
// ----------------------------------------------------------------------------

data_base::data_base(const settings& settings)
  : closed_(true),
    settings_(settings),
    database::store(settings.directory, settings.index_addresses,
        settings.flush_writes)
{
    const auto this_id = boost::this_thread::get_id();

    LOG_DEBUG(LOG_DATABASE)
        << this_id
        << " Buckets: "
        << "block [" << settings.block_table_buckets << "], "
        << "transaction [" << settings.transaction_table_buckets << "], "
        << "address [" << settings.address_table_buckets << "]";
}

data_base::~data_base()
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::~data_base() calling close()";

    close();
}

// Open and close.
// ----------------------------------------------------------------------------

// Throws if there is insufficient disk space, not idempotent.
bool data_base::create( chain::block& genesis)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::create(chain::block& genesis) called.";

    ///////////////////////////////////////////////////////////////////////////
    // Lock exclusive file access.
    if (!store::open())
        return false;

    // Create files.
    if (!create())
        return false;

    start();

    // These leave the databases open.
    bool created = blocks_->create() && transactions_->create();

    if (settings_.index_addresses)
        created = created && addresses_->create();

    created = created && push(genesis) == error::success;

    if (!created)
        return false;

    closed_ = false;
    return created;
}

bool data_base::create( config::block& genesis)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::create(config::block& genesis) called.";

    return create(*(chain::block *)&(genesis)); // cast config::block to chain::block
}

// Must be called before performing queries, not idempotent.
// May be called after stop and/or after close in order to reopen.
bool data_base::open()
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::open() called.";

    ///////////////////////////////////////////////////////////////////////////
    // Lock exclusive file access and conditionally the global flush lock.
    if (!store::open())
        return false;

    start();

    bool opened = blocks_->open() && transactions_->open();

    if (settings_.index_addresses)
        opened = opened && addresses_->open();

    if (!opened)
        return false;

    closed_ = false;
    return opened;
}

// protected
void data_base::start()
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::start() called.";

    blocks_ = std::make_shared<block_database>(block_table, candidate_index,
        confirmed_index, transaction_index, settings_.block_table_buckets,
        settings_.file_growth_rate);

    transactions_ = std::make_shared<transaction_database>(transaction_table,
        settings_.transaction_table_buckets, settings_.file_growth_rate,
        settings_.cache_capacity);

    if (settings_.index_addresses)
    {
        addresses_ = std::make_shared<address_database>(address_table,
            address_rows, settings_.address_table_buckets,
            settings_.file_growth_rate);
    }
}

// protected
void data_base::commit()
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::commit() called.";

    if (settings_.index_addresses)
        addresses_->commit();

    transactions_->commit();
    blocks_->commit();
}

// protected
bool data_base::flush() const
{
    const auto this_id = boost::this_thread::get_id();
    LOG_DEBUG(LOG_DATABASE)
    << this_id
    << " data_base::flush() calling blocks_->flush() transactions_->flush()";

    // Avoid a race between flush and close whereby flush is skipped because
    // close is true and therefore the flush lock file is deleted before close
    // fails. This would leave the database corrupted and undetected. The flush
    // call must execute and successfully flush or the lock must remain.
    ////if (closed_)
    ////    return true;

    bool flushed = blocks_->flush() && transactions_->flush();

    if (settings_.index_addresses)
    {
        LOG_DEBUG(LOG_DATABASE)
        << this_id
        << " data_base::flush() and calling addresses_->flush()";

        flushed = flushed && addresses_->flush();
    }

    LOG_DEBUG(LOG_DATABASE)
        << this_id
        << " data_base::flush() flushed to disk: "
        << code(flushed ? error::success : error::operation_failed).message();

    return flushed;
}

// Close is idempotent and thread safe.
// Optional as the database will close on destruct.
bool data_base::close()
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::close() called.";

    if (closed_)
        return true;

    closed_ = true;

    bool closed = blocks_->close() && transactions_->close();

    if (settings_.index_addresses)
        closed = closed && addresses_->close();

    return closed && store::close();
    // Unlock exclusive file access and conditionally the global flush lock.
    ///////////////////////////////////////////////////////////////////////////
}

// Reader interfaces.
// ----------------------------------------------------------------------------
// public

 block_database& data_base::blocks() const
{
    return *blocks_;
}

 transaction_database& data_base::transactions() const
{
    return *transactions_;
}

// TODO: rename addresses to payments generally.
// Invalid if indexes not initialized.
 address_database& data_base::addresses() const
{
    return *addresses_;
}

// Public writers.
// ----------------------------------------------------------------------------

code data_base::index( transaction& tx)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::index(tx) called.";

    code ec;

    // Existence check prevents duplicated indexing.
    if (!settings_.index_addresses || tx.metadata.existed)
        return ec;

    // Critical Section
    ///////////////////////////////////////////////////////////////////////////
    unique_lock lock(write_mutex_);

    if ((ec = verify_exists(*transactions_, tx)))
        return ec;

    //vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
    conditional_lock flushlock(flush_each_write(), &flush_lock_mutex_);

    if (!begin_write())
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::index begin_write error::store_lock_failure";

        return error::store_lock_failure;
    }
    
    addresses_->index(tx);
    addresses_->commit();

    if (end_write())
    {
        return error::success;
    }
    else
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::index end_write error::store_lock_failure";
        return error::store_lock_failure;
    }

    //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ///////////////////////////////////////////////////////////////////////////
}

code data_base::index( block& block)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::index(block) called.";

    code ec;
    if (!settings_.index_addresses)
        return ec;

    // Critical Section
    ///////////////////////////////////////////////////////////////////////////
    unique_lock lock(write_mutex_);
    
    if ((ec = verify_exists(*blocks_, block.header())))
        return ec;

    //vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
    conditional_lock flushlock(flush_each_write(), &flush_lock_mutex_);

    if (!begin_write())
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::index(block) begin_write error::store_lock_failure";

        return error::store_lock_failure;
    }
    
    // Existence check prevents duplicated indexing.
    for ( auto tx: block.transactions())
        if (!tx.metadata.existed)
            addresses_->index(tx);

    addresses_->commit();

    if (end_write())
    {
        return error::success;
    }
    else
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::index(block) end_write error::store_lock_failure";

        return error::store_lock_failure;
    }
    //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ///////////////////////////////////////////////////////////////////////////
}

code data_base::store( transaction& tx, uint32_t forks)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::store called. tx: " << &tx;

    code ec;

    // Critical Section
    ///////////////////////////////////////////////////////////////////////////
    unique_lock lock(write_mutex_);
    
    // Returns error::duplicate_transaction if tx with same hash exists.
    if ((ec = verify_missing(*transactions_, tx)))
        return ec;

    //vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
    conditional_lock flushlock(flush_each_write(), &flush_lock_mutex_);

    if (!begin_write())
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::store begin_write error::store_lock_failure";
        
        return error::store_lock_failure;
    }
    
    // Store the transaction if missing and always set tx link metadata.
    if (!transactions_->store(tx, forks))
    {
        if (!end_write())
        {
            LOG_VERBOSE(LOG_DATABASE)
            << this_id
            << " data_base::store store end_write error::store_lock_failure";
        }
        return error::operation_failed;
    }

    // TODO: add the tx to unspent transaction cache as unconfirmed.

    transactions_->commit();

    if (end_write())
    {
        return error::success;
    }
    else
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::index(block) end_write error::store_lock_failure";
        
        return error::store_lock_failure;
    }
    //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ///////////////////////////////////////////////////////////////////////////
}

code data_base::reorganize(const config::checkpoint& fork_point,
    header_const_ptr_list_const_ptr incoming,
    header_const_ptr_list_ptr outgoing)
{
    const auto this_id = boost::this_thread::get_id();

    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::reorganize() called.";

    if (fork_point.height() > max_size_t - incoming->size())
        return error::operation_failed;

    const auto result =
        pop_above(outgoing, fork_point) &&
        push_all(incoming, fork_point);

    return result ? error::success : error::operation_failed;
}

// Add missing transactions for an existing block header.
// This allows parallel write when write flushing is not enabled.
code data_base::update( chain::block& block, size_t height)
{
    code ec;
    const auto this_id = boost::this_thread::get_id();

    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::update() called. instantiating conditional_lock lock()";

    // Critical Section
    ///////////////////////////////////////////////////////////////////////////
    unique_lock lock(write_mutex_);
    
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::update() conditional_lock lock() instantiated successfully";

    if ((ec = verify_update(*blocks_, block, height)))
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " error data_base::update() verify_update()"
        << " block height: " << height
        << " error_code: " << ec << ec.message();
        
        return ec;
    }

    // TODO: This could be skipped when stored header's tx count is non-zero.

    // Conditional write mutex preserves write flushing by preventing overlap.
    //vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
    conditional_lock flushlock(flush_each_write(), &flush_lock_mutex_);

    if (!begin_write())
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " error data_base::update() begin_write() error_code: error::store_lock_failure";
        return error::store_lock_failure;
    }
    
    // Store the missing transactions and set tx link metadata for all.
    if (!transactions_->store(block.transactions()))
    {
        if (!end_write())
        {
            LOG_VERBOSE(LOG_DATABASE)
            << this_id
            << " data_base::update store end_write error::store_lock_failure";
        }
        return error::operation_failed;
    }
    
    // Update the block's transaction associations (not its state).
    if (!blocks_->update(block))
    {
        if (!end_write())
        {
            LOG_VERBOSE(LOG_DATABASE)
            << this_id
            << " data_base::update update end_write error::store_lock_failure";
        }
        return error::operation_failed;
    }

    commit();

    if (end_write())
    {
        return error::success;
    }
    else
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::update() end_write error::store_lock_failure";
        
        return error::store_lock_failure;
    }
    //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ///////////////////////////////////////////////////////////////////////////
}

// Promote unvalidated block to valid|invalid based on error value.
code data_base::invalidate( header& header, const code& error)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::invalidate() called. instantiating conditional_lock";

    code ec;

    // Critical Section
    ///////////////////////////////////////////////////////////////////////////
    unique_lock lock(write_mutex_);
    
    if ((ec = verify_exists(*blocks_, header)))
        return ec;

    //vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
    conditional_lock flushlock(flush_each_write(), &flush_lock_mutex_);

    if (!begin_write())
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::invalidate begin_write error::store_lock_failure";

        return error::store_lock_failure;
    }
    
    if (!blocks_->validate(header.hash(), error))
    {
        if (!end_write())
        {
            LOG_VERBOSE(LOG_DATABASE)
            << this_id
            << " data_base::invalidate validate end_write error::store_lock_failure";
        }
        return error::operation_failed;
    }
    
    header.metadata.error = error;
    header.metadata.validated = true;

    if (end_write())
    {
        return error::success;
    }
    else
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::invalidate() end_write error::store_lock_failure";
        
        return error::store_lock_failure;
    }
    ///////////////////////////////////////////////////////////////////////////
}

// Mark candidate as valid, and txs and outputs spent by them as candidate.
code data_base::candidate( block& block)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::candidate() called";

    code ec;

    // Critical Section
    ///////////////////////////////////////////////////////////////////////////
    unique_lock lock(write_mutex_);
    
    if ((ec = verify_not_failed(*blocks_, block)))
        return ec;

    const auto& header = block.header();
    BITCOIN_ASSERT(!header.metadata.error);

    //vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
    conditional_lock flushlock(flush_each_write(), &flush_lock_mutex_);

    if (!begin_write())
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::candidate begin_write error::store_lock_failure";
        
        return error::store_lock_failure;
    }
    
    // Set candidate validation state to valid.
    if (!blocks_->validate(header.hash(), error::success))
    {
        if (!end_write())
        {
            LOG_VERBOSE(LOG_DATABASE)
            << this_id
            << " data_base::candidate validate end_write error::store_lock_failure";
        }
        return error::operation_failed;
    }
    
    // Mark candidate block txs and outputs spent by them as candidate.
    for (const auto& tx: block.transactions())
        if (!transactions_->candidate(tx.metadata.link))
        {
            if (!end_write())
            {
                LOG_VERBOSE(LOG_DATABASE)
                << this_id
                << " data_base::candidate candidate end_write error::store_lock_failure";
            }
            return error::operation_failed;
        }

    header.metadata.error = error::success;
    header.metadata.validated = true;

    if (end_write())
    {
        return error::success;
    }
    else
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::candidate end_write error::store_lock_failure";
        
        return error::store_lock_failure;
    }
    ///////////////////////////////////////////////////////////////////////////
}

// Reorganize blocks.
code data_base::reorganize(const config::checkpoint& fork_point,
    block_const_ptr_list_const_ptr incoming,
    block_const_ptr_list_ptr outgoing)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::reorganize() called";

    if (fork_point.height() > max_size_t - incoming->size())
        return error::operation_failed;

    const auto result =
        pop_above(outgoing, fork_point) &&
        push_all(incoming, fork_point);

    return result ? error::success : error::operation_failed;
}

// TODO: index payments.
// Store, update, validate and confirm the presumed valid block.
code data_base::push( block& block, size_t height,
    uint32_t median_time_past)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::push() called";

    // Critical Section
    ///////////////////////////////////////////////////////////////////////////
    unique_lock lock(write_mutex_);

    //vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
    conditional_lock flushlock(flush_each_write(), &flush_lock_mutex_);

    if (!begin_write())
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::push begin_write error::store_lock_failure";

        return error::store_lock_failure;
    }
    
    // Store the header.
    blocks_->store(block.header(), height, median_time_past);

    // Push header reference onto the candidate index and set candidate state.
    if (!blocks_->index(block.hash(), height, true))
    {
        if (!end_write())
        {
            LOG_VERBOSE(LOG_DATABASE)
            << this_id
            << " data_base::push index candidate end_write error::store_lock_failure";
        }
        return error::operation_failed;
    }

    // Store any missing txs as unconfirmed, set tx link metadata for all.
    if (!transactions_->store(block.transactions()))
    {
        if (!end_write())
        {
            LOG_VERBOSE(LOG_DATABASE)
            << this_id
            << " data_base::push store end_write error::store_lock_failure";
        }
        return error::operation_failed;
    }
    
    // Populate transaction references from link metadata.
    if (!blocks_->update(block))
    {
        if (!end_write())
        {
            LOG_VERBOSE(LOG_DATABASE)
            << this_id
            << " data_base::push update end_write error::store_lock_failure";
        }
        return error::operation_failed;
    }

    // Confirm all transactions (candidate state transition not requried).
    if (!transactions_->confirm(block.transactions(), height, median_time_past))
    {
        if (!end_write())
        {
            LOG_VERBOSE(LOG_DATABASE)
            << this_id
            << " data_base::push confirm end_write error::store_lock_failure";
        }
        return error::operation_failed;
    }

    // Promote validation state to valid (presumed valid).
    if (!blocks_->validate(block.hash(), error::success))
    {
        if (!end_write())
        {
            LOG_VERBOSE(LOG_DATABASE)
            << this_id
            << " data_base::push validate end_write error::store_lock_failure";
        }
        return error::operation_failed;
    }
    
    // Push header reference onto the confirmed index and set confirmed state.
    if (!blocks_->index(block.hash(), height, false))
    {
        if (!end_write())
        {
            LOG_VERBOSE(LOG_DATABASE)
            << this_id
            << " data_base::push index confirmed end_write error::store_lock_failure";
        }
        return error::operation_failed;
    }
    
    commit();

    if (end_write())
    {
        return error::success;
    }
    else
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::push end_write error::store_lock_failure";
        
        return error::store_lock_failure;
    }
    //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ///////////////////////////////////////////////////////////////////////////
}

// Header reorganization.
// ----------------------------------------------------------------------------
// protected

bool data_base::push_all(header_const_ptr_list_const_ptr headers,
    const config::checkpoint& fork_point)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::push_all() called";

    code ec;
    const auto first_height = fork_point.height() + 1;

    // Push all headers onto the fork point.
    for (size_t index = 0; index < headers->size(); ++index)
    {
         auto& header = *((*headers)[index]);
        const auto median_time_past = header.metadata.median_time_past;
        if ((ec = push_header(header, first_height + index, median_time_past)))
            return false;
    }

    return true;
}

bool data_base::pop_above(header_const_ptr_list_ptr headers,
    const config::checkpoint& fork_point)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::pop_above() called";

    code ec;
    headers->clear();
    if ((ec = verify(*blocks_, fork_point, true)))
        return false;

    size_t top;
    if (!blocks_->top(top, true))
        return false;

    const auto fork = fork_point.height();
    const auto depth = top - fork;
    headers->reserve(depth);
    if (depth == 0)
        return true;

    // Pop all headers above the fork point.
    for (size_t height = top; height > fork; --height)
    {
        const auto next = std::make_shared<message::header>();
        if ((ec = pop_header(*next, height)))
            return false;

        headers->insert(headers->begin(), next);
    }

    return true;
}

// Expects header is next candidate and metadata.exists is populated.
// Median time past metadata is populated when the block is validated.
code data_base::push_header( chain::header& header, size_t height,
    uint32_t median_time_past)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::push_header() called";

    code ec;

    // Critical Section
    ///////////////////////////////////////////////////////////////////////////
    unique_lock lock(write_mutex_);

    if ((ec = verify_push(*blocks_, header, height)))
        return ec;

    //vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
    conditional_lock flushlock(flush_each_write(), &flush_lock_mutex_);

    if (!begin_write())
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::push_header begin_write error::store_lock_failure";

        return error::store_lock_failure;
    }
    
    if (!header.metadata.exists)
        blocks_->store(header, height, median_time_past);

    blocks_->index(header.hash(), height, true);
    blocks_->commit();

    if (end_write())
    {
        return error::success;
    }
    else
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::push_header end_write error::store_lock_failure";
        
        return error::store_lock_failure;
    }
    //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ///////////////////////////////////////////////////////////////////////////
}

// Expects header exists at the top of the candidate index.
code data_base::pop_header(chain::header& out_header, size_t height)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::pop_header() called";

    code ec;

    // Critical Section
    ///////////////////////////////////////////////////////////////////////////
    unique_lock lock(write_mutex_);

    if ((verify_top(*blocks_, height, true)))
        return ec;

    const auto result = blocks_->get(height, true);

    if (!result)
        return error::operation_failed;

    //vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
    conditional_lock flushlock(flush_each_write(), &flush_lock_mutex_);

    if (!begin_write())
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::pop_header begin_write error::store_lock_failure";
        
        return error::store_lock_failure;
    }
    
    // Uncandidate previous outputs spent by txs of this candidate block.
    for (const auto link: result)
        if (!transactions_->uncandidate(link))
        {
            if (!end_write())
            {
                LOG_VERBOSE(LOG_DATABASE)
                << this_id
                << " data_base::pop_header uncandidate end_write error::store_lock_failure";
            }
            return error::operation_failed;
        }
    
    // Unindex the candidate header.
    if (!blocks_->unindex(result.hash(), height, true))
    {
        if (!end_write())
        {
            LOG_VERBOSE(LOG_DATABASE)
            << this_id
            << " data_base::pop_header unindex end_write error::store_lock_failure";
        }
        return error::operation_failed;
    }

    // Commit everything that was changed and return header.
    blocks_->commit();
    out_header = result.header();
    BITCOIN_ASSERT(out_header.is_valid());

    if (end_write())
    {
        return error::success;
    }
    else
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::pop_header end_write error::store_lock_failure";
        
        return error::store_lock_failure;
    }
    //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ///////////////////////////////////////////////////////////////////////////
}

// Block reorganization.
// ----------------------------------------------------------------------------
// protected

bool data_base::push_all(block_const_ptr_list_const_ptr blocks,
    const config::checkpoint& fork_point)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::push_all() called";

    code ec;
    const auto first_height = fork_point.height() + 1;

    // Push all blocks onto the fork point.
    for (size_t index = 0; index < blocks->size(); ++index)
    {
         auto& block = *((*blocks)[index]);
        if ((ec = push_block(block, first_height + index)))
            return false;
    }

    return true;
}

bool data_base::pop_above(block_const_ptr_list_ptr blocks,
    const config::checkpoint& fork_point)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::pop_above() called";

    code ec;
    blocks->clear();
    if ((ec = verify(*blocks_, fork_point, false)))
        return false;

    size_t top;
    if (!blocks_->top(top, false))
        return false;

    const auto fork = fork_point.height();
    const auto depth = top - fork;
    blocks->reserve(depth);
    if (depth == 0)
        return true;

    // Pop all blocks above the fork point.
    for (size_t height = top; height > fork; --height)
    {
        const auto next = std::make_shared<message::block>();
        if ((ec = pop_block(*next, height)))
            return false;

        blocks->insert(blocks->begin(), next);
    }

    return true;
}

code data_base::push_block( block& block, size_t height)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::push_block() called";

    code ec;
    BITCOIN_ASSERT(block.header().metadata.state);
    auto median_time_past = block.header().metadata.state->median_time_past();

    // Critical Section
    ///////////////////////////////////////////////////////////////////////////
    unique_lock lock(write_mutex_);

    if ((ec = verify_push(*blocks_, block, height)))
        return ec;

    //vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
    conditional_lock flushlock(flush_each_write(), &flush_lock_mutex_);

    if (!begin_write())
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::push_block error::store_lock_failure";

        return error::store_lock_failure;
    }

    // Confirm txs (and thereby also address indexes), spend prevouts.
    uint32_t position = 0;
    for (const auto& tx: block.transactions())
        if (!transactions_->confirm(tx.metadata.link, height, median_time_past,
            position++))
        {
            if (!end_write())
            {
                LOG_VERBOSE(LOG_DATABASE)
                << this_id
                << " data_base::push_block confirm end_write error::store_lock_failure";
            }
            return error::operation_failed;
        }

    // Confirm candidate block (candidate index unchanged).
    if (!blocks_->index(block.hash(), height, false))
    {
        if (!end_write())
        {
            LOG_VERBOSE(LOG_DATABASE)
            << this_id
            << " data_base::push_block index end_write error::store_lock_failure";
        }
        return error::operation_failed;
    }
    
    commit();

    if (end_write())
    {
        return error::success;
    }
    else
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::push_block end_write error::store_lock_failure";
        
        return error::store_lock_failure;
    }
    //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ///////////////////////////////////////////////////////////////////////////
}

code data_base::pop_block(chain::block& out_block, size_t height)
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::pop_block() called";

    code ec;

    // Critical Section
    ///////////////////////////////////////////////////////////////////////////
    unique_lock lock(write_mutex_);

    if ((ec = verify_top(*blocks_, height, false)))
        return ec;

    const auto result = blocks_->get(height, false);

    if (!result)
        return error::operation_failed;

    // Create a block for walking transactions and return.
    out_block = chain::block(result.header(), to_transactions(result));
    BITCOIN_ASSERT(out_block.hash() == result.hash());

    //vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
    conditional_lock flushlock(flush_each_write(), &flush_lock_mutex_);

    if (!begin_write())
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::pop_block error::store_lock_failure";

        return error::store_lock_failure;
    }
    
    // Deconfirm txs (and thereby also address indexes), unspend prevouts.
    for (const auto& tx: out_block.transactions())
        if (!transactions_->unconfirm(tx.metadata.link))
        {
            if (!end_write())
            {
                LOG_VERBOSE(LOG_DATABASE)
                << this_id
                << " data_base::pop_block unconfirm end_write error::store_lock_failure";
            }
            return error::operation_failed;
        }
    
    // Unconfirm confirmed block (candidate index unchanged).
    if (!blocks_->unindex(result.hash(), height, false))
    {
        if (!end_write())
        {
            LOG_VERBOSE(LOG_DATABASE)
            << this_id
            << " data_base::pop_block unindex end_write error::store_lock_failure";
        }
        return error::operation_failed;
    }
    
    commit();

    BITCOIN_ASSERT(out_block.is_valid());

    if (end_write())
    {
        return error::success;
    }
    else
    {
        LOG_VERBOSE(LOG_DATABASE)
        << this_id
        << " data_base::pop_block end_write error::store_lock_failure";
        
        return error::store_lock_failure;
    }
    //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ///////////////////////////////////////////////////////////////////////////
}

// Utilities.
// ----------------------------------------------------------------------------
// protected

////// TODO: add segwit address indexing.
////void data_base::push_inputs(const transaction& tx)
////{
////    if (tx.is_coinbase())
////        return;
////
////    uint32_t index = 0;
////    const auto& inputs = tx.inputs();
////    const auto link = tx.metadata.link;
////
////    for (const auto& input: inputs)
////    {
////        const auto& prevout = input.previous_output();
////        const payment_record in{ link, index++, prevout.checksum(), false };
////
////        if (prevout.metadata.cache.is_valid())
////        {
////            // This results in a complete and unambiguous history for the
////            // address since standard outputs contain unambiguous address data.
////            for (const auto& address: prevout.metadata.cache.addresses())
////                addresses_->store(address.hash(), in);
////        }
////        else
////        {
////            // For any p2pk spend this creates no record (insufficient data).
////            // For any p2kh spend this creates the ambiguous p2sh address,
////            // which significantly expands the size of the history store.
////            // These are tradeoffs when no prevout is cached (checkpoint sync).
////            for (const auto& address: input.addresses())
////                addresses_->store(address.hash(), in);
////        }
////    }
////}

////// TODO: add segwit address indexing.
////void data_base::push_outputs(const transaction& tx)
////{
////    uint32_t index = 0;
////    const auto& outputs = tx.outputs();
////    const auto link = tx.metadata.link;
////
////    for (const auto& output: outputs)
////    {
////        const payment_record out{ link, index++, output.value(), true };
////
////        // Standard outputs contain unambiguous address data.
////        for (const auto& address: output.addresses())
////            addresses_->store(address.hash(), out);
////    }
////}

// Private (assumes valid result links).
transaction::list data_base::to_transactions(const block_result& result) const
{
    const auto this_id = boost::this_thread::get_id();
    LOG_VERBOSE(LOG_DATABASE)
    << this_id
    << " data_base::to_transactions() called";

    transaction::list txs;
    txs.reserve(result.transaction_count());

    for (const auto link: result)
    {
        const auto tx = transactions_->get(link);
        BITCOIN_ASSERT(tx);
        txs.push_back(tx.transaction());
    }

    return txs;
}

} // namespace database
} // namespace libbitcoin
