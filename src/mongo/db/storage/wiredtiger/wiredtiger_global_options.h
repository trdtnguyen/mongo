// wiredtiger_global_options.h

/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {

namespace moe = mongo::optionenvironment;

class WiredTigerGlobalOptions {
public:
    WiredTigerGlobalOptions()
        : cacheSizeGB(0),
          checkpointDelaySecs(0),
          statisticsLogDelaySecs(0),
          directoryForIndexes(false),
          useCollectionPrefixCompression(false),
          useIndexPrefixCompression(false){};

    Status add(moe::OptionSection* options);
    Status store(const moe::Environment& params, const std::vector<std::string>& args);

    double cacheSizeGB;
    size_t checkpointDelaySecs;
    size_t statisticsLogDelaySecs;
    std::string journalCompressor;
    bool directoryForIndexes;
    std::string engineConfig;

    std::string collectionBlockCompressor;
    std::string indexBlockCompressor;
    bool useCollectionPrefixCompression;
    bool useIndexPrefixCompression;
    std::string collectionConfig;
    std::string indexConfig;
//#if defined(UNIV_PMEMOBJ_BUF)
	std::string pmem_home_dir;
	size_t pmem_pool_size;
	size_t pmem_buf_bucket_size;
	size_t pmem_buf_size;
	size_t pmem_buf_max_range;
	size_t pmem_buf_n_buckets;
	double pmem_buf_flush_pct;

	size_t aio_n_slots_per_seg;

//#endif
	size_t pmem_n_flush_threads;
	size_t pmem_flush_threshold;
#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
	size_t pmem_n_space_bits;
	size_t pmem_page_per_bucket_bits;
#endif
};

extern WiredTigerGlobalOptions wiredTigerGlobalOptions;
}
