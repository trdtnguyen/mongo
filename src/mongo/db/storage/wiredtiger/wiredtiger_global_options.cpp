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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/base/status.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_global_options.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_record_store.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/constraints.h"

namespace mongo {

WiredTigerGlobalOptions wiredTigerGlobalOptions;

Status WiredTigerGlobalOptions::add(moe::OptionSection* options) {
    moe::OptionSection wiredTigerOptions("WiredTiger options");

#if defined(UNIV_PMEMOBJ_BUF)
	//PMEM_BUF options
    wiredTigerOptions.addOptionChaining("storage.wiredTiger.engineConfig.pmem_home_dir",
                                        "pmem_home_dir",
                                        moe::String,
                                        "location of the pmem; "
                                        "default is /mnt/pmem1 ");
    wiredTigerOptions.addOptionChaining("storage.wiredTiger.engineConfig.pmem_pool_size",
                                        "pmem_pool_size",
                                        moe::Long,
                                        "pmemobj pool size in MB 1MB ~ 16GB; "
                                        "default is 8096 MB").validRange(1,16384); 
	wiredTigerOptions.addOptionChaining("storage.wiredTiger.engineConfig.pmem_buf_bucket_size", "pmem_buf_bucket_size", moe::Long, "number of data pages in a bucket; "
                                        "default is 256 pages").validRange(1,65536);
    wiredTigerOptions.addOptionChaining("storage.wiredTiger.engineConfig.pmem_buf_size",
                                        "pmem_buf_size",
                                        moe::Long,
                                        "pm buffer size in MB; " "default is 4096 MB");
    wiredTigerOptions.addOptionChaining("storage.wiredTiger.engineConfig.pmem_buf_n_buckets",
                                        "pmem_buf_n_buckets",
                                        moe::Long,
                                        "number of buckets in the pmem buf; "
                                        "default is 128");
    wiredTigerOptions.addOptionChaining("storage.wiredTiger.engineConfig.pmem_buf_flush_pct",
                                        "pmem_buf_flush_pct",
                                        moe::Double,
                                        "percentage threshold to flush the pmem buf to SSD; "
                                        "default is 0.9");
    wiredTigerOptions.addOptionChaining("storage.wiredTiger.engineConfig.aio_n_slots_per_seg",
                                        "aio_n_slots_per_seg",
                                        moe::Long,
                                        "Max number of slots per segment in AIO, from 1 to 65536; "
                                        "default is 256").validRange(1,65536);

    wiredTigerOptions.addOptionChaining("storage.wiredTiger.engineConfig.pmem_n_flush_threads",
                                        "pmem_n_flush_threads",
                                        moe::Long,
                                        "number of flush threads in the FLUSHER; "
                                        "default is 8").validRange(1,64);
    wiredTigerOptions.addOptionChaining("storage.wiredTiger.engineConfig.pmem_flush_threshold",
                                        "pmem_flush_threshold",
                                        moe::Long,
                                        "the maximum delay flush calls to weakup the flushing threads in FLUSHER; "
                                        "default is number of flush threads - 2").validRange(1,62);
    wiredTigerOptions.addOptionChaining("storage.wiredTiger.engineConfig.pmem_buf_max_range",
                                        "pmem_buf_max_range",
                                        moe::Long,
                                        "max range for hash function; "
                                        "default is 4194304 (2^22)");
#endif //UNIV_PMEMOBJ_BUF

#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
    wiredTigerOptions.addOptionChaining("storage.wiredTiger.engineConfig.pmem_n_space_bits",
                                        "pmem_n_space_bits",
                                        moe::Long,
                                        "Number of bits present a page_no in partition algorithm, from 1 to 32 (space_no is 4-bytes number), default is 5; "
                                        "default is 5").validRange(1,32);
    wiredTigerOptions.addOptionChaining("storage.wiredTiger.engineConfig.pmem_page_per_bucket_bits",
                                        "pmem_page_per_bucket_bits",
                                        moe::Long,
                                        "Number of bits present the maxmum number of pages per space in a bucket in partition algorithm, from 1 to log2(srv_pmem_buf_bucket_size); "
                                        "default is 10").validRange(1,32);
#endif //UNIV_PMEMOBJ_BUF_PARTITION

    // WiredTiger storage engine options
    wiredTigerOptions.addOptionChaining("storage.wiredTiger.engineConfig.cacheSizeGB",
                                        "wiredTigerCacheSizeGB",
                                        moe::Double,
                                        "maximum amount of memory to allocate for cache; "
                                        "defaults to 1/2 of physical RAM");
    wiredTigerOptions
        .addOptionChaining("storage.wiredTiger.engineConfig.statisticsLogDelaySecs",
                           "wiredTigerStatisticsLogDelaySecs",
                           moe::Int,
                           "seconds to wait between each write to a statistics file in the dbpath; "
                           "0 means do not log statistics")
        // FTDC supercedes WiredTiger's statistics logging.
        .hidden()
        .validRange(0, 100000)
        .setDefault(moe::Value(0));
    wiredTigerOptions
        .addOptionChaining("storage.wiredTiger.engineConfig.journalCompressor",
                           "wiredTigerJournalCompressor",
                           moe::String,
                           "use a compressor for log records [none|snappy|zlib]")
        .format("(:?none)|(:?snappy)|(:?zlib)", "(none/snappy/zlib)")
        .setDefault(moe::Value(std::string("snappy")));
    wiredTigerOptions.addOptionChaining("storage.wiredTiger.engineConfig.directoryForIndexes",
                                        "wiredTigerDirectoryForIndexes",
                                        moe::Switch,
                                        "Put indexes and data in different directories");
    wiredTigerOptions
        .addOptionChaining("storage.wiredTiger.engineConfig.configString",
                           "wiredTigerEngineConfigString",
                           moe::String,
                           "WiredTiger storage engine custom "
                           "configuration settings")
        .hidden();

    // WiredTiger collection options
    wiredTigerOptions
        .addOptionChaining("storage.wiredTiger.collectionConfig.blockCompressor",
                           "wiredTigerCollectionBlockCompressor",
                           moe::String,
                           "block compression algorithm for collection data "
                           "[none|snappy|zlib]")
        .format("(:?none)|(:?snappy)|(:?zlib)", "(none/snappy/zlib)")
        .setDefault(moe::Value(std::string("snappy")));
    wiredTigerOptions
        .addOptionChaining("storage.wiredTiger.collectionConfig.configString",
                           "wiredTigerCollectionConfigString",
                           moe::String,
                           "WiredTiger custom collection configuration settings")
        .hidden();


    // WiredTiger index options
    wiredTigerOptions
        .addOptionChaining("storage.wiredTiger.indexConfig.prefixCompression",
                           "wiredTigerIndexPrefixCompression",
                           moe::Bool,
                           "use prefix compression on row-store leaf pages")
        .setDefault(moe::Value(true));
    wiredTigerOptions
        .addOptionChaining("storage.wiredTiger.indexConfig.configString",
                           "wiredTigerIndexConfigString",
                           moe::String,
                           "WiredTiger custom index configuration settings")
        .hidden();

    return options->addSection(wiredTigerOptions);
}

Status WiredTigerGlobalOptions::store(const moe::Environment& params,
                                      const std::vector<std::string>& args) {

#if defined(UNIV_PMEMOBJ_BUF)
    if (params.count("storage.wiredTiger.engineConfig.pmem_home_dir")) {
        wiredTigerGlobalOptions.pmem_home_dir =
            params["storage.wiredTiger.engineConfig.pmem_home_dir"].as<std::string>();
        log() << "PMEM home dir: " << wiredTigerGlobalOptions.pmem_home_dir;
    }
    if (params.count("storage.wiredTiger.engineConfig.pmem_pool_size")) {
        wiredTigerGlobalOptions.pmem_pool_size =
            params["storage.wiredTiger.engineConfig.pmem_pool_size"].as<long>();
        log() << "pmem_pool_size: " << wiredTigerGlobalOptions.pmem_pool_size;
    }
    if (params.count("storage.wiredTiger.engineConfig.pmem_buf_bucket_size")) {
        wiredTigerGlobalOptions.pmem_buf_bucket_size =
            params["storage.wiredTiger.engineConfig.pmem_buf_bucket_size"].as<long>();
        log() << "pmem_buf_bucket_size: " << wiredTigerGlobalOptions.pmem_buf_bucket_size;
    }
    if (params.count("storage.wiredTiger.engineConfig.pmem_buf_size")) {
        wiredTigerGlobalOptions.pmem_buf_size =
            params["storage.wiredTiger.engineConfig.pmem_buf_size"].as<long>();
        log() << "pmem_buf_size: " << wiredTigerGlobalOptions.pmem_buf_size;
    }
    if (params.count("storage.wiredTiger.engineConfig.pmem_buf_n_buckets")) {
        wiredTigerGlobalOptions.pmem_buf_n_buckets =
            params["storage.wiredTiger.engineConfig.pmem_buf_n_buckets"].as<long>();
        log() << "pmem_buf_n_buckets: " << wiredTigerGlobalOptions.pmem_buf_n_buckets;
    }
    if (params.count("storage.wiredTiger.engineConfig.pmem_buf_flush_pct")) {
        wiredTigerGlobalOptions.pmem_buf_flush_pct =
            params["storage.wiredTiger.engineConfig.pmem_buf_flush_pct"].as<double>();
        log() << "pmem_buf_flush_pct: " << wiredTigerGlobalOptions.pmem_buf_flush_pct;
    }
    if (params.count("storage.wiredTiger.engineConfig.aio_n_slots_per_seg")) {
        wiredTigerGlobalOptions.aio_n_slots_per_seg =
            params["storage.wiredTiger.engineConfig.aio_n_slots_per_seg"].as<long>();
        log() << "aio_n_slots_per_seg: " << wiredTigerGlobalOptions.aio_n_slots_per_seg;
    }
    if (params.count("storage.wiredTiger.engineConfig.pmem_n_flush_threads")) {
        wiredTigerGlobalOptions.pmem_n_flush_threads =
            params["storage.wiredTiger.engineConfig.pmem_n_flush_threads"].as<long>();
        log() << "pmem_n_flush_threads: " << wiredTigerGlobalOptions.pmem_n_flush_threads;
    }
    if (params.count("storage.wiredTiger.engineConfig.pmem_flush_threshold")) {
        wiredTigerGlobalOptions.pmem_flush_threshold =
            params["storage.wiredTiger.engineConfig.pmem_flush_threshold"].as<long>();
        log() << "pmem_flush_threshold: " << wiredTigerGlobalOptions.pmem_flush_threshold;
    }
    if (params.count("storage.wiredTiger.engineConfig.pmem_buf_max_range")) {
        wiredTigerGlobalOptions.pmem_buf_max_range =
            params["storage.wiredTiger.engineConfig.pmem_buf_max_range"].as<long>();
        log() << "pmem_buf_max_range: " << wiredTigerGlobalOptions.pmem_buf_max_range;
    }
#endif
#if defined (UNIV_PMEMOBJ_BUF_PARTITION)
    if (params.count("storage.wiredTiger.engineConfig.pmem_n_space_bits")) {
        wiredTigerGlobalOptions.pmem_n_space_bits =
            params["storage.wiredTiger.engineConfig.pmem_n_space_bits"].as<long>();
        log() << "pmem_n_space_bits: " << wiredTigerGlobalOptions.pmem_n_space_bits;
    }
    if (params.count("storage.wiredTiger.engineConfig.pmem_page_per_bucket_bits")) {
        wiredTigerGlobalOptions.pmem_page_per_bucket_bits =
            params["storage.wiredTiger.engineConfig.pmem_page_per_bucket_bits"].as<long>();
        log() << "pmem_page_per_bucket_bits: " << wiredTigerGlobalOptions.pmem_page_per_bucket_bits;
    }
#endif

    // WiredTiger storage engine options
    if (params.count("storage.wiredTiger.engineConfig.cacheSizeGB")) {
        wiredTigerGlobalOptions.cacheSizeGB =
            params["storage.wiredTiger.engineConfig.cacheSizeGB"].as<double>();
    }
    if (params.count("storage.syncPeriodSecs")) {
        wiredTigerGlobalOptions.checkpointDelaySecs =
            static_cast<size_t>(params["storage.syncPeriodSecs"].as<double>());
    }
    if (params.count("storage.wiredTiger.engineConfig.statisticsLogDelaySecs")) {
        wiredTigerGlobalOptions.statisticsLogDelaySecs =
            params["storage.wiredTiger.engineConfig.statisticsLogDelaySecs"].as<int>();
    }
    if (params.count("storage.wiredTiger.engineConfig.journalCompressor")) {
        wiredTigerGlobalOptions.journalCompressor =
            params["storage.wiredTiger.engineConfig.journalCompressor"].as<std::string>();
    }
    if (params.count("storage.wiredTiger.engineConfig.directoryForIndexes")) {
        wiredTigerGlobalOptions.directoryForIndexes =
            params["storage.wiredTiger.engineConfig.directoryForIndexes"].as<bool>();
    }
    if (params.count("storage.wiredTiger.engineConfig.configString")) {
        wiredTigerGlobalOptions.engineConfig =
            params["storage.wiredTiger.engineConfig.configString"].as<std::string>();
        log() << "Engine custom option: " << wiredTigerGlobalOptions.engineConfig;
    }

    // WiredTiger collection options
    if (params.count("storage.wiredTiger.collectionConfig.blockCompressor")) {
        wiredTigerGlobalOptions.collectionBlockCompressor =
            params["storage.wiredTiger.collectionConfig.blockCompressor"].as<std::string>();
    }
    if (params.count("storage.wiredTiger.collectionConfig.configString")) {
        wiredTigerGlobalOptions.collectionConfig =
            params["storage.wiredTiger.collectionConfig.configString"].as<std::string>();
        log() << "Collection custom option: " << wiredTigerGlobalOptions.collectionConfig;
    }

    // WiredTiger index options
    if (params.count("storage.wiredTiger.indexConfig.prefixCompression")) {
        wiredTigerGlobalOptions.useIndexPrefixCompression =
            params["storage.wiredTiger.indexConfig.prefixCompression"].as<bool>();
    }
    if (params.count("storage.wiredTiger.indexConfig.configString")) {
        wiredTigerGlobalOptions.indexConfig =
            params["storage.wiredTiger.indexConfig.configString"].as<std::string>();
        log() << "Index custom option: " << wiredTigerGlobalOptions.indexConfig;
    }

    return Status::OK();
}

}  // namespace mongo
