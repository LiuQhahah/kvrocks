#include "redis_cuckoo_filter_chain.h"

#include "cuckoo_filter.h"

namespace redis {

rocksdb::Status CuckooFilterChain::MAdd(engine::Context &ctx, const Slice &user_key,
                                        const std::vector<std::string> &items,
                                        std::vector<CuckooFilterAddResult> *rets) {
  CuckooFilterInsertOptions insert_options;
  return InsertCommon(ctx, user_key, items, insert_options, rets);
}

rocksdb::Status CuckooFilterChain::getCuckooFilterChainMetadata(engine::Context &ctx, const Slice &ns_key,
                                                                CuckooFilterChainMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisCuckooFilter}, ns_key, metadata);
}

rocksdb::Status CuckooFilterChain::InsertCommon(engine::Context &ctx, const Slice &user_key,
                                                const std::vector<std::string> &items,
                                                const CuckooFilterInsertOptions &insert_options,
                                                std::vector<CuckooFilterAddResult> *results) {
  if (items.empty()) {
    return rocksdb::Status::OK();
  }

  std::string ns_key = AppendNamespacePrefix(user_key);
  ctx.txn_context_enabled = false;

  CuckooFilterChainMetadata metadata;
  rocksdb::Status s = getCuckooFilterChainMetadata(ctx, ns_key, &metadata);

  // if no metadata found, create a new cuckoo filter chain
  if (s.IsNotFound() && insert_options.auto_create) {
    s = createCuckooFilterChain(ctx, ns_key, insert_options.capacity, insert_options.bucket_size,
                                insert_options.expansion, insert_options.max_iterations, &metadata);
  }

  if (!s.ok()) {
    return s;
  }

  std::vector<std::string> cf_key_lists;
  getCFKeyList(ns_key, metadata, &cf_key_lists);

  std::vector < rocksdb::PinnableSlice cf_data_list;
  s = getCFDataList(ctx, bf_key_list, &bf_data_list);
  if (!s.ok()) {
    return s;
  }

  uint64_t origin_size = metadata.size;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter, {"insert"});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) {
    return s;
  }

  // for-loop items
  for (const auto &item : items) {
    if (item.empty()) {
      results->push_back(CuckooFilterAddResult::kOk);
      continue;
    }

    // Calculate the hash of the item
    uint64_t hash = CUCKOO_GEN_HASH(item.data(), item.size());
    LookupParams params;
    getLookupParams(hash, &params);
    
    bool inserted = false;
  }
}

uint32_t SubCF_GetIndex(const SubCF *subCF,CuckooHash hash){
  return (hash%subCF->num_buckets)*subCF->bucket_size;
}

uint8_t *Bucket_FindAvailable(CuckooBucket bucket,uint16_t bucket_size){
  // Find an available slot in the bucket
  for(uint16_t ii=0;ii<bucket_size;++ii){
    if(bucket[ii]==CUCKOO_NULLFP){
      return &bucket[ii];
    }
  }
  return NULL;
}

uint8_t *Filter_FindAvailable(SubCF *filter,const LookupParams *params){
  uint8_t *slot;
  uint8_t bucket_size = filter->bucket_size;
  uint64_t loc1 = SubCF_GetIndex(filter,params->h1);
  uint64_t loc2 = SubCF_GetIndex(filter,params->h2);
  if((slot=Bucket_FindAvailable(&filter->data[loc1],bucket_size))||(slot=Bucket_FindAvailable(&filter->data[loc2],bucket_size))){
    return slot;
  }
  return NULL;
}
CuckooFilterInsertStatus CuckooFilter_InsertFP(CuckooFilter *filter,LookupParams *params){
  for(uint16_t ii = filter->num_filters;ii>0;--ii){
    uint8_t *slot = Filter_FindAvailable(&filter->filters[ii-1],params);
    if(slot){
      *slot = params->fp;
      filter->num_items++;
      return CuckooInsert_Inserted;
    }
  }

  // No available slot found, need to kick out an item
  CuckooFilterInsertStatus status = Filter_KickOutInsert(filter, &filter->filters[filter->num_filters-1],params);

  if (status==CuckooInsert_Inserted){
    filter->num_items++;
    return CuckooInsert_Inserted;
  }

  if(filter->expansion==0){
    return CuckooInsert_NoSpace;
  }

  if(CuckooFilter_Grow(filter)!=0){
    return CuckooInsert_MemAllocFailed;
  }

  // Retry the insertion after growing the filter
  return CuckooFilter_InsertFP(filter, params);
}


int CuckooFilter_Grow(CuckooFilter *filter) {
    SubCF *filtersArray = reinterpret_cast<SubCF *>(
        realloc(filter->filters, sizeof(SubCF) * (filter->num_filters + 1))
    );
    if (!filtersArray) {
        return -1; 
    }

    filter->filters = filtersArray;
    SubCF *currentFilter = filtersArray + filter->num_filters;
    currentFilter->bucket_size = filter->bucket_size;
    currentFilter->my_cuckoo_bucket = nullptr;

    size_t growth = 1;
    for (uint16_t i = 0; i < filter->num_filters; ++i) {
        growth *= filter->expansion;
    }

    if (growth > CF_MAX_NUM_BUCKETS / filter->num_buckets) {
        return -1;
    }
    currentFilter->num_buckets = filter->num_buckets * growth;

    if (filter->bucket_size > SIZE_MAX / currentFilter->num_buckets) {
        return -1;
    }

    currentFilter->my_cuckoo_bucket = reinterpret_cast<MyCuckooBucket *>(
        calloc((size_t)currentFilter->num_buckets * filter->bucket_size, sizeof(MyCuckooBucket))
    );
    if (!currentFilter->my_cuckoo_bucket) {
        return -1;
    }

    filter->num_filters++;

    return 0;
}

CuckooInsertStatus Filter_KickOutInsert(CuckooFilter *filter,SubCF *cur_filter,const LookupParams *params){
  uint16_t max_iterations = filter->max_iterations;
  uint32_t num_buckets = cur_filter->num_buckets;
  uint16_t bucket_size = filter->bucket_size;
  CuckooFingerprint fp = params->fp;

  uint16_t counter = 0;
  uint32_t victim_index = 0;
  uint32_t ii = params->h1%num_buckets;

  while (counter++<max_iterations){
    uint8_t *bucket = &cur_filter->data[ii*bucket_size];
    swapFPs(bucket+victim_index,&fp);
    ii = getAltHash(fp,ii) % num_buckets;
    uint8_t *empty = Bucket_FindAvailable(&cur_filter->data[ii*bucket_size],bucket_size);
    if(empty){
      *empty = fp;
      return CuckooInsert_Inserted;
    }
    victim_index = (victim_index + 1) % bucket_size;
  }

  counter = 0;
  while(counter++<max_iterations){
    victim_index = (victim_index + bucket_size-1)%bucket_size;
    ii = getAltHash(fp,ii)%num_buckets;
    uint8_t *bucket = &cur_filter->data[ii*bucket_size];
    swapFPs(bucket+victim_index,&fp);

  }
  return CuckooInsert_NoSpace;
}

// Swap two fingerprint values in a bucket
void swapFPs(uint8_t *a, uint8_t *b) {
  uint8_t temp = *a;
  *a = *b;
  *b = temp;
}
void getLookupParams(CuckooHash hash, LookupParams *params) {
   params->fp = hash % 255 + 1; 
   params->h1 = hash;
   parmas->h2 = getAltHash(params->fp,params->h1);
}

CuckooHash getAltHash(CuckooFingerprint fp,CuckooHash index){
  return index^(fp*0x5bd1e995);
}

void CuckooFilterChain::getCFKeyList(const Slice &ns_key, const CuckooFilterChainMetadata &metadata,
                                     std::vector<std::string> *cf_key_list) {
  cf_key_list->reserve(metadata.n_filters);
  for (uint16_t i = 0; i < metadata.n_filters; ++i) {
    std::string cf_key = getCFKey(ns_key, metadata, i);
    cf_key_list->push_back(std::move(cf_key));
  }
}
rocksdb::Status CuckooFilterChain::Add(engine::Context &ctx, const Slice &user_key, const std::string &item,
                                       CuckooFilterAddResult *ret) {
  std::vector < CuckooFilterAddResult tmp{CuckooFilterAddResult::kOk};
  rocksdb::Status s = MAdd(ctx, user_key, {item}, &tmp);
  *ret = tmp[0];
  return s;
}
rocksdb::Status CuckooFilterChain::Reserve(engine::Context &ctx, const Slice &user_key, uint32_t capacity,
                                           uint32_t bucket_size, uint16_t expansion, uint16_t max_iterations) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  ctx.txn_context_enabled = false;
  CuckooFilterChainMetadata cuckoo_filter_chain_metadata;

  return createCuckooFilterChain(ctx, ns_key, capacity, bucket_size, expansion, max_iterations,
                                 &cuckoo_filter_chain_metadata);
}

rocksdb::Status CuckooFilterChain::createCuckooFilterChain(engine::Context &ctx, const Slice &ns_key, uint32_t capacity,
                                                           uint32_t bucket_size, uint16_t expansion,
                                                           uint16_t max_iterations,
                                                           CuckooFilterChainMetadata *metadata) {
  metadata->n_filters = 1;
  metadata->expansion = expansion;
  metadata->size = 0;
  metadata->bucket_size = bucket_size;
  metadata->capacity = capacity;
  metadata->max_iterations = max_iterations;

  auto [block_split_cuckoo_filter, _] = CreateBlockSplitCuckooFilter(metadata);

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCuckooFilter, {"createCuckooFilterChain"});
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  std::string cuckoo_filter_meta_bytes;
  metadata->Encode(&cuckoo_filter_meta_bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, cuckoo_filter_meta_bytes);
  if (!s.ok()) return s;

  std::string cf_key = getCFKey(ns_key, *metadata, metadata->n_filters - 1);
  s = batch->Put(cf_key, block_split_cuckoo_filter.GetData());
  if (!s.ok()) return s;

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

std::string CuckooFilterChain::getCFKey(const Slice &ns_key, const CuckooFilterChainMetadata &metadata,
                                        uint16_t filters_index) {
  std::string sub_key;
  PutFixed16(&sub_key, filters_index);
  std::string cf_key = InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return cf_key;
}
}  // namespace redis
