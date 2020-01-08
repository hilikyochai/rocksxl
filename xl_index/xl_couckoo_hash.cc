namespace xl_index
{
  #include "xl_cuckoo_hash.h"
  
  
  static size_t compute_hash(string const& s, size_t seed)
  {
    static const size_t p_pow[256];
    if (p_pow[0] == 0) {
      p_pow[0] = 1;
      const int p = 31;
      for (int i = 1; i < 255; i++) {
	p_pow[i] = (p_pow[i-1] *p) % m;
      }
    }
	
      
    size_t hash_value = seed;
    uint8_t i = 0;
    for (char c : s) {
        hash_value = (hash_value + (c + 1) * p_pow[i]);
        i++;
    }
    return hash_value;
  }

  // load from file
  RoCuckooHash::RoCuckooHash(const char *savedStr)
  {
    bcopy(savedStr, &data, s_sizeofData);
  }
			     
  void RoCuckooHash::find(string const &key, std::vector<uint8_t> &possibleLocations)
  {
    possibleLocations.clear();
    uint8_t signature = compute_hash(key, data.m_hashBase);  
    for (size_t i = 0; i < n_hashes; i++) {
      size_t h = compute_hash(key, data.m_hashBase * (i+1) * 0x123) % nEntries;
      if (data.m_entries[h].second == signature) {
	possibleLocations.push_back(data.m_entries[h].first);
      }      
    }
  }


  void RwCuckooHash::find(string const &key, std::vector<uint8_t> &possibleLocations)
  {
    possibleLocations.clear();
    for (size_t i = 0; i < n_hashes; i++) {
      size_t h = compute_hash(key, data.m_hashBase * (i+1) * 0x123) % nEntries;
      if (data.m_entries[h].second == key) {
	possibleLocations.push_back(data.m_entries[h].first);
	return;
      }      
    }
  }

  bool RwCuckooHash::insert(string const &key, uint8_t blockNum)
  {
    m_nkeys++;
    for (int i = 0; i < 8; i++) {
      if (tryToAdd(key, blockNum)) {
	return true;
      } else {
	if (m_nkeys > m_maxKeys)
	  return false;
	if (!reHash())
	  return false;
      }
    }
    return false;
  }

  bool RwCuckooHash::tryToAdd(string const &key, uint8_t blockNum)
  {
    
    std::unordered_map< string const &, bool > checkedKey;
    std::vector< RwEntry *> path;
    RwEntry tmp(key, blockNum);
    path.push_back(&tmp);
    return (RecursiveAdd(path, checkedKey));
  }

  bool XHashTable::RecursiveAdd(std::vector< RwEntry *> &path,
				std::unordered_map<string const &, bool > &checkedKey)
  {
    auto entry = path.back();
    auto &key = entry->first;
    uint64_t hashes[n_hashes];
  
    for (int i = 0; i < n_hashes; i++) {
      auto hashVal = hashes[i] = hash(key.c_str(), key.size(), seed, i) % kNelements;
      if (hashEntries[hashVal].empty()) {
	path.push_back(&hashEntries[hashVal]);
	ApplyPath(path);
	return true;
      } // not found try with one of the options
    }
  
    for (int i = 0; i < n_hashes; i++) {
      auto hashVal = hashes[i];
      if (checkedKey.find(hashVal) == checkedKey.end()) {
	checkedKey.insert(std::make_pair(hashVal, true));
	path.push_back(&hashEntries[hashVal]);
	if (RecursiveAdd(path, checkedKey, get))
	  return true;
	path.pop_back();
      }
    }
  
    return false;
}
  
	  
	
    
  
  

  
  
  

  

  
  
	
      
    
      
    
  
