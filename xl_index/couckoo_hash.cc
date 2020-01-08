#include "couckoo_hash_imp.h"
#include <cstring>

uint64_t MurmurHash64A ( const void * key, int len, unsigned int seed );



#define Dassert(cond) do {if (!cond) {size_t _xxx_ = 0x555; *(char *)_xxx_ = 0xff;}} while (0)
namespace xl_index
{
  
  
  static size_t computeHash(const UserKey &s, size_t seed)
  {
    return MurmurHash64A(s.data(), s.size(), seed);
  }

  // must be common to both struct...
  static uint computeLocation(const UserKey &key, size_t seed,
			      uint16_t hashNum, uint hashSize)
  {
    
    seed = (seed ^ ((hashNum+1) * 0x1000ll));
    return (computeHash(key, seed)) % hashSize;    
  }

  
  // load from file
  RCouckooHashImp::RCouckooHashImp(const char *savedStr)
  {
    
    bcopy(savedStr, &m_hashBase, sizeof(m_hashBase));
    savedStr += sizeof(m_hashBase);    
    bcopy(savedStr, &m_size, sizeof(m_size));
    savedStr += sizeof(m_size);
    m_entries = new Entry[m_size];
    bcopy(savedStr, m_entries, sizeof(Entry)*m_size);
  }
  
  // build from RW 
  RCouckooHashImp::RCouckooHashImp(const WCouckooHashImp &from) :
    m_hashBase(from.m_hashBase),
    m_size(from.m_size)
  {
    m_entries = new Entry[m_size];
    for (uint i = 0; i < m_size; i++) {
      const auto &f_entry = from.m_entries[i];
      if (!f_entry.empty()) {
	
	m_entries[i]  = Entry(computeSignature(f_entry.key),
			      f_entry.location.isUpdate,
			      f_entry.location.blockNum);
      } 
    }
  }
  
  void RCouckooHashImp::save(char *savedStr) const
  {
    bcopy(&m_hashBase, savedStr, sizeof(m_hashBase));
    savedStr += sizeof(m_hashBase);
    bcopy(&m_size, savedStr, sizeof(m_size));
    savedStr += sizeof(m_size);
    bcopy(m_entries, savedStr, sizeof(Entry)*m_size);
  }
  
  
  void RCouckooHashImp::find(const UserKey &key, std::vector<ObjectLocationInfo> &possibleLocations) const
  {
    possibleLocations.clear();
    size_t signature = computeSignature(key);
    for (size_t i = 0; i < n_hashes; i++) {
      size_t location = computeLocation(key, m_hashBase, i,  m_size);
      auto const &entry = m_entries[location];
      if (entry.signature == signature) {
	possibleLocations.push_back(ObjectLocationInfo(entry.location, entry.update, false));
      } else if (entry.empty()) {
	return;
      }
      
    }
  }

  size_t RCouckooHashImp::computeSignature(const UserKey &key) const
  {
    Dassert(!key.empty());
    uint seed = 0;
    return computeHash(key, seed) % Entry::s_maxSignature + 1;
  }

  
  // WCouckooHashImp...
  WCouckooHashImp::WCouckooHashImp(size_t initSize) :
    m_nKeys(0),
    m_hashBase(0),
    m_size(initSize)
    
  {
    m_entries = new RwEntry[m_size];
  }
  
  WCouckooHashImp::~WCouckooHashImp() {
    if (m_entries)
      delete[] m_entries;
  }



  RCouckooHash *WCouckooHashImp::makeReadOnly(int) const
  {    
    return new RCouckooHashImp(*this);
  }

  bool WCouckooHashImp::insert(const UserKey &key, const ObjectLocationInfo &location)
  {
    // this function should not be called except from one thread!!!
    m_nKeys++;
    if (!tryToAdd(key, location)) {      
      // we could not add so we try another values for hash but before
      // we realse the lock so reads can go thrue
      reHash(key, location);
    } else {
    }
    return true;
  }
  

  bool WCouckooHashImp::tryToAdd(const UserKey &key, const ObjectLocationInfo & location)
  {    
    std::map<size_t, bool > checkedKey;
    std::vector< RwEntry *> path;
    RwEntry tmp(key, location);
    path.push_back(&tmp);
    return (recursiveAdd(path, checkedKey));
  }

  bool WCouckooHashImp::recursiveAdd(std::vector< RwEntry *> &path,
				     std::map<size_t , bool > &checkedKey)
  {
    auto entry = path.back();
    auto &key = entry->key;
    uint64_t hashes[n_hashes];
  
    for (int i = 0; i < n_hashes; i++) {
      auto hashVal = hashes[i] = computeLocation(key, m_hashBase, i,  m_size);
      auto &entry = m_entries[hashVal];
      if (entry.empty()) {
	
	path.push_back(&entry);
	applyPath(path);
	return true;
      } // not found try with one of the options
    }
  
    for (int i = 0; i < n_hashes; i++) {
      auto hashVal = hashes[i];
      if (checkedKey.find(hashVal) == checkedKey.end()) {
	checkedKey.insert(std::make_pair(hashVal, true));
	path.push_back(&m_entries[hashVal]);
	if (recursiveAdd(path, checkedKey ))
	  return true;
	path.pop_back();
      }
    }
    return false;
  }


  void WCouckooHashImp::reHash(const UserKey &key, const ObjectLocationInfo &location)
  {
    // we  do not need to take a lock for reading
    uint16_t hashBase = m_hashBase + 1;    
    uint16_t newSize = m_size;
    if (hashBase >= s_try) {
      hashBase = 0;
      newSize += s_initSize/8;
      // printf("must increase size from %u to %u (n_keys=%u)!!\n", m_size, newSize, m_nKeys);
    };
    
    uint safeCount=0;
    do {
      WCouckooHashImp second(newSize);
      second.m_hashBase = hashBase;
      bool failed = false;
      for (uint i = 0; i < m_size; i++) {
	auto const &entry = m_entries[i];
	if (!entry.empty()) {
	  if (!second.tryToAdd(entry.key, entry.location)) {
	    failed = true;
	    break; 
	  }
	}
      }
      if (!failed) {	
	if (second.tryToAdd(key, location)) {
	  // all is good take a lock to prevent reads
	  delete [] m_entries;
	  m_hashBase = second.m_hashBase;
	  m_size = second.m_size;
	  m_entries = second.m_entries;
	  second.m_entries = 0;
	  return;
	}
      }
      if (hashBase >= s_try) {
	hashBase = 0;
	newSize += s_initSize/8;
	//printf("must increase size from %u to %u (n_keys=%u)!!\n", m_size, newSize,m_nKeys);

      } else {
	hashBase++;
      }

    } while (safeCount++ < s_try * 8);
    Dassert(0);
  }

  void WCouckooHashImp::applyPath(const std::vector< RwEntry *> &path)
  {
    Dassert(path.back()->empty());
    for(int i = path.size()-1; i > 0; i--)
      *path[i] = *path[i-1];
  }

  WCouckooHash *WCouckooHash::construct(size_t initSize)
  {
    return new WCouckooHashImp(initSize*72/64);
  }
  RCouckooHash *RCouckooHash::load(const char *data)
  {
    return new RCouckooHashImp(data);
  }
  
}

#ifdef COUCKOO_HASH_UTEST

const size_t testMaxSize = 0xffff;
using namespace xl_index;
std::vector<std::pair<UserKey, uint16_t> > testVector(testMaxSize );
#include <pthread.h>
size_t s_falseNegatives;
size_t s_total;
size_t s_nKeys;
size_t s_saveSize;


WCouckooHash *insert()
{
  uint n_elements = 1024 + rand() % 1024;
  WCouckooHash *c = WCouckooHash::construct(n_elements);
  for(uint i = 0; i < n_elements; i++) {    
    uint elementNum = rand() % testMaxSize;
    while(testVector[elementNum].second != 0xffff) {
      elementNum = rand() % testMaxSize;
    }
    testVector[elementNum].second  = rand() % 256;
    c->insert(testVector[elementNum].first,
	      ObjectLocationInfo(testVector[elementNum].second, false, false));
  }
  s_nKeys += n_elements;
  return c;
}


void *lookup(void *data)
{
  RCouckooHash *c = (RCouckooHash *)data;
  uint n_elements = testMaxSize /2;
  size_t start = rand() % testMaxSize/2;
  uint falseNegatives = 0;
  for(uint i = 0; i < n_elements; i++) {
    std::vector<ObjectLocationInfo> possibleLocations;
    auto elementNum = start+i;
    c->find(testVector[elementNum].first,
	    possibleLocations) ;
    if (testVector[elementNum].second == 0xffff) {
      falseNegatives += possibleLocations.size();      
    } else {
      bool found = false;
      for (uint i = 0; i < possibleLocations.size(); i++) {
	if (testVector[elementNum].second == possibleLocations[i].blockNum) {
	  found = true;
	} else {
	  falseNegatives++;
	}
      }
      assert(found);
    }
  }
  //printf("false negatives %u, ratio %g\n", falseNegatives, falseNegatives*1.0/n_elements);
  s_falseNegatives += falseNegatives;
  s_total += n_elements;
  return NULL;
}
	  
	
	
	    
    


int main()
{
  
  for( int i = 0; i < testMaxSize; i++) {
    testVector[i].second = 0xffff;
    char data[sizeof(int) * 4];
    char *d = data;
    bcopy((const char *)&i, d, sizeof(int));
    d += sizeof(int);
    for (int j = 0; j < 3; j++) {
      int r = rand();
      bcopy((const char *)&r, d , sizeof(int));
      d+= sizeof(int);
    }
    testVector[i].first.resize(sizeof(int) * 4);
    for (uint j = 0; j < sizeof(int) * 4; j++)
      testVector[i].first[j] = data[j];
  }
  size_t startTime = time(0);
  
  for (int testNum = 0; testNum < 1024; testNum++) {    
    WCouckooHash *rwObject = insert();
    for( int i = 0; i < testMaxSize; i++) {
      testVector[i].second = 0xffff;
    }
    delete rwObject;
  }
  printf("1024 insert took %lu\n",time(0)-startTime);
  s_nKeys = 0;
  char *saveStr = new char[0x1000000];
  for (int testNum = 0; testNum < 1024; testNum++) {    
    WCouckooHash *rwObject = insert();
    RCouckooHash *roObject = rwObject->makeReadOnly(8);
    delete rwObject;
    lookup(roObject);
    
    roObject->save(saveStr);
    s_saveSize += roObject->saveSize();
    delete roObject;   
    roObject = RCouckooHash::load(saveStr);
    lookup(roObject);
    delete roObject;   
    for( int i = 0; i < testMaxSize; i++) {
      testVector[i].second = 0xffff;
    }    
  }
  printf("1024 full cycles took %lu, add %lu, lookup %lu, falseNegRatio %g, sizeRatio %g\n ",
	 time(0)-startTime,
	 s_nKeys,s_total, 
	 s_falseNegatives*1.0/s_total,
	 s_saveSize *1.0 / s_nKeys);
  
	 

}
#endif
	  
	
    
  
  

  
  
  

  

  
  
	
      
    
      
    
  
