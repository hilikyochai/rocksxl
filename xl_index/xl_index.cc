#include "xl_index_impl.h"
#include <strings.h>
#define Dassert(cond) do {if (!(cond)) {size_t _xxx_ = 0x555; *(char *)_xxx_ = 0xff;}} while (0)
namespace xl_index
{
 
  IndexImp::IndexImp(const std::vector<std::pair<const UserKey *, ObjectLocationInfo> > &entries)    
  {
    Dassert(!entries.empty() && entries.front().second.blockNum == 0);
    size_t lastBlock = entries.back().second.blockNum;
    if (lastBlock % s_megaBlockSizeBlocks) 
      lastBlock += s_megaBlockSizeBlocks - (lastBlock % s_megaBlockSizeBlocks);
    m_index.resize(lastBlock/s_megaBlockSizeBlocks);
    uint curLocation = 0;
    for (auto &index : m_index) {
      index.build(entries, curLocation);
    } 
  }
    
  IndexImp::IndexImp(const char *from)    
  {
    uint size = *(uint *)from;
    from += sizeof(uint);
    m_index.resize(size);
    for (auto &index : m_index) {
      index.load(from);
      from += index.saveSize();
    }
  }

  void IndexImp::get_posible_locations(const UserKey &key,
				       std::vector<ObjectLocationInfo> &ret) const
  {
    int location = search(key);
    if (location >= 0) {
      m_index[location].get_posible_locations(key, ret);
      for (auto &b : ret)
	b.blockNum += s_megaBlockSizeBlocks * location;
    }
  }
  
  void IndexImp::save(char *data) const
  {
    *(uint *)data = m_index.size();
    data += sizeof(uint);
    for (auto const &index : m_index) {
      index.save(data);
      data += index.saveSize();
    }
  }
  
  int IndexImp::saveSize() const {
    int ret = sizeof(uint);    
    for (auto const &index : m_index) {
      ret += index.saveSize();
    }
    return ret;
  }

  int IndexImp::search(const UserKey &key) const
  {
    size_t lower = 0;
    if(key <= m_index[0].lastKey) {
      return 0;
    }
    
    size_t upper = m_index.size()-1;
    while (lower <= upper) {
      auto mid = lower + (upper-lower) /2;
      auto const &entry = m_index[mid];
      if(key <= entry.lastKey) {
	if (key > m_index[mid-1].lastKey) {
	  return mid;
	} else {
	  upper  = mid-1;
	}
      } else {
	lower = mid+1;
      }
    }
    Dassert(key > m_index.back().lastKey);
    return -1;
  }
  
  // index entry functions

  void IndexEntry::build(const std::vector<std::pair<const UserKey *,
			 ObjectLocationInfo> >  &entries,
			 uint &startLocation)
  {
    auto const &entry = entries[startLocation];
    uint startMegaBlockOffest = entry.second.blockNum / s_megaBlockSizeBlocks *
      s_megaBlockSizeBlocks;
    bool last = false;
    uint lastLocation= startLocation;
    while (!last) {
      if (lastLocation == entries.size()-1)  {
	last = true;
      } else {
	auto const &nextEntry = entries[lastLocation+1];
	if (nextEntry.second.blockNum - startMegaBlockOffest >=
	    s_megaBlockSizeBlocks) {
	  last = true;
	}
      }
      if (!last) 
	lastLocation++;
    };
    lastKey = *entries[lastLocation].first;
    //update = entries[i].second.is_update();
    WCouckooHash *tmp = WCouckooHash::construct(lastLocation-startLocation+1);    
    for (; startLocation <= lastLocation; startLocation++) {
      Dassert(startLocation == 0 || entries[startLocation].first > entries[startLocation-1].first);
      tmp->insert(*entries[startLocation].first,
		  entries[startLocation].second);
    }
    m_hash = tmp->makeReadOnly(0);
    delete tmp;
  }

  void IndexEntry::load(const char *from)
  {
    const uint32_t keySize = *(uint32_t *) from;
    from += sizeof(uint32_t);
    lastKey = UserKey(from, keySize);
    from += keySize;
    m_hash = RCouckooHash::load(from);
  }

  void IndexEntry::save(char *to) const
  {
    const uint32_t keySize = lastKey.size();
    *(uint32_t *) to = keySize;
    to += sizeof(uint32_t);
    bcopy(lastKey.data(), to, keySize);
    to += keySize;
    m_hash->save(to);
  }

  IndexInterface *IndexInterface::construct(const char *from) {
    return new IndexImp(from);
  }
  IndexInterface *IndexInterface::build(const std::vector<std::pair<const UserKey*,
					ObjectLocationInfo> > &entries ) {
    return new IndexImp(entries);
  }
}

#ifdef XL_INDEX_UTEST

const size_t testMaxSize = 0x100000;
using namespace xl_index;

std::vector<std::pair<UserKey, size_t> > testVector(testMaxSize );
#include <pthread.h>
size_t s_falseNegatives;
size_t s_total;
size_t s_nKeys;
size_t s_saveSize;


IndexInterface *fillup()
{
  size_t curFileLocation = 0;
  uint propability = 64;
  std::vector<std::pair<const UserKey *, ObjectLocationInfo> >  indexInput(testMaxSize);
  uint curSize = 0;
  for (uint i = 0; i < testMaxSize; i++) {
    if (rand() % 128 <= propability) {
      testVector[i].second = curFileLocation / s_readBlockSize;
      curFileLocation += s_readBlockSize/8 + rand() % (s_readBlockSize *4);
      indexInput[curSize++]= std::make_pair(&testVector[i].first,
					    ObjectLocationInfo(testVector[i].second,
							       false,
							       curFileLocation / s_readBlockSize != testVector[i].second));
    }
  }
  indexInput.resize(curSize);
  s_nKeys += curSize;    
  return IndexInterface::build(indexInput);
}



void *lookup(void *data)
{
  IndexInterface *c = (IndexInterface *)data;
  uint n_elements = testMaxSize;
  size_t start = 0;
  uint falseNegatives = 0;
  for(uint i = 0; i < n_elements; i++) {
    std::vector<ObjectLocationInfo> posibleLocations;
    auto elementNum = start+i;
    c->get_posible_locations(testVector[elementNum].first,
	    posibleLocations) ;
    if (testVector[elementNum].second == -1ull) {
      falseNegatives += posibleLocations.size();      
    } else {
      bool found = false;
      for (uint i = 0; i < posibleLocations.size(); i++) {
	if (testVector[elementNum].second == posibleLocations[i].blockNum) {
	  found = true;
	} else {
	  falseNegatives++;
	}
      }
      Dassert(found);
    }
  }
  //printf("false negatives %u, ratio %g\n", falseNegatives, falseNegatives*1.0/n_elements);
  s_falseNegatives += falseNegatives;
  s_total += n_elements;
  return NULL;
}
int main()
{
  static const int n_tests = 8;
  
  for( int i = 0; i < testMaxSize; i++) {
    testVector[i].second = -1ull;
    char data[256];
    sprintf(data, "%8.8d%8.8d%8.8d%8.8d", i, rand(),rand(),rand());
    
    testVector[i].first = std::string(data, 32);
  }
  size_t startTime = time(0);
  
  for (int testNum = 0; testNum < n_tests ; testNum++) {    
    IndexInterface *rwObject = fillup();
    for( int i = 0; i < testMaxSize; i++) {
      testVector[i].second = -1ull;
    }
    delete rwObject;
  }
  printf("%d  insert tesys took %lu\n",n_tests, time(0)-startTime);
  s_nKeys = 0;
  for (int testNum = 0; testNum < n_tests; testNum++) {    
    IndexInterface *rwObject = fillup();
    lookup(rwObject);
    char *saveStr = new char[rwObject->saveSize()];    
    rwObject->save(saveStr);
    s_saveSize += rwObject->saveSize();
    delete rwObject;   
    rwObject = IndexInterface::construct(saveStr);
    lookup(rwObject);
    delete rwObject;   
    for( int i = 0; i < testMaxSize; i++) {
      testVector[i].second = -1ull;
    }
    delete [] (saveStr);
  }
  printf("%d full cycles took %lu, add %lu, lookup %lu, falseNegRatio %g, sizeRatio %g\n ",
	 n_tests,
	 time(0)-startTime,
	 s_nKeys,s_total, 
	 s_falseNegatives*1.0/s_total,
	 s_saveSize *1.0 / s_nKeys);
  
	 

}


#endif
    
    


