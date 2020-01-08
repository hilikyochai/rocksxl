#include <stdlib.h>


#include "zobrist.h"

static uint64_t get64rand(uint seed) {
    return
    (((uint64_t) rand() <<  0) & 0x000000000000FFFFull) |
    (((uint64_t) rand() << 16) & 0x00000000FFFF0000ull) |
    (((uint64_t) rand() << 32) & 0x0000FFFF00000000ull) |
    (((uint64_t) rand() << 48) & 0xFFFF000000000000ull);
}

zobrist::zobrist(uint seed) {
  srand(seed);
  for ( int32_t i = 0 ; i < MAX_ZOBRIST_LENGTH ; i++ ) {
    for ( int32_t j = 0 ; j < ( 1 << CHAR_BIT) ; j++ ) {
      k->hashtab [i][j]  = get64rand();
    }
  }
}

uint64_t zobrist::hash (std::string &key) {
    uint64_t h = 0;
    size_t i = 0;
    size_t length = key.size();
  
    for ( ; i + 7  < length ; i += 8 ) {
      h ^= k->hashtab [ i ] [key[i]];
      h ^= k->hashtab [ i + 1 ] [key[i + 1]];
      h ^= k->hashtab [ i + 2 ] [key[i + 2]];
      h ^= k->hashtab [ i + 3 ] [key[i + 3]];
      h ^= k->hashtab [ i + 4 ] [key[i + 4]];
      h ^= k->hashtab [ i + 5 ] [key[i + 5]];
      h ^= k->hashtab [ i + 6 ] [key[i + 6]];
      h ^= k->hashtab [ i + 7 ] [key[i + 7]];
    }
    for (; i < length ; i++ )
      h ^= k->hashtab [ i ] [key[i]];
    return h;
}



