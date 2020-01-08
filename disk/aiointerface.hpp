namespace rocksxl
{
namespace aiointerface
{
  struct ReadData
  {
    ReadData(size_t readLba_,
	     void   *data,
	     size_t  size_,
	     void   *userCntxt_,
	     void   *(callbackfunc_)(ReadData *)) :
      readLba(readLba_), data(data_), size(size_),
      userCntxt(userCntxt_), status(0),
      callbackfunc(callbackfunc_) {};
	     
    size_t  readLba;
    void   *data;
    size_t  size;
    void   *userCntxt;
    size_t  status;
    void   *(callbackfunc)(ReadData *);
  };

  struct WriteData
  {
    WriteData(size_t writeLba_,
	     const void   *data,
	     size_t  size_,
	     void   *userCntxt_,
	     void   *(callbackfunc_)(WrireData *)) :
      writeLba(writeLba_), data(data_), size(size_),
      userCntxt(userCntxt_), status(0),
      callbackfunc(callbackfunc_) {};
	     
    size_t  writeLba;
    const void   *data;
    size_t  size;
    void   *userCntxt;
    size_t  status;
    void   *(callbackfunc)(WriteData *);
  };

  void read(ReadData *);
  void write(WriteData *);
}
}
  
