#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
		status = db.createFile(fileName); //create new file
        if (status != OK) return (status);
        
        db.openFile(fileName, file); //open newly created file
		if (status != OK) return (status);

        //allocate an empty page in buffer pool
        status = bufMgr->allocPage(file, hdrPageNo, newPage); 
		if (status != OK) return (status);
        //Take the Page* pointer(newPage) returned from allocPage() and cast it to a FileHdrPage*
        hdrPage = (FileHdrPage *) newPage;

        // Then make a second call to bm->allocPage(). This page will be the first data page of the file. 
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK) return (status);

        // Using the Page* pointer returned, invoke its init() method to initialize the page contents.
        newPage->init(newPageNo);

        // Finally, store the page number of the data page in firstPage and lastPage attributes of the FileHdrPage.
        if (hdrPage != nullptr){
            strcpy(hdrPage->fileName, fileName.c_str());
            hdrPage->firstPage = newPageNo;
            hdrPage->lastPage = newPageNo;
            hdrPage->pageCnt = 1;
            hdrPage->recCnt = 0;
            
        }
        // When you have done all this unpin both pages and mark them as dirty.
        bufMgr->unPinPage(file, newPageNo, true); 
        bufMgr->unPinPage(file, hdrPageNo, true); 
		
		// flush and close file
        bufMgr->flushFile(file);
        db.closeFile(file);
        return OK; // FIXME: what should I return?
    }

    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

/**
 * This constructor opens the underlying file
 * @param fileName The name of the file to be opened
 * @param returnStatus A reference to a Status variable
*/
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
		// Next, it reads and pins the header page for the file in the buffer pool, 
        // initializing the private data members headerPage, headerPageNo, and hdrDirtyFlag
        int firstPageNo;
        Page* firstPage;
        //returns via firstPageNo
        status = filePtr->getFirstPage(firstPageNo);
        if (status != OK){
            returnStatus = status;
            return;
        }
        //returns page via firstPage
		status = bufMgr->readPage(filePtr,firstPageNo,firstPage);
        if (status != OK){
            returnStatus = status;
            return;
        }
		headerPage = (FileHdrPage *) firstPage;
		headerPageNo = firstPageNo;
        //TODO: is it false? 
		hdrDirtyFlag = false;
		
		// Finally, read and pin the first page of the file into the buffer pool, 
        // initializing the values of curPage, curPageNo, and curDirtyFlag appropriately. 
        // Set curRec to NULLRID.
        curPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr,curPageNo,curPage);
        if (status != OK){
            returnStatus = status;
            return;
        }
        curDirtyFlag = false;
        curRec = NULLRID;
        returnStatus = status;
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;
    // check if curPage is NULL. 
    // If yes, read the right page (the one with the requested record on it) into the buffer
    if (curPage == NULL){
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK){
            return status;
        }
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
        curRec = rid;
        status = curPage->getRecord(rid, rec);
        return status;
    }
    // If the desired record is on the currently pinned page
    if (curPageNo == rid.pageNo){
        // Call getRecord on current page (gets record by slot number)
        status = curPage->getRecord(rid, rec);
        if (status != OK){
            return status;
        }
        // Update HeapFile object
        curRec = rid;
        return status;
    }
    else{
        // unpin the currently pinned page (assuming a page is pinned)
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK){
            return status;
        }
        // Update HeapFile object
        curPage = NULL;
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
        curRec = rid;
        // use the pageNo field of the RID to read the page into the buffer pool
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK){
            return status;
        }
        status = curPage->getRecord(rid, rec);
        if (status != OK){
            return status;
        }
    }
    return status;
    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
   
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}
//****************************************************************//
//************************ working here **************************//
//****************************************************************//

/*
Scans through files looking for the next record to satisfy the predicate
Output: RID of next record to satisfy the predicate :)
Method: Scan one file page at a time. 
 - For each page, use the firstRecord() and nextRecord() methods of the Page class to get the 
    rids of all the records on the page.
 - Convert the rid to a pointer to the record data and invoke matchRec() to determine 
   if record satisfies the filter associated with the scan. 

*/
const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;
    bool firstLoop = true;

    if (curPageNo < 0) return FILEEOF; 
    if (curPage == NULL){
        // build page info
        curPageNo = headerPage->firstPage;
        if (curPageNo < 0) return FILEEOF;
        status = bufMgr->readPage(filePtr, curPageNo, curPage );
        if (status != OK) return(status);
        curDirtyFlag = false;

        // build Rec info
        status = curPage->firstRecord(tmpRid);
        if (status != OK) { // there are no records
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return(status);
            curPage = NULL;
            curPageNo = -1;
            curRec = tmpRid; // not sure about this line
            return FILEEOF;
        }
        curRec = tmpRid;
        // have to then check this record.
        getRecord(rec);
        if(matchRec(rec)){
            outRid = tmpRid;
            return OK;
        }
    }

    // loop through pages. 
    while (status == OK){
        // get the most recently used record
        if (firstLoop){
            tmpRid = curRec;
            firstLoop = false;
        }
        else {
            status = curPage->firstRecord(tmpRid);
            if (status != OK) return(status);
            curRec = tmpRid;
            // check this record.
            getRecord(rec);
            if(matchRec(rec)){
                outRid = tmpRid;
                return OK;
            }
        }
        // loops through records on a page.
        while ((curPage->nextRecord(tmpRid, nextRid)) == OK){
            curRec = nextRid;
            getRecord(rec);
            if(matchRec(rec)){
                outRid = nextRid;
                return OK;
            }
            tmpRid = nextRid;
        }
        // outside of this loop means we are at the end of the page

        // get next page number
        status = curPage->getNextPage(nextPageNo);
        if (status != OK) return(status);
        // unpin current page.
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) return(status);
        if (nextPageNo == -1){ // end of file
            return (NORECORDS); //FIXME
        }
        else{
            curPageNo = nextPageNo;
        }
        // go to next page. update status based on next page call
        status = bufMgr->readPage(filePtr, curPageNo, curPage );
    }
    // broke out of this loop bc status is not OK
	return status;
}

// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}

// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    //Invalid current Page
    // check if curPage is null
    if (curPage == NULL){
        //Read the last page (Use hdrPage->lastPage)
        curPageNo = headerPage->lastPage;
        //Set last page as current page
        status = bufMgr->readPage(filePtr,curPageNo,curPage);
        if (status != OK){
            //cout << "11111: "<<curPageNo<< " "<< status;
            return status;
        }
    }
    //cout << "--------HERE--------";
    //Valid current Page
    if (curPage != NULL){
        status = curPage->insertRecord(rec,rid);
        if (status == OK){
            headerPage->recCnt++;
            outRid = rid;
            hdrDirtyFlag = true;
            curDirtyFlag = true;
            //cout << "successfully inserted";
            return status;
        }
        else{
            //Allocate new page and initialize it
            status = bufMgr->allocPage(filePtr, newPageNo, newPage);
            if (status != OK){
                //cout << "allocation failed";
                return status;
            }
            newPage->init(newPageNo);
            status = curPage->setNextPage(newPageNo);
            if (status != OK){
                return status;
            }
            //Unpin current page and update to new page
            curDirtyFlag = true;
            unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            curPage = newPage;
            curPageNo = newPageNo;
            curDirtyFlag = false;
            //Call insertRecord on this new Page
            status = curPage->insertRecord(rec, rid);
            if (status == OK) {
                outRid = rid;
                curDirtyFlag = true;
                headerPage->recCnt++;
                //cout << "New page allocated and record inserted";
                return OK;
            } else {
                //when error, unpin and reset
                //cout << "insertion failed";
                //TODO anything else to update??
                bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
                curPage = NULL;
                curPageNo = -1;
                return status;
            }
        }
    }
    return status;
}
