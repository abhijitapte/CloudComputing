/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

#include  <iomanip>


/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
//	int id = *(int*)(&memberNode->addr.addr);
//	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TGOSSIP;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
//	MessageHdr *msg;
//#ifdef DEBUGLOG
//    static char s[1024];
//#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        SendJoinRequest(joinaddr);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    return 1;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    
    bool retVal = false;
    
    if (data)
    {
        MessageHdr* msgHeader   = reinterpret_cast<MessageHdr*>(data);
        MsgTypes    msgType     = msgHeader->msgType;
        char*       payload     = data + sizeof(MessageHdr);
        size_t      payloadSize = size - sizeof(MessageHdr);
        
        // Dispatch the message to appropriate handler with a lookup
        // in message map
        
        switch (msgType)
        {
            case JOINREQ:
                retVal = ProcessJoinRequest(payload, payloadSize);
                break;
            case JOINREP:
                retVal = ProcessJoinReply(payload, payloadSize);
                break;
            case PUSHGOSSIP:
                retVal = ProcessPushGossip(payload, payloadSize);
                break;
            case DUMMYLASTMSGTYPE:
                break;
            default:
                assert("Cannot find message handler");
        }
    }
    
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
	/*
	 * Your code goes here
	 */

    DetectFailures();
    Disseminate();
}

void MP1Node::DetectFailures()
{
    memberNode->timeOutCounter++;
    PruneMemberList();
}

void MP1Node::Disseminate()
{
    if(memberNode->pingCounter > 0)
    {
        memberNode->pingCounter--;
        return;
    }
    
    // Reset ping counter back to TGOSSIP
    memberNode->pingCounter = TGOSSIP;

    // Increment number of heartbeats before gossiping
    memberNode->heartbeat++;
    Gossip();
}

void MP1Node::Gossip()
{
    MemberList randomGossipTargets;
    GetRandomGossipTargets(randomGossipTargets);
    
    // Send heartbeat messages to all nodes
    std::for_each(
                  randomGossipTargets.begin(),
                  randomGossipTargets.end(),
                  [this](const MemberListEntry& entry) {
                      Address gossipTargetAddress = getAddress(entry.id, entry.port);
                      this->SendPushGossip(&gossipTargetAddress);
                  });
}


/*
 *  REVISIT
 *
 *  Using std::random_shuffle(). A lookup in Google suggested Fischer-Yates shuffle
 *  is far more efficient. Need to try out later.
 */

#ifdef USE_FISCHER_YATES_SHUFFLE

// Fischer-Yates Shuffle
    template<class bidiiter>
    bidiiter random_unique(bidiiter begin, bidiiter end, size_t num_random) {
        size_t left = std::distance(begin, end);
        while (num_random--) {
            bidiiter r = begin;
            std::advance(r, rand()%left);
            std::swap(*begin, *r);
            ++begin;
            --left;
        }
        return begin;
    }

#endif


void MP1Node::GetRandomGossipTargets(MemberList& randomGossipTargets)
{
    auto& memberList = memberNode->memberList;
    
    // Copy over the entire membership list as gossip targets if
    // we do not have enough items.
    if (memberList.size() <= GOSSIP_FANOUT)
    {
        randomGossipTargets = memberList;
        return;
    }
    
    int id      = 0;
    int port    = 0;
    
    memcpy(&id, &memberNode->addr.addr[0], sizeof(int));
    memcpy(&port, &memberNode->addr.addr[4], sizeof(short));
    
    MemberList nonFaultyMemberList;
    std::copy_if(memberList.begin(), memberList.end(), std::back_inserter(nonFaultyMemberList), [](const MemberListEntry& entry) { return entry.heartbeat != FAILURE; });
    
    if (nonFaultyMemberList.size() <= GOSSIP_FANOUT)
        return;
    
    MemberList shuffledMemberList;
    
    while(1)
    {
        // Start with clean slate
        randomGossipTargets.clear();
        shuffledMemberList.clear();
        
        
        shuffledMemberList = nonFaultyMemberList;
	/*
	 * REVISIT:
         * Use some other efficient algorithm here whose complexity
         * is better than std::random_shuffle() 
	 */
        std::random_shuffle(shuffledMemberList.begin(), shuffledMemberList.end());
        
        randomGossipTargets = MemberList(shuffledMemberList.begin(), shuffledMemberList.begin() + GOSSIP_FANOUT);
        
        auto self = std::find_if(
                                 randomGossipTargets.begin(),
                                 randomGossipTargets.end(),
                                 [&](const MemberListEntry& entry) {
                                     return (entry.id == id && entry.port == port);
                                 });
        
        if (self == randomGossipTargets.end())
        {
            break;
        }
    }
}


/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

// Similar to above except that the {id, port}
// pairs are used rather than the hardcoded values

/*static*/
Address MP1Node::getAddress(int id, short port)
{
    Address joinaddr;
    
    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = id;
    *(short *)(&joinaddr.addr[4]) = port;
    
    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
/*static*/
void MP1Node::printAddress(const Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

/*static*/
void MP1Node::PrettyPrintBuffer(char* data, int size)
{
    std::cout << "Message size: " << size << ": <";
    for (int i = 0; i < size; i++)
        std::cout << std::hex << setfill('0') << setw(2) << static_cast<int>(*(data+i)) << ((i == size-1)? "": " ") << std::dec;
    std::cout << ">" << std::endl;
}

/*static*/
void MP1Node::PrettyPrintMemberList(const MemberList &memberList)
{

    if (!memberList.empty())
        std::cout << "Id\tPort\tHeartbeat\tTimestamp" << std::endl;
    else
        std::cout << "--- Empty ---" << std::endl;
    
    std::for_each(
                  memberList.begin(),
                  memberList.end(),
                  [](const MemberListEntry& memberListEntry) {
                      std::cout << memberListEntry.id << "\t"
                      << memberListEntry.port << "\t"
                      << memberListEntry.heartbeat << "\t\t"
                      << memberListEntry.timestamp << std::endl;
                  });
    
    std::cout << std::endl;
}


size_t MP1Node::MemberListToStream(
                const MemberList& memberList,
                char* stream)
{
    size_t numEntries = 0;
    std::for_each(
            memberList.begin(),
            memberList.end(),
                  [&](const MemberListEntry& entry) {
                      
                      if (entry.heartbeat != -1)
                      {
                          numEntries++;
                          
                          memcpy((char *)stream, &entry.id, sizeof(int));
                          stream += sizeof(int);
                          
                          memcpy((char *)stream, &entry.port, sizeof(short));
                          stream += sizeof(short);
                          
                          memcpy((char *)stream, &entry.heartbeat, sizeof(long));
                          stream += sizeof(long);
                          
                          memcpy((char *)stream, &entry.timestamp, sizeof(long));
                          stream += sizeof(long);

                      }
                  });
    
    return numEntries;
}

size_t MP1Node::StreamToMemberList(
                const char* payload,
                MemberList& gossipMemberList)
{
    size_t memberListSize;
    memcpy(&memberListSize, payload, sizeof(size_t));
    payload += sizeof(size_t);
    
    
    for(int i = 0; i < memberListSize; i++) {
        int id;
        short port;
        long heartbeat;
        long timestamp;
        
        memcpy(&id, payload, sizeof(int));
        payload += sizeof(int);
        
        memcpy(&port, payload, sizeof(short));
        payload += sizeof(short);
        
        memcpy(&heartbeat, payload, sizeof(long));
        payload += sizeof(long);
        
        memcpy(&timestamp, payload, sizeof(long));
        payload += sizeof(long);
        
        MemberListEntry memberListEntry(id, port, heartbeat, memberNode->timeOutCounter);
        gossipMemberList.push_back(memberListEntry);
    }
    
    return memberListSize;
}


bool MP1Node::ProcessJoinRequest(char* payload, int payloadSize)
{
    /*
     * 1) Obtain current timestamp.
     * 2) Search for an entry in membership table and if such an entry did
     *    exist, delete it from table.
     * 3) Add an entry in membership entry with the current timestamp.
     *
     * Heartbeat value is in the join request message itself.
     */
    
    Address*    joinerAddr  = reinterpret_cast<Address*>(payload);
    long*       heartbeatP  = reinterpret_cast<long*>(payload + sizeof(Address) + 1);
    
    // Extract id and port from the toAddr
    int     id          = 0;
    short   port        = 0;
    long    heartbeat   = *heartbeatP;
    long    timestamp   = memberNode->timeOutCounter;
    
    memcpy(&id, &joinerAddr->addr[0], sizeof(int));
    memcpy(&port, &joinerAddr->addr[4], sizeof(short));
    
    AddToMemberList(MemberListEntry(id, port, heartbeat, timestamp));

    SendJoinReply(joinerAddr);
    
    return true;
}

bool MP1Node::ProcessJoinReply(char* payload, int payloadSize)
{
    memberNode->inGroup = true;
    
    MemberList clusterMemberList;
    StreamToMemberList(payload, clusterMemberList);
    if (MergeWithMemberList(clusterMemberList))
        Gossip();
    
    return true;
}

bool MP1Node::ProcessPushGossip(char *payload, int payloadSize)
{
//    
//#ifdef DEBUGLOG
//    static char s[1024];
//    sprintf(s, "Received gossip");
//    log->LOG(&memberNode->addr, s);
//#endif
//
    /*
     * ProcessPushGossip() in functionality should be the same as
     * ProcessJoinReply() however, after receiving gossip, a
     * node has to gossip again.
     */
    
    MemberList clusterMemberList;
    StreamToMemberList(payload, clusterMemberList);
    if (MergeWithMemberList(clusterMemberList))
        Gossip();
    
    return true;
}

void MP1Node::SendJoinRequest(Address* destAddress)
{
#ifdef DEBUGLOG
    static char s[1024];
#endif
    
    size_t msgsize = sizeof(MessageHdr) + sizeof(destAddress->addr) + sizeof(long) + 1;
    MessageHdr* msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    
    // create JOINREQ message: format of data is {struct Address myaddr}
    msg->msgType = JOINREQ;
    memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
    
#ifdef DEBUGLOG
    sprintf(s, "Trying to join...");
    log->LOG(&memberNode->addr, s);
#endif
    
    // send JOINREQ message to introducer member
    emulNet->ENsend(&memberNode->addr, destAddress, (char *)msg, msgsize);
    
    free(msg);
}


void MP1Node::SendJoinReply(Address* destAddress)
{
//#ifdef DEBUGLOG
//    static char s[1024];
//#endif
    
    PropagateMembershipList(JOINREP, destAddress);
    
//#ifdef DEBUGLOG
//    sprintf(s, "Join Reply...");
//    log->LOG(&memberNode->addr, s);
//#endif
}

void MP1Node::SendPushGossip(Address* destAddress)
{
//#ifdef DEBUGLOG
//    static char s[1024];
//#endif
    
    int id      = 0;
    int port    = 0;
    
    memcpy(&id, &memberNode->addr.addr[0], sizeof(int));
    memcpy(&port, &memberNode->addr.addr[4], sizeof(short));

    
    auto& memberList = memberNode->memberList;
    auto self = std::find_if(
                        memberList.begin(),
                        memberList.end(),
                        [&](const MemberListEntry& entry) {
                            return (entry.id == id && entry.port == port);
                        });
    
    if (self != memberList.end())
    {
        
        // Update the entry with the new heartbeat
        self->heartbeat = memberNode->heartbeat;
    }
    else
    {
        AddToMemberList(MemberListEntry(id, port, memberNode->heartbeat, memberNode->timeOutCounter));
    }
    
    PropagateMembershipList(PUSHGOSSIP, destAddress);
//    
//#ifdef DEBUGLOG
//    
//    sprintf(s, "Propagate Gossip to %d.%d.%d.%d:%d",
//                    destAddress->addr[0],
//                    destAddress->addr[1],
//                    destAddress->addr[2],
//                    destAddress->addr[3],
//                    *(short *)&destAddress->addr[4]);
//    log->LOG(&memberNode->addr, s);
//#endif
//    
}

void MP1Node::PropagateMembershipList(MsgTypes msgType, Address* destAddress)
{
    
    /*
     * JOINREP messages should specify the cluster member list.
     */
    
    size_t memberListSize = std::count_if(
                                memberNode->memberList.begin(),
                                memberNode->memberList.end(),
                                          [&](const MemberListEntry& entry) {
                                              return (entry.heartbeat != -1);
                                          });
    
    size_t oneMemberListEntry = sizeof(MemberListEntry::id) +
                                    sizeof(MemberListEntry::port) +
                                    sizeof(MemberListEntry::heartbeat) +
                                    sizeof(MemberListEntry::timestamp);
    
    size_t msgsize = sizeof(MessageHdr) +
                        sizeof(memberListSize) +
                        (memberListSize * oneMemberListEntry);
    
    MessageHdr* msg = (MessageHdr*) malloc(msgsize * sizeof(char));
    
    // create JOINREP message: format of data is {memberListSize, memberList}
    msg->msgType = msgType;
        
    char* stream = (char*)(msg + 1) + sizeof(memberListSize);
    MemberListToStream(memberNode->memberList, stream);
    
    // update the size here
    memcpy((char *)(msg + 1), &memberListSize, sizeof(memberListSize));
    
    // send JOINREP message
    emulNet->ENsend(&memberNode->addr, destAddress, (char *)msg, static_cast<int>(msgsize));
    
    free(msg);
}

bool MP1Node::MergeWithMemberList(const MemberList& gossipMemberList)
{
    bool didMerge = false;
    
    auto& memberList = memberNode->memberList;
//#ifdef DEBUGLOG
//    int myMembershipList = memberList.size();
//    int entriesToMerge = gossipMemberList.size();
//    int mergedEntries = 0;
//#endif
    
    for (auto gossipIter = gossipMemberList.begin();
            gossipIter != gossipMemberList.end();
                ++gossipIter)
    {
        const MemberListEntry& gossipEntry = *gossipIter;
        auto iter = std::find_if(memberList.begin(), memberList.end(),
                                 [&](const MemberListEntry& entry) {
                                     return gossipEntry.id == entry.id &&
                                            gossipEntry.port == entry.port;
                                 });
        
        if (iter != memberList.end())
        {
            // Found matching entry in the membership list
            if (iter->heartbeat == FAILURE)
                    // Faulty node already
                continue;
            
            if (gossipEntry.timestamp <= iter->timestamp)
                    // Already have a recent version than the one
                    // received via gossip
                continue;
            
            if (gossipEntry.heartbeat == FAILURE ||
                gossipEntry.heartbeat > iter->heartbeat)
            {
//#ifdef DEBUGLOG
//                mergedEntries++;
//#endif
                didMerge = true;
                (*iter) = gossipEntry;
            }
        }
        else
        {
//#ifdef DEBUGLOG
//            mergedEntries++;
//#endif
            didMerge = true;
            memberList.push_back(gossipEntry);
            Address memberAddress = getAddress(gossipEntry.id, gossipEntry.port);
#ifdef DEBUGLOG
            log->logNodeAdd(&memberNode->addr, &memberAddress);
#endif
        }
    }
    
//    
//#ifdef DEBUGLOG
//    static char s[1024];
//    sprintf(s, "Merge: member list entries = %d, gossip entries = %d, merged entries = %d",
//            myMembershipList,
//            entriesToMerge,
//            mergedEntries);
//    log->LOG(&memberNode->addr, s);
//#endif
//    
    
    return didMerge;
}

void MP1Node::AddToMemberList(const MemberListEntry& entry)
{
    // Remove if this entry already exists in the membership table
    auto& memberList = memberNode->memberList;
    memberList.erase(
            std::remove_if(
                    memberList.begin(),
                    memberList.end(),
                    [&](const MemberListEntry& member) {
                        return member.id == entry.id &&
                                member.port == entry.port;
                    }),
            memberNode->memberList.end());
    
    // Proceed by adding entry into the membership table now!
    memberNode->memberList.push_back(entry);
    Address memberAddress = getAddress(entry.id, entry.port);
#ifdef DEBUGLOG
    log->logNodeAdd(&memberNode->addr, &memberAddress);
#endif

}

void MP1Node::PruneMemberList()
{
    auto& memberList = memberNode->memberList;
    int id = 0;
    short port = 0;
    int timeOutCounter = memberNode->timeOutCounter;
    
    memcpy(&id, &memberNode->addr.addr[0], sizeof(int));
    memcpy(&port, &memberNode->addr.addr[4], sizeof(short));
    
    /*
        1) Identify and mark nodes that have failed. Gossip didn't arrive in time.
	2) Prune the member list by deleting nodes for which we waited enough.
     */
    for(auto iter = memberList.begin(); iter != memberList.end(); ++iter)
    {
        MemberListEntry& entry = *iter;
        
        /*
            Entry being iterated is not the one pertaining to this node itself
                (i.e. exclude this node)
         */
        
        /*
         REVISIT: Scope for using vector<MemberListEntry>::iterator
         in conjunction with mypos member that is not being used now
         */
        if (entry.id != id || entry.port != port)
        {
            /*
                Entry has a timeOutCounter that is old enough to be considered for
                marking the node as failed.
             */
            if (timeOutCounter - entry.timestamp > TFAIL)
            {
                entry.heartbeat = FAILURE;
            }
        }
    }
    
    /*
        Remove all the nodes that didn't respond even after waiting until TREMOVE
     */
    memberList.erase(
        std::remove_if(memberList.begin(), memberList.end(),
                       [&](const MemberListEntry& entry) {
                           bool canRemove = (entry.id != id || entry.port != port) &&
                                            (entry.heartbeat == FAILURE) &&
                                            (timeOutCounter - entry.timestamp > TREMOVE);
                           if (canRemove)
                           {
                               Address memberAddress = getAddress(entry.id, entry.port);
#ifdef DEBUGLOG
                               log->logNodeRemove(&memberNode->addr, &memberAddress);
#endif
                           }
                           
                           return canRemove;
                           
                       }
                     ),
        memberList.end());
}

