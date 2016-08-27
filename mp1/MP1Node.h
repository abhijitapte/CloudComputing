/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5
#define TGOSSIP 2

#define GOSSIP_FANOUT 4

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
    PUSHGOSSIP,
    PULLGOSSIP,
    DUMMYLASTMSGTYPE
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	char NULLADDR[6];
    
    /********************************************************************
     *                      Useful Typedefs                             *
     ********************************************************************/
    typedef std::vector<MemberListEntry> MemberList;

    /********************************************************************
     *                      Debugging methods                           *
     ********************************************************************/
    static void PrettyPrintBuffer(char* data, int size);
    static void PrettyPrintMemberList(const MemberList& memberList);
    
    /********************************************************************
     *                      Helper methods                              *
     ********************************************************************/
    size_t MemberListToStream(const MemberList& memberList,
                                   char* stream);

    size_t StreamToMemberList(const char* stream,
                                   MemberList& memberList);
    
    /********************************************************************
     *                  Join request and reply methods                  *
     ********************************************************************/
    
    // incoming
    bool ProcessJoinRequest(char* payload, int payloadSize);
    bool ProcessJoinReply(char* payload, int payloadSize);
    bool ProcessPushGossip(char* payload, int payloadSize);
    
    // outgoing
    void SendJoinRequest(/*const*/ Address* destAddress);
    void SendJoinReply(/*const*/ Address* destAddress);
    void SendPushGossip(/*const*/ Address* destAddress);

            /*
             * FIXME: Both the methods responsible for outward traffic
             * could have accepted a pointer to const Address however
             * trying to do so will ensue a change in signature of 
             * EmulNet's send which cannot be modified. :-(
             */
    
    /********************************************************************
     *                         Gossip methods                           *
     ********************************************************************/

    void Gossip();
    void GetRandomGossipTargets(MemberList& randomGossipTargets);
    
    /********************************************************************
     *                     Membership table methods                     *
     ********************************************************************/
    bool MergeWithMemberList(const MemberList&);
    void AddToMemberList(const MemberListEntry& entry);
    void PruneMemberList();
    void PropagateMembershipList(MsgTypes msgType, /*const*/ Address* destAddress);

    
    /********************************************************************
     * 		Two components of Gossip-style Membership               *
     *                 1) Failure Detection component                   *
     *                 2) Dissemination component                       *
     ********************************************************************/

    void DetectFailures();
            // Handles Failure detection responsibilities
            // of Group Membership protocol
    void Disseminate();
            // Handles Dissemination responsibilities of
            // of Group membership protocol
    
public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	static Address getJoinAddress();
    static Address getAddress(int id, short port);
	void initMemberListTable(Member *memberNode);
	static void printAddress(const Address *addr);
	virtual ~MP1Node();
};

#endif /* _MP1NODE_H_ */
