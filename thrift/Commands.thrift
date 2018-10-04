namespace java edu.dair.sgdb.thrift

struct KeyValue {
  1: binary key,
  2: binary value,
}

struct GigaScan {
    1: required list<KeyValue> kvs,
    2: required binary bitmap,
}

struct Movement {
  1: required i32 loc,
  2: required KeyValue kv,
}

exception RedirectException{
    1: required i32 status,
    2: optional i32 target,
    3: optional list<Movement> re,
    4: optional binary bitmap,
}

struct Dist{
  1: required i32 splitNum,
  2: required i32 vertexNum,
}

struct Status{
  1: binary key,
  2: i32 issplit,
  3: i32 location,
}
/*
 * Travel
 */
struct EpochEntity{
    1: required i32 serverId,
    2: required i32 epoch,
}

enum TravelCommandType {
    TRAVEL = 1,
    TRAVEL_MASTER = 2,
    TRAVEL_RTN = 3,
    TRAVEL_REG = 4,
    TRAVEL_FIN = 5,
    TRAVEL_NEXT = 6,
    TRAVEL_EXTEND = 7,
    TRAVEL_LAST = 8,

    SYNC_TRAVEL = 9,
    SYNC_TRAVEL_MASTER = 10,
    SYNC_TRAVEL_RTN = 11,
    SYNC_TRAVEL_START = 12,
    SYNC_TRAVEL_EXTEND = 13,
    SYNC_TRAVEL_FINISH = 14,

    TRAVEL_DEL = 15,
    TRAVEL_SYNC_DEL = 16,
}

struct TravelCommand {
    1: required TravelCommandType type,
    2: required i64 travelId,
    3: required i32 stepId,
    4: optional i32 reply_to,
    5: optional i32 get_from,
    6: optional string payload,
    7: optional i64 ts,
    8: optional list<i32> ext_srv,
    9: optional list<KeyValue> vals,
    10: optional list<binary> keys,
    11: optional i32 sub_type,
    12: optional i32 local_id,
    13: optional list<EpochEntity> epoch,
}

service TGraphFSServer {
    i32 insert(1:binary src, 2:binary dst, 3:i32 type, 4:binary val) throws (1: RedirectException r),

    i32 batch_insert(1:list<KeyValue> batches, 2:i32 type) throws (1: RedirectException r),

    list<Dist> get_state(), //list<Dist> stat_dst(1: Command c),

    list<KeyValue> read(1:binary src, 2:binary dst, 3:i32 type) throws (1: RedirectException r),

    list<KeyValue> scan(1:binary src, 2:i32 type) throws (1: RedirectException r),

    GigaScan giga_scan(1:binary src, 2:i32 type),

    list<KeyValue> force_scan(1:binary src, 2:i32 type) throws (1: RedirectException r),

    i32 split(1:binary src) throws (1: RedirectException r),

    i32 reassign(1:binary src, 2:i32 type, 3:i32 target),

    i32 fennel(1:binary src) throws (1: RedirectException r),

    i32 syncstatus(1: list<Status> statuses) throws (1: RedirectException r),

    i32 syncTravel(1:TravelCommand tc),

    i32 syncTravelMaster(1:TravelCommand tc),

    i32 syncTravelRtn(1:TravelCommand tc),

    i32 syncTravelStart(1:TravelCommand tc),

    i32 syncTravelExtend(1:TravelCommand tc),

    i32 syncTravelFinish(1:TravelCommand tc),

    i32 deleteSyncTravelInstance(1:TravelCommand tc),

}