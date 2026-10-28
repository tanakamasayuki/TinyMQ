#include "mq_props.h"

namespace tinymq {

namespace {

enum Type : uint8_t { kByte, kU16, kU32, kVarint, kString, kBinary, kPair, kUnknown };

struct Spec {
    uint8_t id;
    uint8_t type;
    uint16_t scope;
};

const uint16_t kMsg = kInPublish | kInWill;
const uint16_t kReplies = kInConnack | kInAck | kInSuback | kInUnsuback | kInDisconnect | kInAuth;

const Spec kSpecs[] = {
    {kPayloadFormatIndicator, kByte, kMsg},
    {kMessageExpiryInterval, kU32, kMsg},
    {kContentType, kString, kMsg},
    {kResponseTopic, kString, kMsg},
    {kCorrelationData, kBinary, kMsg},
    {kSubscriptionIdentifier, kVarint, kInPublish | kInSubscribe},
    {kSessionExpiryInterval, kU32, kInConnect | kInConnack | kInDisconnect},
    {kAssignedClientIdentifier, kString, kInConnack},
    {kServerKeepAlive, kU16, kInConnack},
    {kAuthenticationMethod, kString, kInConnect | kInConnack | kInAuth},
    {kAuthenticationData, kBinary, kInConnect | kInConnack | kInAuth},
    {kRequestProblemInformation, kByte, kInConnect},
    {kWillDelayInterval, kU32, kInWill},
    {kRequestResponseInformation, kByte, kInConnect},
    {kResponseInformation, kString, kInConnack},
    {kServerReference, kString, kInConnack | kInDisconnect},
    {kReasonString, kString, kReplies},
    {kReceiveMaximum, kU16, kInConnect | kInConnack},
    {kTopicAliasMaximum, kU16, kInConnect | kInConnack},
    {kTopicAlias, kU16, kInPublish},
    {kMaximumQos, kByte, kInConnack},
    {kRetainAvailable, kByte, kInConnack},
    {kUserProperty, kPair, 0xFFFF},
    {kMaximumPacketSize, kU32, kInConnect | kInConnack},
    {kWildcardSubscriptionAvailable, kByte, kInConnack},
    {kSubscriptionIdentifierAvailable, kByte, kInConnack},
    {kSharedSubscriptionAvailable, kByte, kInConnack},
};

const Spec *spec(uint8_t id)
{
    for (const Spec &s : kSpecs)
        if (s.id == id)
            return &s;
    return nullptr;
}

}  // namespace

bool PropReader::next(Property &p)
{
    if (!r_.ok() || r_.remaining() == 0)
        return false;
    uint32_t id = r_.varint();
    const Spec *s = id < 0x80 ? spec((uint8_t)id) : nullptr;
    if (!s) {
        r_.fail();
        return false;
    }
    p.id = (uint8_t)id;
    p.value = 0;
    p.data = p.data2 = Bytes();
    switch (s->type) {
    case kByte:
        p.value = r_.u8();
        break;
    case kU16:
        p.value = r_.u16();
        break;
    case kU32:
        p.value = r_.u32();
        break;
    case kVarint:
        p.value = r_.varint();
        break;
    case kString:
        p.data = r_.str();
        break;
    case kBinary:
        p.data = r_.bin();
        break;
    case kPair:
        p.data = r_.str();
        p.data2 = r_.str();
        break;
    }
    return r_.ok();
}

uint8_t checkProps(Bytes props, uint16_t scope)
{
    PropReader r(props);
    Property p;
    uint32_t seen[2] = {0, 0};  // ids 0x00-0x3F
    while (r.next(p)) {
        const Spec *s = spec(p.id);
        if (!(s->scope & scope))
            return kProtocolError;
        bool repeatable = p.id == kUserProperty || (p.id == kSubscriptionIdentifier && scope == kInPublish);
        uint32_t bit = 1u << (p.id & 31);
        uint32_t &word = seen[p.id >> 5];
        if ((word & bit) && !repeatable)
            return kProtocolError;
        word |= bit;
        switch (p.id) {
        case kPayloadFormatIndicator:
        case kRequestProblemInformation:
        case kRequestResponseInformation:
        case kMaximumQos:
        case kRetainAvailable:
        case kWildcardSubscriptionAvailable:
        case kSubscriptionIdentifierAvailable:
        case kSharedSubscriptionAvailable:
            if (p.value > 1)
                return kProtocolError;
            break;
        case kReceiveMaximum:
        case kMaximumPacketSize:
        case kTopicAlias:
        case kSubscriptionIdentifier:
            if (p.value == 0)
                return kProtocolError;
            break;
        default:
            break;
        }
    }
    return r.ok() ? kSuccess : kMalformedPacket;
}

bool findProp(Bytes props, uint8_t id, Property &out)
{
    PropReader r(props);
    while (r.next(out))
        if (out.id == id)
            return true;
    return false;
}

void PropWriter::byte(uint8_t id, uint8_t v)
{
    w_.u8(id);
    w_.u8(v);
}

void PropWriter::u16(uint8_t id, uint16_t v)
{
    w_.u8(id);
    w_.u16(v);
}

void PropWriter::u32(uint8_t id, uint32_t v)
{
    w_.u8(id);
    w_.u32(v);
}

void PropWriter::varint(uint8_t id, uint32_t v)
{
    w_.u8(id);
    w_.varint(v);
}

void PropWriter::string(uint8_t id, Bytes s)
{
    w_.u8(id);
    w_.str(s);
}

void PropWriter::pair(uint8_t id, Bytes name, Bytes value)
{
    w_.u8(id);
    w_.str(name);
    w_.str(value);
}

void PropWriter::copy(Bytes props, const uint8_t *skip, size_t skipCount)
{
    PropReader r(props);
    Property p;
    size_t start = 0;
    while (r.next(p)) {
        size_t end = r.offset();
        bool keep = true;
        for (size_t i = 0; i < skipCount; ++i)
            if (skip[i] == p.id)
                keep = false;
        if (keep)
            w_.raw(props.data + start, end - start);
        start = end;
    }
}

}  // namespace tinymq
