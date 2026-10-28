#include "mq_topic.h"

namespace tinymq {

bool validTopicName(Bytes topic)
{
    if (topic.len == 0)
        return false;
    for (uint32_t i = 0; i < topic.len; ++i)
        if (topic.data[i] == '+' || topic.data[i] == '#')
            return false;
    return true;
}

bool validTopicFilter(Bytes filter)
{
    if (filter.len == 0)
        return false;
    for (uint32_t i = 0; i < filter.len; ++i) {
        uint8_t c = filter.data[i];
        bool levelStart = i == 0 || filter.data[i - 1] == '/';
        bool levelEnd = i + 1 == filter.len || filter.data[i + 1] == '/';
        if (c == '+' && !(levelStart && levelEnd))
            return false;
        if (c == '#' && !(levelStart && i + 1 == filter.len))
            return false;
    }
    return true;
}

bool topicMatches(Bytes filter, Bytes topic)
{
    const uint8_t *f = filter.data;
    const uint8_t *fe = f + filter.len;
    const uint8_t *t = topic.data;
    const uint8_t *te = t + topic.len;

    if (topic.len && topic.data[0] == '$' && filter.len && (f[0] == '+' || f[0] == '#'))
        return false;

    // Compare level by level; f and t point at the start of a level.
    for (;;) {
        if (f < fe && *f == '#')
            return true;  // matches this level and everything below, including nothing
        if (f < fe && *f == '+') {
            ++f;
            while (t < te && *t != '/')
                ++t;
        } else {
            while (f < fe && t < te && *f != '/' && *t != '/') {
                if (*f != *t)
                    return false;
                ++f;
                ++t;
            }
            if ((f < fe && *f != '/') || (t < te && *t != '/'))
                return false;  // the level differs in length
        }
        // both at the end of a level
        if (f == fe && t == te)
            return true;
        if (f == fe)
            return false;  // topic has more levels
        if (t == te) {
            // topic ended; "a/#" still matches "a"
            return fe - f == 2 && f[0] == '/' && f[1] == '#';
        }
        ++f;  // skip '/'
        ++t;
    }
}

bool parseSharedFilter(Bytes filter, Bytes &group, Bytes &inner)
{
    static const char kPrefix[] = "$share/";
    const uint32_t n = sizeof(kPrefix) - 1;
    if (filter.len < n || memcmp(filter.data, kPrefix, n) != 0)
        return false;
    group = Bytes();
    inner = Bytes();
    uint32_t i = n;
    while (i < filter.len && filter.data[i] != '/')
        ++i;
    Bytes g(filter.data + n, i - n);
    if (g.len == 0 || i + 1 >= filter.len)
        return true;
    for (uint32_t k = 0; k < g.len; ++k)
        if (g.data[k] == '+' || g.data[k] == '#')
            return true;
    group = g;
    inner = Bytes(filter.data + i + 1, filter.len - i - 1);
    return true;
}

}  // namespace tinymq
