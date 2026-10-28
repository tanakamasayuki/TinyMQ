// Topic names, topic filters and matching.
#pragma once

#include "mq_types.h"

namespace tinymq {

// A topic name to publish to: not empty, no wildcards.
bool validTopicName(Bytes topic);

// A topic filter to subscribe with: not empty, `+` takes a whole level,
// `#` only as the whole last level.
bool validTopicFilter(Bytes filter);

// Whether `topic` matches `filter`. Both must be valid. A filter starting
// with a wildcard does not match a topic starting with `$`.
bool topicMatches(Bytes filter, Bytes topic);

// Splits a shared subscription `$share/<group>/<filter>`. Returns false if
// `filter` is not one; returns true with an empty `group` if it is one but
// malformed (empty or wildcard group name, nothing after the group).
bool parseSharedFilter(Bytes filter, Bytes &group, Bytes &inner);

}  // namespace tinymq
