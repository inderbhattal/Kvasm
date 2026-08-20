//! Core data types for Redis-like values

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

/// A Redis sorted set: members ranked by (score, member), like Redis ZSETs.
///
/// Keeps a member -> score map for O(1) score lookup plus a BTreeSet ordered
/// by (score, member) for rank/range queries. The two are always in sync.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SortedSet {
    scores: HashMap<String, OrderedFloat<f64>>,
    order: BTreeSet<(OrderedFloat<f64>, String)>,
}

impl SortedSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or update a member. Returns true if the member was new.
    pub fn insert(&mut self, member: &str, score: f64) -> bool {
        let score = OrderedFloat(score);
        match self.scores.insert(member.to_string(), score) {
            Some(old) => {
                if old != score {
                    self.order.remove(&(old, member.to_string()));
                    self.order.insert((score, member.to_string()));
                }
                false
            }
            None => {
                self.order.insert((score, member.to_string()));
                true
            }
        }
    }

    /// Remove a member. Returns true if it was present.
    pub fn remove(&mut self, member: &str) -> bool {
        match self.scores.remove(member) {
            Some(score) => {
                self.order.remove(&(score, member.to_string()));
                true
            }
            None => false,
        }
    }

    pub fn score(&self, member: &str) -> Option<f64> {
        self.scores.get(member).map(|s| s.0)
    }

    /// 0-based rank in ascending (score, member) order.
    pub fn rank(&self, member: &str) -> Option<usize> {
        let score = *self.scores.get(member)?;
        self.order
            .iter()
            .position(|(s, m)| *s == score && m == member)
    }

    pub fn len(&self) -> usize {
        self.scores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    /// Iterate members in ascending (score, member) order.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&str, f64)> {
        self.order.iter().map(|(s, m)| (m.as_str(), s.0))
    }
}

/// The type of a Redis value
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueType {
    String,
    List,
    Set,
    SortedSet,
    Hash,
    None, // Key doesn't exist
}

impl ValueType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ValueType::String => "string",
            ValueType::List => "list",
            ValueType::Set => "set",
            ValueType::SortedSet => "zset",
            ValueType::Hash => "hash",
            ValueType::None => "none",
        }
    }
}

/// Main value enum representing all Redis data types.
///
/// Strings are binary-safe byte sequences, like Redis strings. Callers that
/// need text decode them (typically as UTF-8) at the API boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    String(Vec<u8>),
    List(VecDeque<String>),
    Set(HashSet<String>),
    SortedSet(SortedSet),
    Hash(HashMap<String, String>),
}

impl Value {
    /// Returns the type name of this value
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::List(_) => "list",
            Value::Set(_) => "set",
            Value::SortedSet(_) => "zset",
            Value::Hash(_) => "hash",
        }
    }

    /// Returns the ValueType enum
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::String(_) => ValueType::String,
            Value::List(_) => ValueType::List,
            Value::Set(_) => ValueType::Set,
            Value::SortedSet(_) => ValueType::SortedSet,
            Value::Hash(_) => ValueType::Hash,
        }
    }

    /// Returns the number of elements in the value
    pub fn len(&self) -> usize {
        match self {
            Value::String(s) => s.len(),
            Value::List(l) => l.len(),
            Value::Set(s) => s.len(),
            Value::SortedSet(z) => z.len(),
            Value::Hash(h) => h.len(),
        }
    }

    /// Returns true if the value is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create a new empty string value (for or_insert_with)
    pub fn new_empty_string() -> Self {
        Value::String(Vec::new())
    }

    /// Try to get as string bytes
    pub fn as_bytes(&self) -> Option<&Vec<u8>> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as mutable string bytes
    pub fn as_bytes_mut(&mut self) -> Option<&mut Vec<u8>> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as list
    pub fn as_list(&self) -> Option<&VecDeque<String>> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    /// Try to get as mutable list
    pub fn as_list_mut(&mut self) -> Option<&mut VecDeque<String>> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    /// Try to get as set
    pub fn as_set(&self) -> Option<&HashSet<String>> {
        match self {
            Value::Set(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as mutable set
    pub fn as_set_mut(&mut self) -> Option<&mut HashSet<String>> {
        match self {
            Value::Set(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as sorted set
    pub fn as_sorted_set(&self) -> Option<&SortedSet> {
        match self {
            Value::SortedSet(z) => Some(z),
            _ => None,
        }
    }

    /// Try to get as mutable sorted set
    pub fn as_sorted_set_mut(&mut self) -> Option<&mut SortedSet> {
        match self {
            Value::SortedSet(z) => Some(z),
            _ => None,
        }
    }

    /// Try to get as hash
    pub fn as_hash(&self) -> Option<&HashMap<String, String>> {
        match self {
            Value::Hash(h) => Some(h),
            _ => None,
        }
    }

    /// Try to get as mutable hash
    pub fn as_hash_mut(&mut self) -> Option<&mut HashMap<String, String>> {
        match self {
            Value::Hash(h) => Some(h),
            _ => None,
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::String(Vec::new())
    }
}

// ============================================================================
// String Operations
// ============================================================================

/// Maximum string value size in bytes (Redis' default proto-max-bulk-len)
pub const MAX_STRING_SIZE: usize = 512 * 1024 * 1024;

impl Value {
    /// Create a new string value from anything byte-like
    pub fn new_string(s: impl Into<Vec<u8>>) -> Self {
        Value::String(s.into())
    }

    /// Create a new string value from str
    pub fn new_string_str(s: &str) -> Self {
        Value::String(s.as_bytes().to_vec())
    }

    /// Append bytes to string value (returns new byte length)
    pub fn append(&mut self, suffix: &[u8]) -> Result<usize, TypeError> {
        let s = self.as_bytes_mut().ok_or(TypeError::WrongType)?;
        s.len()
            .checked_add(suffix.len())
            .filter(|&len| len <= MAX_STRING_SIZE)
            .ok_or(TypeError::StringTooLarge)?;
        s.extend_from_slice(suffix);
        Ok(s.len())
    }

    /// Get byte range (start, end inclusive like Redis GETRANGE)
    pub fn get_range(&self, start: isize, end: isize) -> Result<Vec<u8>, TypeError> {
        let s = self.as_bytes().ok_or(TypeError::WrongType)?;
        let Some((start, end)) = Self::clamp_range(s.len(), start, end) else {
            return Ok(Vec::new());
        };
        Ok(s[start..end].to_vec())
    }

    /// Overwrite bytes at a byte offset, zero-padding any gap
    /// (Redis SETRANGE). Returns the new byte length.
    pub fn set_range(&mut self, offset: usize, value: &[u8]) -> Result<usize, TypeError> {
        let s = self.as_bytes_mut().ok_or(TypeError::WrongType)?;
        // Redis: an empty value never modifies or extends the string.
        if value.is_empty() {
            return Ok(s.len());
        }

        // Checked, capped arithmetic: a hostile offset must error like Redis,
        // not wrap (32-bit wasm) or attempt a multi-GB allocation.
        let end = offset
            .checked_add(value.len())
            .filter(|&end| end <= MAX_STRING_SIZE)
            .ok_or(TypeError::StringTooLarge)?;
        if end > s.len() {
            s.resize(end, 0);
        }
        s[offset..end].copy_from_slice(value);
        Ok(s.len())
    }

    /// Get string length in bytes (Redis STRLEN)
    pub fn str_len(&self) -> Result<usize, TypeError> {
        self.as_bytes().map(|s| s.len()).ok_or(TypeError::WrongType)
    }
}

// ============================================================================
// List Operations
// ============================================================================

impl Value {
    /// Create a new list value
    pub fn new_list() -> Self {
        Value::List(VecDeque::new())
    }

    /// Create list from vector
    pub fn new_list_from(vec: Vec<String>) -> Self {
        Value::List(VecDeque::from(vec))
    }

    /// Push to left (LPUSH)
    pub fn lpush(&mut self, values: &[String]) -> Result<usize, TypeError> {
        let list = self.as_list_mut().ok_or(TypeError::WrongType)?;
        // Insert in order so the last value ends up at the head (Redis LPUSH).
        for v in values {
            list.push_front(v.clone());
        }
        Ok(list.len())
    }

    /// Push to right (RPUSH)
    pub fn rpush(&mut self, values: &[String]) -> Result<usize, TypeError> {
        let list = self.as_list_mut().ok_or(TypeError::WrongType)?;
        for v in values {
            list.push_back(v.clone());
        }
        Ok(list.len())
    }

    /// Pop from left (LPOP)
    pub fn lpop(&mut self, count: usize) -> Result<Vec<String>, TypeError> {
        let list = self.as_list_mut().ok_or(TypeError::WrongType)?;
        let mut result = Vec::new();
        for _ in 0..count.min(list.len()) {
            if let Some(v) = list.pop_front() {
                result.push(v);
            }
        }
        Ok(result)
    }

    /// Pop from right (RPOP)
    pub fn rpop(&mut self, count: usize) -> Result<Vec<String>, TypeError> {
        let list = self.as_list_mut().ok_or(TypeError::WrongType)?;
        let mut result = Vec::new();
        for _ in 0..count.min(list.len()) {
            if let Some(v) = list.pop_back() {
                result.push(v);
            }
        }
        Ok(result)
    }

    /// Get range (LRANGE)
    pub fn lrange(&self, start: isize, stop: isize) -> Result<Vec<String>, TypeError> {
        let list = self.as_list().ok_or(TypeError::WrongType)?;
        let Some((start, end)) = Self::clamp_range(list.len(), start, stop) else {
            return Ok(Vec::new());
        };
        Ok(list.range(start..end).cloned().collect())
    }

    /// Get length (LLEN)
    pub fn llen(&self) -> Result<usize, TypeError> {
        self.as_list().map(|l| l.len()).ok_or(TypeError::WrongType)
    }

    /// Get element at index (LINDEX)
    pub fn lindex(&self, index: isize) -> Result<Option<String>, TypeError> {
        let list = self.as_list().ok_or(TypeError::WrongType)?;
        let len = list.len() as isize;
        let idx = if index < 0 { len + index } else { index };

        if idx < 0 || idx >= len {
            return Ok(None);
        }

        Ok(list.get(idx as usize).cloned())
    }

    /// Set element at index (LSET)
    pub fn lset(&mut self, index: isize, value: String) -> Result<(), TypeError> {
        let list = self.as_list_mut().ok_or(TypeError::WrongType)?;
        let len = list.len() as isize;
        let idx = if index < 0 { len + index } else { index };

        if idx < 0 || idx >= len {
            return Err(TypeError::IndexOutOfRange);
        }

        list[idx as usize] = value;
        Ok(())
    }

    /// Remove elements (LREM)
    pub fn lrem(&mut self, count: isize, value: &str) -> Result<usize, TypeError> {
        let list = self.as_list_mut().ok_or(TypeError::WrongType)?;
        let mut removed = 0;

        if count > 0 {
            // Remove from head
            list.retain(|v| {
                if removed < count as usize && v == value {
                    removed += 1;
                    false
                } else {
                    true
                }
            });
        } else if count < 0 {
            // Remove from tail
            let mut indices = Vec::new();
            for (i, v) in list.iter().rev().enumerate() {
                if v == value {
                    indices.push(list.len() - 1 - i);
                    if indices.len() == (-count) as usize {
                        break;
                    }
                }
            }
            for &idx in &indices {
                list.remove(idx);
                removed += 1;
            }
        } else {
            // Remove all
            let original_len = list.len();
            list.retain(|v| v != value);
            removed = original_len - list.len();
        }

        Ok(removed)
    }

    /// Trim list (LTRIM)
    pub fn ltrim(&mut self, start: isize, stop: isize) -> Result<(), TypeError> {
        let list = self.as_list_mut().ok_or(TypeError::WrongType)?;
        match Self::clamp_range(list.len(), start, stop) {
            Some((start, end)) => {
                list.truncate(end);
                list.drain(..start);
            }
            None => list.clear(),
        }
        Ok(())
    }
}

// ============================================================================
// Set Operations
// ============================================================================

impl Value {
    /// Create a new set value
    pub fn new_set() -> Self {
        Value::Set(HashSet::new())
    }

    /// Add members (SADD)
    pub fn sadd(&mut self, members: &[String]) -> Result<usize, TypeError> {
        let set = self.as_set_mut().ok_or(TypeError::WrongType)?;
        let mut added = 0;
        for m in members {
            if set.insert(m.clone()) {
                added += 1;
            }
        }
        Ok(added)
    }

    /// Remove members (SREM)
    pub fn srem(&mut self, members: &[String]) -> Result<usize, TypeError> {
        let set = self.as_set_mut().ok_or(TypeError::WrongType)?;
        let mut removed = 0;
        for m in members {
            if set.remove(m) {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Check membership (SISMEMBER)
    pub fn sismember(&self, member: &str) -> Result<bool, TypeError> {
        self.as_set()
            .map(|s| s.contains(member))
            .ok_or(TypeError::WrongType)
    }

    /// Get all members (SMEMBERS)
    pub fn smembers(&self) -> Result<Vec<String>, TypeError> {
        self.as_set()
            .map(|s| s.iter().cloned().collect())
            .ok_or(TypeError::WrongType)
    }

    /// Get cardinality (SCARD)
    pub fn scard(&self) -> Result<usize, TypeError> {
        self.as_set().map(|s| s.len()).ok_or(TypeError::WrongType)
    }

    /// Intersection (SINTER)
    pub fn sinter(&self, other: &HashSet<String>) -> Result<Vec<String>, TypeError> {
        let set = self.as_set().ok_or(TypeError::WrongType)?;
        Ok(set.intersection(other).cloned().collect())
    }

    /// Union (SUNION)
    pub fn sunion(&self, other: &HashSet<String>) -> Result<Vec<String>, TypeError> {
        let set = self.as_set().ok_or(TypeError::WrongType)?;
        Ok(set.union(other).cloned().collect())
    }

    /// Difference (SDIFF)
    pub fn sdiff(&self, other: &HashSet<String>) -> Result<Vec<String>, TypeError> {
        let set = self.as_set().ok_or(TypeError::WrongType)?;
        Ok(set.difference(other).cloned().collect())
    }
}

// ============================================================================
// Sorted Set Operations
// ============================================================================

impl Value {
    /// Create a new sorted set value
    pub fn new_sorted_set() -> Self {
        Value::SortedSet(SortedSet::new())
    }

    /// Add members with scores (ZADD)
    pub fn zadd(&mut self, members: &[(String, f64)]) -> Result<usize, TypeError> {
        let zset = self.as_sorted_set_mut().ok_or(TypeError::WrongType)?;
        let mut added = 0;
        for (member, score) in members {
            if zset.insert(member, *score) {
                added += 1;
            }
        }
        Ok(added)
    }

    /// Remove members (ZREM)
    pub fn zrem(&mut self, members: &[String]) -> Result<usize, TypeError> {
        let zset = self.as_sorted_set_mut().ok_or(TypeError::WrongType)?;
        let mut removed = 0;
        for m in members {
            if zset.remove(m) {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Get score (ZSCORE)
    pub fn zscore(&self, member: &str) -> Result<Option<f64>, TypeError> {
        self.as_sorted_set()
            .map(|z| z.score(member))
            .ok_or(TypeError::WrongType)
    }

    /// Get rank (ZRANK) - 0-based, ascending score order
    pub fn zrank(&self, member: &str) -> Result<Option<usize>, TypeError> {
        self.as_sorted_set()
            .map(|z| z.rank(member))
            .ok_or(TypeError::WrongType)
    }

    /// Get reverse rank (ZREVRANK) - 0-based, highest score is rank 0
    pub fn zrevrank(&self, member: &str) -> Result<Option<usize>, TypeError> {
        let zset = self.as_sorted_set().ok_or(TypeError::WrongType)?;
        Ok(zset.rank(member).map(|r| zset.len() - 1 - r))
    }

    /// Clamp Redis-style (start, stop) indices to a half-open usize range.
    /// Returns None when the range is empty.
    fn clamp_range(len: usize, start: isize, stop: isize) -> Option<(usize, usize)> {
        let len = len as isize;
        if len == 0 {
            return None;
        }
        let start = if start < 0 {
            (len + start).max(0)
        } else {
            start.min(len)
        };
        let stop = if stop < 0 {
            (len + stop).max(-1)
        } else {
            stop.min(len - 1)
        };
        if start > stop || start >= len {
            return None;
        }
        Some((start as usize, stop as usize + 1))
    }

    /// Get range by index (ZRANGE), ascending score order
    pub fn zrange(
        &self,
        start: isize,
        stop: isize,
        with_scores: bool,
    ) -> Result<Vec<String>, TypeError> {
        let zset = self.as_sorted_set().ok_or(TypeError::WrongType)?;
        let Some((start, end)) = Self::clamp_range(zset.len(), start, stop) else {
            return Ok(Vec::new());
        };
        Ok(collect_range(
            zset.iter().skip(start).take(end - start),
            with_scores,
        ))
    }

    /// Get reverse range by index (ZREVRANGE), descending score order
    pub fn zrevrange(
        &self,
        start: isize,
        stop: isize,
        with_scores: bool,
    ) -> Result<Vec<String>, TypeError> {
        let zset = self.as_sorted_set().ok_or(TypeError::WrongType)?;
        let Some((start, end)) = Self::clamp_range(zset.len(), start, stop) else {
            return Ok(Vec::new());
        };
        Ok(collect_range(
            zset.iter().rev().skip(start).take(end - start),
            with_scores,
        ))
    }

    /// Get range by score (ZRANGEBYSCORE), ascending score order
    pub fn zrangebyscore(&self, min: f64, max: f64) -> Result<Vec<(String, f64)>, TypeError> {
        let zset = self.as_sorted_set().ok_or(TypeError::WrongType)?;
        Ok(zset
            .iter()
            .filter(|(_, s)| *s >= min && *s <= max)
            .map(|(m, s)| (m.to_string(), s))
            .collect())
    }

    /// Get cardinality (ZCARD)
    pub fn zcard(&self) -> Result<usize, TypeError> {
        self.as_sorted_set()
            .map(|z| z.len())
            .ok_or(TypeError::WrongType)
    }

    /// Count in score range (ZCOUNT)
    pub fn zcount(&self, min: f64, max: f64) -> Result<usize, TypeError> {
        let zset = self.as_sorted_set().ok_or(TypeError::WrongType)?;
        Ok(zset.iter().filter(|(_, s)| *s >= min && *s <= max).count())
    }
}

/// Flatten a (member, score) iterator to Redis reply format:
/// [member, ...] or [member, score, member, score, ...] with scores.
fn collect_range<'a>(iter: impl Iterator<Item = (&'a str, f64)>, with_scores: bool) -> Vec<String> {
    let mut out = Vec::new();
    for (member, score) in iter {
        out.push(member.to_string());
        if with_scores {
            out.push(score.to_string());
        }
    }
    out
}

// ============================================================================
// Hash Operations
// ============================================================================

impl Value {
    /// Create a new hash value
    pub fn new_hash() -> Self {
        Value::Hash(HashMap::new())
    }

    /// Set field (HSET)
    pub fn hset(&mut self, field: String, value: String) -> Result<usize, TypeError> {
        let hash = self.as_hash_mut().ok_or(TypeError::WrongType)?;
        let is_new = !hash.contains_key(&field);
        hash.insert(field, value);
        Ok(if is_new { 1 } else { 0 })
    }

    /// Get field (HGET)
    pub fn hget(&self, field: &str) -> Result<Option<String>, TypeError> {
        self.as_hash()
            .map(|h| h.get(field).cloned())
            .ok_or(TypeError::WrongType)
    }

    /// Get all fields and values (HGETALL)
    pub fn hgetall(&self) -> Result<Vec<(String, String)>, TypeError> {
        self.as_hash()
            .map(|h| h.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .ok_or(TypeError::WrongType)
    }

    /// Delete fields (HDEL)
    pub fn hdel(&mut self, fields: &[String]) -> Result<usize, TypeError> {
        let hash = self.as_hash_mut().ok_or(TypeError::WrongType)?;
        let mut deleted = 0;
        for f in fields {
            if hash.remove(f).is_some() {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// Check field exists (HEXISTS)
    pub fn hexists(&self, field: &str) -> Result<bool, TypeError> {
        self.as_hash()
            .map(|h| h.contains_key(field))
            .ok_or(TypeError::WrongType)
    }

    /// Get length (HLEN)
    pub fn hlen(&self) -> Result<usize, TypeError> {
        self.as_hash().map(|h| h.len()).ok_or(TypeError::WrongType)
    }

    /// Get all keys (HKEYS)
    pub fn hkeys(&self) -> Result<Vec<String>, TypeError> {
        self.as_hash()
            .map(|h| h.keys().cloned().collect())
            .ok_or(TypeError::WrongType)
    }

    /// Get all values (HVALS)
    pub fn hvals(&self) -> Result<Vec<String>, TypeError> {
        self.as_hash()
            .map(|h| h.values().cloned().collect())
            .ok_or(TypeError::WrongType)
    }
}

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum TypeError {
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
    #[error("ERR index out of range")]
    IndexOutOfRange,
    #[error("ERR string exceeds maximum allowed size (proto-max-bulk-len)")]
    StringTooLarge,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zrevrank() {
        let mut z = Value::new_sorted_set();
        z.zadd(&[("a".into(), 1.0), ("b".into(), 2.0), ("c".into(), 3.0)])
            .unwrap();
        assert_eq!(z.zrank("a").unwrap(), Some(0));
        assert_eq!(z.zrank("c").unwrap(), Some(2));
        // Highest score has reverse rank 0
        assert_eq!(z.zrevrank("c").unwrap(), Some(0));
        assert_eq!(z.zrevrank("a").unwrap(), Some(2));
    }

    #[test]
    fn test_zset_orders_by_score_not_member() {
        let mut z = Value::new_sorted_set();
        // Member-lexicographic order (a, b, c) differs from score order (c, a, b)
        z.zadd(&[("a".into(), 2.0), ("b".into(), 3.0), ("c".into(), 1.0)])
            .unwrap();
        assert_eq!(z.zrange(0, -1, false).unwrap(), vec!["c", "a", "b"]);
        assert_eq!(z.zrevrange(0, -1, false).unwrap(), vec!["b", "a", "c"]);
        assert_eq!(z.zrank("c").unwrap(), Some(0));
        assert_eq!(z.zrevrank("b").unwrap(), Some(0));

        // Updating a score reorders
        z.zadd(&[("c".into(), 10.0)]).unwrap();
        assert_eq!(z.zrange(0, -1, false).unwrap(), vec!["a", "b", "c"]);
        assert_eq!(z.zscore("c").unwrap(), Some(10.0));

        // Ties break by member name, like Redis
        let mut t = Value::new_sorted_set();
        t.zadd(&[("y".into(), 1.0), ("x".into(), 1.0)]).unwrap();
        assert_eq!(t.zrange(0, -1, false).unwrap(), vec!["x", "y"]);
    }

    #[test]
    fn test_zset_remove_keeps_order_in_sync() {
        let mut z = Value::new_sorted_set();
        z.zadd(&[("a".into(), 1.0), ("b".into(), 2.0)]).unwrap();
        assert_eq!(z.zrem(&["a".into()]).unwrap(), 1);
        assert_eq!(z.zrem(&["a".into()]).unwrap(), 0);
        assert_eq!(z.zrange(0, -1, true).unwrap(), vec!["b", "2"]);
        assert_eq!(z.zcard().unwrap(), 1);
    }

    #[test]
    fn test_zrange_negative_stop_out_of_range() {
        let mut z = Value::new_sorted_set();
        z.zadd(&[("a".into(), 1.0), ("b".into(), 2.0), ("c".into(), 3.0)])
            .unwrap();
        // Previously overflowed (panic in debug builds)
        assert!(z.zrange(0, -10, false).unwrap().is_empty());
        assert!(z.zrevrange(0, -10, false).unwrap().is_empty());
        assert_eq!(z.zrange(-2, -1, false).unwrap(), vec!["b", "c"]);
        assert_eq!(z.zrevrange(0, 0, false).unwrap(), vec!["c"]);
    }

    #[test]
    fn test_lpush_order() {
        let mut l = Value::new_list();
        l.lpush(&["a".into(), "b".into()]).unwrap();
        // Redis LPUSH key a b => [b, a]
        assert_eq!(l.lrange(0, -1).unwrap(), vec!["b", "a"]);
    }

    #[test]
    fn test_lrange_negative_out_of_range() {
        let mut l = Value::new_list();
        l.rpush(&["a".into(), "b".into(), "c".into()]).unwrap();
        assert!(l.lrange(0, -5).unwrap().is_empty());
        assert_eq!(l.lrange(0, -1).unwrap(), vec!["a", "b", "c"]);
        assert_eq!(l.lrange(-1, -1).unwrap(), vec!["c"]);
    }

    #[test]
    fn test_lindex_negative_out_of_range() {
        let mut l = Value::new_list();
        l.rpush(&["a".into(), "b".into(), "c".into()]).unwrap();
        assert_eq!(l.lindex(-5).unwrap(), None);
        assert_eq!(l.lindex(-1).unwrap(), Some("c".to_string()));
    }

    #[test]
    fn test_lset_negative_out_of_range() {
        let mut l = Value::new_list();
        l.rpush(&["a".into(), "b".into(), "c".into()]).unwrap();
        assert!(l.lset(-5, "x".into()).is_err());
        l.lset(-1, "x".into()).unwrap();
        assert_eq!(l.lindex(-1).unwrap(), Some("x".to_string()));
    }

    #[test]
    fn test_get_range_out_of_range() {
        let v = Value::new_string("a");
        assert_eq!(v.get_range(5, 10).unwrap(), b"");
        assert_eq!(v.get_range(0, 0).unwrap(), b"a");
        assert_eq!(v.get_range(0, -1).unwrap(), b"a");
    }

    #[test]
    fn test_string_ops_are_byte_oriented() {
        // "é" is 2 bytes in UTF-8, so byte-oriented ops see length 6.
        let mut v = Value::new_string("héllo");
        assert_eq!(v.str_len().unwrap(), 6);
        assert_eq!(v.get_range(1, 2).unwrap(), "é".as_bytes());
        assert_eq!(v.append(b"!").unwrap(), 7);

        // SETRANGE zero-pads the gap like Redis.
        let mut v = Value::new_string("ab");
        assert_eq!(v.set_range(4, b"cd").unwrap(), 6);
        assert_eq!(v.as_bytes().unwrap(), b"ab\0\0cd");

        // An empty value never extends the string.
        let mut v = Value::new_string("ab");
        assert_eq!(v.set_range(10, b"").unwrap(), 2);
        assert_eq!(v.as_bytes().unwrap(), b"ab");
    }
}
