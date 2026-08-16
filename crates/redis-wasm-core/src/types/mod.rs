//! Core data types for Redis-like values

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

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

/// Main value enum representing all Redis data types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    String(String),
    List(VecDeque<String>),
    Set(HashSet<String>),
    SortedSet(BTreeMap<String, OrderedFloat<f64>>), // member -> score
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
        Value::String(String::new())
    }

    /// Try to get as string
    pub fn as_string(&self) -> Option<&String> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as mutable string
    pub fn as_string_mut(&mut self) -> Option<&mut String> {
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
    pub fn as_sorted_set(&self) -> Option<&BTreeMap<String, OrderedFloat<f64>>> {
        match self {
            Value::SortedSet(z) => Some(z),
            _ => None,
        }
    }

    /// Try to get as mutable sorted set
    pub fn as_sorted_set_mut(&mut self) -> Option<&mut BTreeMap<String, OrderedFloat<f64>>> {
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
        Value::String(String::new())
    }
}

// ============================================================================
// String Operations
// ============================================================================

impl Value {
    /// Create a new string value
    pub fn new_string(s: String) -> Self {
        Value::String(s)
    }

    /// Create a new string value from str
    pub fn new_string_str(s: &str) -> Self {
        Value::String(s.to_string())
    }

    /// Append to string value (returns new length)
    pub fn append(&mut self, suffix: &str) -> Result<usize, TypeError> {
        let s = self.as_string_mut().ok_or(TypeError::WrongType)?;
        s.push_str(suffix);
        Ok(s.len())
    }

    /// Get substring (start, end inclusive like Redis)
    pub fn get_range(&self, start: isize, end: isize) -> Result<String, TypeError> {
        let s = self.as_string().ok_or(TypeError::WrongType)?;
        let len = s.chars().count() as isize;
        if len == 0 {
            return Ok(String::new());
        }

        let start = if start < 0 { (len + start).max(0) } else { start.min(len) };
        let end = if end < 0 { (len + end).max(-1) } else { end.min(len - 1) };

        if start > end || start >= len {
            return Ok(String::new());
        }

        let chars: Vec<char> = s.chars().collect();
        Ok(chars[start as usize..=end as usize].iter().collect())
    }

    /// Set range (overwrite substring)
    pub fn set_range(&mut self, offset: usize, value: &str) -> Result<usize, TypeError> {
        let s = self.as_string_mut().ok_or(TypeError::WrongType)?;
        let chars: Vec<char> = value.chars().collect();
        let mut s_chars: Vec<char> = s.chars().collect();

        // Extend if necessary
        if offset + chars.len() > s_chars.len() {
            s_chars.resize(offset + chars.len(), '\0');
        }

        for (i, c) in chars.into_iter().enumerate() {
            s_chars[offset + i] = c;
        }

        *s = s_chars.into_iter().collect();
        Ok(s.len())
    }

    /// Get string length
    pub fn str_len(&self) -> Result<usize, TypeError> {
        self.as_string().map(|s| s.len()).ok_or(TypeError::WrongType)
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
        let len = list.len() as isize;
        if len == 0 {
            return Ok(Vec::new());
        }

        let start = if start < 0 { (len + start).max(0) } else { start.min(len) };
        let stop = if stop < 0 { (len + stop).max(-1) } else { stop.min(len - 1) };

        if start > stop || start >= len {
            return Ok(Vec::new());
        }

        let end = (stop as usize + 1).min(len as usize);
        Ok(list.range(start as usize..end).cloned().collect())
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
        let len = list.len() as isize;
        if len == 0 {
            return Ok(());
        }

        let start = if start < 0 { (len + start).max(0) } else { start.min(len) };
        let stop = if stop < 0 { (len + stop).max(-1) } else { stop.min(len - 1) };

        if start > stop || start >= len {
            list.clear();
            return Ok(());
        }

        let end = (stop as usize + 1).min(len as usize);
        let new_list: VecDeque<String> = list.range(start as usize..end).cloned().collect();
        *list = new_list;
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
        self.as_set().map(|s| s.contains(member)).ok_or(TypeError::WrongType)
    }

    /// Get all members (SMEMBERS)
    pub fn smembers(&self) -> Result<Vec<String>, TypeError> {
        self.as_set().map(|s| s.iter().cloned().collect()).ok_or(TypeError::WrongType)
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
        Value::SortedSet(BTreeMap::new())
    }

    /// Add members with scores (ZADD)
    pub fn zadd(&mut self, members: &[(String, f64)]) -> Result<usize, TypeError> {
        let zset = self.as_sorted_set_mut().ok_or(TypeError::WrongType)?;
        let mut added = 0;
        for (member, score) in members {
            if !zset.contains_key(member) {
                added += 1;
            }
            zset.insert(member.clone(), OrderedFloat(*score));
        }
        Ok(added)
    }

    /// Remove members (ZREM)
    pub fn zrem(&mut self, members: &[String]) -> Result<usize, TypeError> {
        let zset = self.as_sorted_set_mut().ok_or(TypeError::WrongType)?;
        let mut removed = 0;
        for m in members {
            if zset.remove(m).is_some() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Get score (ZSCORE)
    pub fn zscore(&self, member: &str) -> Result<Option<f64>, TypeError> {
        self.as_sorted_set()
            .map(|z| z.get(member).map(|s| s.into_inner()))
            .ok_or(TypeError::WrongType)
    }

    /// Get rank (ZRANK) - 0-based
    pub fn zrank(&self, member: &str) -> Result<Option<usize>, TypeError> {
        let zset = self.as_sorted_set().ok_or(TypeError::WrongType)?;
        for (i, (m, _)) in zset.iter().enumerate() {
            if m == member {
                return Ok(Some(i));
            }
        }
        Ok(None)
    }

    /// Get reverse rank (ZREVRANK) - 0-based, highest score is rank 0
    pub fn zrevrank(&self, member: &str) -> Result<Option<usize>, TypeError> {
        let zset = self.as_sorted_set().ok_or(TypeError::WrongType)?;
        for (i, (m, _)) in zset.iter().rev().enumerate() {
            if m == member {
                return Ok(Some(i));
            }
        }
        Ok(None)
    }

    /// Get range by index (ZRANGE)
    pub fn zrange(&self, start: isize, stop: isize, with_scores: bool) -> Result<Vec<String>, TypeError> {
        let zset = self.as_sorted_set().ok_or(TypeError::WrongType)?;
        let len = zset.len() as isize;

        let start = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
        let stop = if stop < 0 { (len + stop).max(-1) } else { stop.min(len - 1) } as isize;

        if start > stop as usize || start >= len as usize {
            return Ok(Vec::new());
        }

        let end = (stop as usize + 1).min(len as usize);
        let result: Vec<String> = zset
            .keys()
            .skip(start)
            .take(end - start)
            .cloned()
            .collect();

        if with_scores {
            // Return as "member score member score..." format for Redis compatibility
            let scores: Vec<String> = zset
                .values()
                .skip(start)
                .take(end - start)
                .map(|s| s.to_string())
                .collect();
            let mut combined = Vec::with_capacity(result.len() * 2);
            for (m, s) in result.into_iter().zip(scores) {
                combined.push(m);
                combined.push(s);
            }
            Ok(combined)
        } else {
            Ok(result)
        }
    }

    /// Get reverse range by index (ZREVRANGE)
    pub fn zrevrange(&self, start: isize, stop: isize, with_scores: bool) -> Result<Vec<String>, TypeError> {
        let zset = self.as_sorted_set().ok_or(TypeError::WrongType)?;
        let len = zset.len() as isize;

        let start = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
        let stop = if stop < 0 { (len + stop).max(-1) } else { stop.min(len - 1) } as isize;

        if start > stop as usize || start >= len as usize {
            return Ok(Vec::new());
        }

        let end = (stop as usize + 1).min(len as usize);
        let keys: Vec<_> = zset.keys().cloned().collect();
        let values: Vec<_> = zset.values().cloned().collect();

        let result_keys: Vec<String> = keys.into_iter().rev().skip(start).take(end - start).collect();
        let result_values: Vec<f64> = values.into_iter().rev().skip(start).take(end - start).map(|v| v.into_inner()).collect();

        if with_scores {
            let mut combined = Vec::with_capacity(result_keys.len() * 2);
            for (m, s) in result_keys.into_iter().zip(result_values) {
                combined.push(m);
                combined.push(s.to_string());
            }
            Ok(combined)
        } else {
            Ok(result_keys)
        }
    }

    /// Get range by score (ZRANGEBYSCORE)
    pub fn zrangebyscore(&self, min: f64, max: f64) -> Result<Vec<(String, f64)>, TypeError> {
        let zset = self.as_sorted_set().ok_or(TypeError::WrongType)?;
        Ok(zset
            .iter()
            .filter(|(_, v)| v.0 >= min && v.0 <= max)
            .map(|(k, v)| (k.clone(), v.0))
            .collect())
    }

    /// Get cardinality (ZCARD)
    pub fn zcard(&self) -> Result<usize, TypeError> {
        self.as_sorted_set().map(|z| z.len()).ok_or(TypeError::WrongType)
    }

    /// Count in score range (ZCOUNT)
    pub fn zcount(&self, min: f64, max: f64) -> Result<usize, TypeError> {
        let zset = self.as_sorted_set().ok_or(TypeError::WrongType)?;
        Ok(zset.values().filter(|v| v.0 >= min && v.0 <= max).count())
    }
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
        self.as_hash().map(|h| h.get(field).cloned()).ok_or(TypeError::WrongType)
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
        self.as_hash().map(|h| h.contains_key(field)).ok_or(TypeError::WrongType)
    }

    /// Get length (HLEN)
    pub fn hlen(&self) -> Result<usize, TypeError> {
        self.as_hash().map(|h| h.len()).ok_or(TypeError::WrongType)
    }

    /// Get all keys (HKEYS)
    pub fn hkeys(&self) -> Result<Vec<String>, TypeError> {
        self.as_hash().map(|h| h.keys().cloned().collect()).ok_or(TypeError::WrongType)
    }

    /// Get all values (HVALS)
    pub fn hvals(&self) -> Result<Vec<String>, TypeError> {
        self.as_hash().map(|h| h.values().cloned().collect()).ok_or(TypeError::WrongType)
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zrevrank() {
        let mut z = Value::new_sorted_set();
        z.zadd(&[("a".into(), 1.0), ("b".into(), 2.0), ("c".into(), 3.0)]).unwrap();
        assert_eq!(z.zrank("a").unwrap(), Some(0));
        assert_eq!(z.zrank("c").unwrap(), Some(2));
        // Highest score has reverse rank 0
        assert_eq!(z.zrevrank("c").unwrap(), Some(0));
        assert_eq!(z.zrevrank("a").unwrap(), Some(2));
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
        let v = Value::new_string("a".to_string());
        assert_eq!(v.get_range(5, 10).unwrap(), "");
        assert_eq!(v.get_range(0, 0).unwrap(), "a");
        assert_eq!(v.get_range(0, -1).unwrap(), "a");
    }
}