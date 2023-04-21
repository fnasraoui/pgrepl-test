use serde::Serializer;
use serde::Serialize;
use serde::ser::SerializeStruct;
use postgres_protocol::message::backend::*;

#[derive(Debug)]
pub(crate) struct SerializedXLogDataBody<T>(pub XLogDataBody<T>);

impl Serialize for SerializedXLogDataBody<LogicalReplicationMessage> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let data = &SerializedLogicalReplicationMessage(&self.0.data());
        let mut state = serializer.serialize_struct("XLogDataBody", 3)?;
        state.serialize_field("wal_start", &self.0.wal_start())?;
        state.serialize_field("wal_end", &self.0.wal_end())?;
        state.serialize_field("timestamp", &self.0.timestamp())?;
        state.serialize_field("data", &data)?;
        state.end()
    }
}

pub(crate) struct SerializedLogicalReplicationMessage<'a>(pub &'a LogicalReplicationMessage);

impl<'a> Serialize for SerializedLogicalReplicationMessage<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        let mut state = serializer.serialize_struct("LogicalReplicationMessage", 5)?;
        match self.0 {
            LogicalReplicationMessage::Begin(ref msg) => {
                state.serialize_field("final_lsn", &msg.final_lsn())?;
                state.serialize_field("timestamp", &msg.timestamp())?;
                state.serialize_field("xid", &msg.xid())?;
            }
            LogicalReplicationMessage::Commit(ref msg) => {
                state.serialize_field("flags", &msg.flags())?;
                state.serialize_field("commit_lsn", &msg.commit_lsn())?;
                state.serialize_field("end_lsn", &msg.end_lsn())?;
                state.serialize_field("timestamp", &msg.timestamp())?;
            }
            LogicalReplicationMessage::Origin(ref msg) => {
                state.serialize_field("commit_lsn", &msg.commit_lsn())?;
                // state.serialize_field("name", &msg.name())?;
            }
            LogicalReplicationMessage::Relation(ref msg) => {
                state.serialize_field("rel_id", &msg.rel_id())?;
                // state.serialize_field("namespace", &msg.namespace())?;
                // state.serialize_field("name", &msg.name())?;
                // state.serialize_field("replica_identity", &msg.replica_identity())?;
                // state.serialize_field("columns", &msg.columns())?;
            }
            LogicalReplicationMessage::Type(ref msg) => {
                state.serialize_field("id", &msg.id())?;
                // state.serialize_field("namespace", &msg.namespace())?;
                // state.serialize_field("name", &msg.name())?;
            }
            LogicalReplicationMessage::Insert(ref msg) => {
                state.serialize_field("rel_id", &msg.rel_id())?;
                // state.serialize_field("tuple", &msg.tuple())?;
            }
            LogicalReplicationMessage::Update(ref msg) => {
                if let Some(_old_tuple) = &msg.old_tuple() {
                    // state.serialize_field("old_tuple", &old_tuple)?;
                }
                if let Some(_key_tuple) = &msg.key_tuple() {
                    // state.serialize_field("key_tuple", &key_tuple)?;
                }
                // state.serialize_field("new_tuple", &msg.new_tuple())?;
            }
            LogicalReplicationMessage::Delete(ref msg) => {
                if let Some(_old_tuple) = &msg.old_tuple() {
                    // state.serialize_field("old_tuple", &old_tuple)?;
                }
                if let Some(_key_tuple) = &msg.key_tuple() {
                    // state.serialize_field("key_tuple", &key_tuple)?;
                }
            }
            LogicalReplicationMessage::Truncate(ref msg) => {
                state.serialize_field("options", &msg.options())?;
                state.serialize_field("rel_ids", &msg.rel_ids())?;
            }
            _ => {}
        }
        state.end()
    }
}