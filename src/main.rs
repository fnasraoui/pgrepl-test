use anyhow::anyhow;
use futures::{Stream, StreamExt};
use mz_expr::MirScalarExpr;
use mz_postgres_util::{desc::PostgresTableDesc, Config};
use mz_repr::{Datum, DatumVec, Row};
use once_cell::sync::Lazy;
use postgres_protocol::message::backend::{
    LogicalReplicationMessage, ReplicationMessage, XLogDataBody,
};
use std::{
    collections::BTreeMap,
    env,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{
    replication::LogicalReplicationStream, types::PgLsn, Client, SimpleQueryMessage,
};

//https://dev.materialize.com/api/rust/mz_postgres_util/struct.Config.html
//https://github.com/MaterializeInc/materialize/blob/main/src/storage/src/source/postgres.rs#L507

// CREATE DATABASE testdb;
// \c testdb;
// CREATE TABLE testtbl (id int, name text);
// CREATE PUBLICATION testpub FOR TABLE testtbl;
// INSERT INTO testtbl VALUES (1, 'snot');

type ReplicationError = anyhow::Error;

// https://github.com/MaterializeInc/materialize/blob/main/src/storage/src/source/postgres.rs#L836
/// Parses SQL results that are expected to be a single row into a Rust type
fn parse_single_row<T: FromStr>(
    result: &[SimpleQueryMessage],
    column: &str,
) -> Result<T, ReplicationError> {
    let mut rows = result.iter().filter_map(|msg| match msg {
        SimpleQueryMessage::Row(row) => Some(row),
        _ => None,
    });
    match (rows.next(), rows.next()) {
        (Some(row), None) => row
            .get(column)
            .ok_or_else(|| anyhow!("missing expected column: {column}"))
            .and_then(|col| col.parse().map_err(|_| anyhow!("invalid data"))),
        (None, None) => Err(anyhow!("empty result")),
        _ => Err(anyhow!("ambiguous result, more than one row")),
    }
}

#[tokio::main]
async fn main() -> Result<(), ReplicationError> {
    let args: Vec<String> = env::args().collect();
    let mut replication_lsn = if args.len() > 2 {
        PgLsn::from_str(&args[2]).unwrap()
    } else {
        PgLsn::from(0)
    };

    //"host=127.0.0.1 port=5433 user=postgres password=password dbname=testdb",
    let pg_config = tokio_postgres::Config::from_str(&args[1])?;

    let tunnel_config = mz_postgres_util::TunnelConfig::Direct;
    let connection_config = Config::new(pg_config, tunnel_config)?;
    let slot = "gamess";

    let publication = "gamespub";
    let source_id = "source_id";

    if replication_lsn == PgLsn::from(0) {
        println!("======== BEGIN SNAPSHOT ==========");

        let publication_tables =
            mz_postgres_util::publication_info(&connection_config, publication, None).await?;

        // Validate publication tables against the state snapshot
        dbg!(&publication_tables);
        let source_tables: BTreeMap<u32, SourceTable> = publication_tables
            .into_iter()
            .map(|t| {
                (
                    t.oid,
                    SourceTable {
                        output_index: t.oid as usize,
                        desc: t,
                        casts: vec![],
                    },
                )
            })
            .collect();
        let client = connection_config.clone().connect_replication().await?;

        // Technically there is TOCTOU problem here but it makes the code easier and if we end
        // up attempting to create a slot and it already exists we will simply retry
        // Also, we must check if the slot exists before we start a transaction because creating a
        // slot must be the first statement in a transaction
        let res = client
            .simple_query(&format!(
                r#"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{}'"#,
                slot
            ))
            .await?;

        dbg!(&res);
        let slot_lsn = parse_single_row(&res, "confirmed_flush_lsn");
        client
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
            .await?;
        let (slot_lsn, snapshot_lsn, temp_slot): (PgLsn, PgLsn, _) = match slot_lsn {
            Ok(slot_lsn) => {
                dbg!(&slot_lsn);
                // The main slot already exists which means we can't use it for the snapshot. So
                // we'll create a temporary replication slot in order to both set the transaction's
                // snapshot to be a consistent point and also to find out the LSN that the snapshot
                // is going to run at.
                //
                // When this happens we'll most likely be snapshotting at a later LSN than the slot
                // which we will take care below by rewinding.
                let temp_slot = uuid::Uuid::new_v4().to_string().replace('-', "");
                dbg!(&temp_slot);
                let res = client
                    .simple_query(&format!(
                        r#"CREATE_REPLICATION_SLOT {:?} TEMPORARY LOGICAL "pgoutput" USE_SNAPSHOT"#,
                        temp_slot
                    ))
                    .await?;
                dbg!(&res);
                let snapshot_lsn = parse_single_row(&res, "consistent_point")?;
                (slot_lsn, snapshot_lsn, Some(temp_slot))
            }
            Err(e) => {
                dbg!(e);
                let res = client
                    .simple_query(&format!(
                        r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
                        slot
                    ))
                    .await?;
                dbg!(&res);
                let slot_lsn: PgLsn = parse_single_row(&res, "consistent_point")?;
                (slot_lsn, slot_lsn, None)
            }
        };

        dbg!(&slot_lsn, &snapshot_lsn, &temp_slot);

        let mut stream = Box::pin(produce_snapshot(&client, &source_tables).enumerate());

        while let Some((_i, event)) = stream.as_mut().next().await {
            // if i > 0 {
            //     // Failure scenario after we have produced at least one row, but before a
            //     // successful `COMMIT`
            //     // fail::fail_point!("pg_snapshot_failure", |_| {
            //     return Err(anyhow::anyhow!(
            //         "recoverable errors should crash the process"
            //     ));
            //     // });
            // }
            let (output, row) = event?;

            dbg!(output, row, slot_lsn, 1);
        }
        println!("======== END SNAPSHOT ==========");

        if let Some(temp_slot) = temp_slot {
            let _ = client
                .simple_query(&format!("DROP_REPLICATION_SLOT {temp_slot:?}"))
                .await;
        }
        client.simple_query("COMMIT;").await?;

        // Drop the stream and the client, to ensure that the future `produce_replication` don't
        // conflict with the above processing.
        //
        // Its possible we can avoid dropping the `client` value here, but we do it out of an
        // abundance of caution, as rust-postgres has had curious bugs around this.
        drop(stream);
        drop(client);
        assert!(slot_lsn <= snapshot_lsn);
        if slot_lsn < snapshot_lsn {
            println!("postgres snapshot was at {snapshot_lsn:?} but we need it at {slot_lsn:?}. Rewinding");
            // Our snapshot was too far ahead so we must rewind it by reading the replication
            // stream until the snapshot lsn and emitting any rows that we find with negated diffs
            let replication_stream = produce_replication(
                connection_config.clone(),
                slot,
                publication,
                slot_lsn,
                Arc::new(0.into()),
            )
                .await;
            tokio::pin!(replication_stream);

            while let Some(event) = replication_stream.next().await {
                let event = event?;
                dbg!(event);
            }
        }

        println!("replication snapshot for source {} succeeded", &source_id);
        replication_lsn = slot_lsn;
    }

    let replication_stream = produce_replication(
        connection_config.clone(),
        slot,
        publication,
        replication_lsn,
        // Arc::clone(&resume_lsn),
        Arc::new(AtomicU64::new(0)),
    )
        .await;
    tokio::pin!(replication_stream);

    // TODO(petrosagg): The API does not guarantee that we won't see an error after we have already
    // partially emitted a transaction, but we know it is the case due to the implementation. Find
    // a way to encode this in the type signature
    while let Some(event) = replication_stream.next().await.transpose()? {
        // match event {
        //     Event::Message(lsn, (output, row, diff)) => {
        //         task_info.row_sender.send_row(output, row, lsn, diff).await;
        //     }
        //     Event::Progress([lsn]) => {
        //         // The lsn passed to `START_REPLICATION_SLOT` produces all transactions that
        //         // committed at LSNs *strictly after*, but upper frontiers have "greater than
        //         // or equal" semantics, so we must subtract one from the upper to make it
        //         // compatible with what `START_REPLICATION_SLOT` expects.
        //         task_info.row_sender.close_lsn(lsn).await;
        //     }
        // }
        // replication_lsn = PgLsn::from(event.wal_end() - 1);
        dbg!(event.wal_end(), event);
    }

    println!("Hello, world!");
    Ok(())
}

/// Information about an ingested upstream table
struct SourceTable {
    /// The source output index of this table
    output_index: usize,
    /// The relational description of this table
    desc: PostgresTableDesc,
    // /// The scalar expressions required to cast the text encoded columns received from postgres
    // /// into the target relational types
    casts: Vec<MirScalarExpr>,
}

// https://github.com/MaterializeInc/materialize/blob/main/src/storage/src/source/postgres.rs#L941
/// Casts a text row into the target types
fn cast_row(table_cast: &[MirScalarExpr], datums: &[Datum<'_>]) -> Result<Row, anyhow::Error> {
    let arena = mz_repr::RowArena::new();
    let mut row = Row::default();
    let mut packer = row.packer();
    for column_cast in table_cast {
        let datum = column_cast.eval(datums, &arena)?;
        packer.push(datum);
    }
    Ok(row)
}

//https://github.com/MaterializeInc/materialize/blob/main/src/storage/src/source/postgres.rs#L855
/// Produces the initial snapshot of the data by performing a `COPY` query for each of the provided
/// `source_tables`.
///
/// The return stream of data returned is not annotated with LSN numbers. It is up to the caller to
/// provide a client that is in a known LSN context in which the snapshot will be taken. For
/// example by calling this method while being in a transaction for which the LSN is known.
fn produce_snapshot<'a>(
    client: &'a Client,
    source_tables: &'a BTreeMap<u32, SourceTable>,
) -> impl Stream<Item = Result<(usize, Row), ReplicationError>> + 'a {
    async_stream::try_stream! {
        // Scratch space to use while evaluating casts
        let mut datum_vec = DatumVec::new();

        for info in source_tables.values() {
            let reader = client
                .copy_out_simple(
                    format!(
                        "COPY {:?}.{:?} TO STDOUT (FORMAT TEXT, DELIMITER '\t')",
                        info.desc.namespace, info.desc.name
                    )
                    .as_str(),
                )
                .await?;

            tokio::pin!(reader);
            let mut text_row = Row::default();
            // TODO: once tokio-stream is released with https://github.com/tokio-rs/tokio/pull/4502
            //    we can convert this into a single `timeout(...)` call on the reader CopyOutStream
            while let Some(b) = tokio::time::timeout(Duration::from_secs(30), reader.next())
                .await?
                .transpose()?
            {
                let mut packer = text_row.packer();
                // Convert raw rows from COPY into repr:Row. Each Row is a relation_id
                // and list of string-encoded values, e.g. Row{ 16391 , ["1", "2"] }
                let parser = mz_pgcopy::CopyTextFormatParser::new(b.as_ref(), "\t", "\\N");

                let mut raw_values = parser.iter_raw(info.desc.columns.len());
                while let Some(raw_value) = raw_values.next() {
                    match raw_value? {
                        Some(value) => {
                            packer.push(Datum::String(std::str::from_utf8(value)?))
                        }
                        None => packer.push(Datum::Null),
                    }
                }

                let mut datums = datum_vec.borrow();
                datums.extend(text_row.iter());

                let row = cast_row(&info.casts, &datums)?;


                yield (info.output_index, row);
            }

        }
    }
}

// // https://github.com/MaterializeInc/materialize/blob/main/src/storage/src/source/postgres.rs#L917
// /// Packs a Tuple received in the replication stream into a Row packer.
// fn datums_from_tuple<'a, T>(
//     rel_id: u32,
//     tuple_data: T,
//     datums: &mut Vec<Datum<'a>>,
// ) -> Result<(), anyhow::Error>
// where
//     T: IntoIterator<Item = &'a TupleData>,
// {
//     for val in tuple_data.into_iter() {
//         let datum = match val {
//             TupleData::Null => Datum::Null,
//             TupleData::UnchangedToast => bail!(
//                 "Missing TOASTed value from table with OID = {}. \
//                 Did you forget to set REPLICA IDENTITY to FULL for your table?",
//                 rel_id
//             ),
//             TupleData::Text(b) => std::str::from_utf8(b)?.into(),
//         };
//         datums.push(datum);
//     }
//     Ok(())
// }

// https://github.com/MaterializeInc/materialize/blob/main/src/storage/src/source/postgres.rs#L60
/// Postgres epoch is 2000-01-01T00:00:00Z
static PG_EPOCH: Lazy<SystemTime> = Lazy::new(|| UNIX_EPOCH + Duration::from_secs(946_684_800));

/// How often a status update message should be sent to the server
static FEEDBACK_INTERVAL: Duration = Duration::from_secs(30);

/// The amount of time we should wait after the last received message before worrying about WAL lag
static WAL_LAG_GRACE_PERIOD: Duration = Duration::from_secs(30);

// https://github.com/MaterializeInc/materialize/blob/main/src/storage/src/source/postgres.rs#L956
async fn produce_replication<'a>(
    client_config: mz_postgres_util::Config,
    slot: &'a str,
    publication: &'a str,
    as_of: PgLsn,
    committed_lsn: Arc<AtomicU64>,
) -> impl Stream<Item = Result<XLogDataBody<LogicalReplicationMessage>, ReplicationError>> + 'a {
    async_stream::try_stream!({
        // //let mut last_data_message = Instant::now();
        // let mut inserts = vec![];
        // let mut deletes = vec![];

        let mut last_feedback = Instant::now();

        // Scratch space to use while evaluating casts
        // let mut datum_vec = DatumVec::new();

        let mut last_commit_lsn = as_of;
        // let mut observed_wal_end = as_of;
        // The outer loop alternates the client between streaming the replication slot and using
        // normal SQL queries with pg admin functions to fast-foward our cursor in the event of WAL
        // lag.
        //
        // TODO(petrosagg): we need to do the above because a replication slot can be active only
        // one place which is why we need to do this dance of entering and exiting replication mode
        // in order to be able to use the administrative functions below. Perhaps it's worth
        // creating two independent slots so that we can use the secondary to check without
        // interrupting the stream on the first one
        loop {
            let client = client_config.clone().connect_replication().await?;
            let query = format!(
                r#"START_REPLICATION SLOT "{name}" LOGICAL {lsn}
                  ("proto_version" '1', "publication_names" '{publication}')"#,
                name = &slot,
                lsn = last_commit_lsn,
                publication = publication
            );
            let copy_stream = client.copy_both_simple(&query).await?;
            let mut stream = Box::pin(LogicalReplicationStream::new(copy_stream));

            let mut last_data_message = Instant::now();

            // The inner loop
            loop {
                // The upstream will periodically request status updates by setting the keepalive's
                // reply field to 1. However, we cannot rely on these messages arriving on time. For
                // example, when the upstream is sending a big transaction its keepalive messages are
                // queued and can be delayed arbitrarily. Therefore, we also make sure to
                // send a proactive status update every 30 seconds There is an implicit requirement
                // that a new resumption frontier is converted into an lsn relatively soon after
                // startup.
                //
                // See: https://www.postgresql.org/message-id/CAMsr+YE2dSfHVr7iEv1GSPZihitWX-PMkD9QALEGcTYa+sdsgg@mail.gmail.com
                let mut needs_status_update = last_feedback.elapsed() > FEEDBACK_INTERVAL;

                match stream.as_mut().next().await {
                    Some(Ok(ReplicationMessage::XLogData(xlog_data))) => {
                        last_data_message = Instant::now();
                        match xlog_data.data() {
                            LogicalReplicationMessage::Commit(commit) => {
                                // metrics.transactions.inc();
                                last_commit_lsn = PgLsn::from(commit.end_lsn());

                                // for (output, row) in deletes.drain(..) {
                                //     yield Event::Message(last_commit_lsn, (output, row, -1));
                                // }
                                // for (output, row) in inserts.drain(..) {
                                //     yield Event::Message(last_commit_lsn, (output, row, 1));
                                // }
                                // yield Event::Progress([PgLsn::from(u64::from(last_commit_lsn) + 1)]);
                                // metrics.lsn.set(last_commit_lsn.into());
                            }
                            _ => yield xlog_data,
                        }
                    }
                    // Some(Ok(XLogData(xlog_data))) => match xlog_data.data() {
                    //     Begin(_) => {
                    //         last_data_message = Instant::now();
                    //         if !inserts.is_empty() || !deletes.is_empty() {
                    //             return Err(Definite(anyhow!(
                    //                 "got BEGIN statement after uncommitted data"
                    //             )))?;
                    //         }
                    //     }
                    //     Insert(insert) if source_tables.contains_key(&insert.rel_id()) => {
                    //         last_data_message = Instant::now();
                    //         metrics.inserts.inc();
                    //         let rel_id = insert.rel_id();
                    //         let info = source_tables.get(&rel_id).unwrap();
                    //         let new_tuple = insert.tuple().tuple_data();
                    //         let mut datums = datum_vec.borrow();
                    //         datums_from_tuple(rel_id, new_tuple, &mut *datums).err_definite()?;
                    //         let row = cast_row(&info.casts, &datums).err_definite()?;
                    //         inserts.push((info.output_index, row));
                    //     }
                    //     Update(update) if source_tables.contains_key(&update.rel_id()) => {
                    //         last_data_message = Instant::now();
                    //         metrics.updates.inc();
                    //         let rel_id = update.rel_id();
                    //         let info = source_tables.get(&rel_id).unwrap();
                    //         let err = || {
                    //             anyhow!(
                    //                 "Old row missing from replication stream for table with OID = {}.
                    //                  Did you forget to set REPLICA IDENTITY to FULL for your table?",
                    //                 rel_id
                    //             )
                    //         };
                    //         let old_tuple = update
                    //             .old_tuple()
                    //             .ok_or_else(err)
                    //             .err_definite()?
                    //             .tuple_data();

                    //         let mut old_datums = datum_vec.borrow();
                    //         datums_from_tuple(rel_id, old_tuple, &mut *old_datums)
                    //             .err_definite()?;
                    //         let old_row = cast_row(&info.casts, &old_datums).err_definite()?;
                    //         deletes.push((info.output_index, old_row));
                    //         drop(old_datums);

                    //         // If the new tuple contains unchanged toast values, reuse the ones
                    //         // from the old tuple
                    //         let new_tuple = update
                    //             .new_tuple()
                    //             .tuple_data()
                    //             .iter()
                    //             .zip(old_tuple.iter())
                    //             .map(|(new, old)| match new {
                    //                 TupleData::UnchangedToast => old,
                    //                 _ => new,
                    //             });
                    //         let mut new_datums = datum_vec.borrow();
                    //         datums_from_tuple(rel_id, new_tuple, &mut *new_datums)
                    //             .err_definite()?;
                    //         let new_row = cast_row(&info.casts, &new_datums).err_definite()?;
                    //         inserts.push((info.output_index, new_row));
                    //     }
                    //     Delete(delete) if source_tables.contains_key(&delete.rel_id()) => {
                    //         last_data_message = Instant::now();
                    //         metrics.deletes.inc();
                    //         let rel_id = delete.rel_id();
                    //         let info = source_tables.get(&rel_id).unwrap();
                    //         let err = || {
                    //             anyhow!(
                    //                 "Old row missing from replication stream for table with OID = {}.
                    //                  Did you forget to set REPLICA IDENTITY to FULL for your table?",
                    //                 rel_id
                    //             )
                    //         };
                    //         let old_tuple = delete
                    //             .old_tuple()
                    //             .ok_or_else(err)
                    //             .err_definite()?
                    //             .tuple_data();
                    //         let mut datums = datum_vec.borrow();
                    //         datums_from_tuple(rel_id, old_tuple, &mut *datums).err_definite()?;
                    //         let row = cast_row(&info.casts, &datums).err_definite()?;
                    //         deletes.push((info.output_index, row));
                    //     }
                    //     Commit(commit) => {
                    //         last_data_message = Instant::now();
                    //         metrics.transactions.inc();
                    //         last_commit_lsn = PgLsn::from(commit.end_lsn());

                    //         for (output, row) in deletes.drain(..) {
                    //             yield Event::Message(last_commit_lsn, (output, row, -1));
                    //         }
                    //         for (output, row) in inserts.drain(..) {
                    //             yield Event::Message(last_commit_lsn, (output, row, 1));
                    //         }
                    //         yield Event::Progress([PgLsn::from(u64::from(last_commit_lsn) + 1)]);
                    //         metrics.lsn.set(last_commit_lsn.into());
                    //     }
                    //     Relation(relation) => {
                    //         last_data_message = Instant::now();
                    //         let rel_id = relation.rel_id();
                    //         let mut valid_schema_change = true;
                    //         if let Some(info) = source_tables.get(&rel_id) {
                    //             // Start with the cheapest check first, this will catch the majority of alters
                    //             if info.desc.columns.len() != relation.columns().len() {
                    //                 warn!(
                    //                     "alter table detected on {} with id {}",
                    //                     info.desc.name, info.desc.oid
                    //                 );
                    //                 valid_schema_change = false;
                    //             }
                    //             let same_name = info.desc.name == relation.name().unwrap();
                    //             let same_namespace =
                    //                 info.desc.namespace == relation.namespace().unwrap();
                    //             if !same_name || !same_namespace {
                    //                 warn!(
                    //                     "table name changed on {}.{} with id {} to {}.{}",
                    //                     info.desc.namespace,
                    //                     info.desc.name,
                    //                     info.desc.oid,
                    //                     relation.namespace().unwrap(),
                    //                     relation.name().unwrap()
                    //                 );
                    //                 valid_schema_change = false;
                    //             }

                    //             // Relation messages do not include nullability/primary_key data so we
                    //             // check the name, type_oid, and type_mod explicitly and error if any
                    //             // of them differ
                    //             for (src, rel) in info.desc.columns.iter().zip(relation.columns()) {
                    //                 let same_name = src.name == rel.name().unwrap();
                    //                 let rel_typoid = u32::try_from(rel.type_id()).unwrap();
                    //                 let same_typoid = src.type_oid == rel_typoid;
                    //                 let same_typmod = src.type_mod == rel.type_modifier();

                    //                 if !same_name || !same_typoid || !same_typmod {
                    //                     warn!(
                    //                         "alter table error: name {}, oid {}, old_schema {:?}, new_schema {:?}",
                    //                         info.desc.name,
                    //                         info.desc.oid,
                    //                         info.desc.columns,
                    //                         relation.columns()
                    //                     );

                    //                     valid_schema_change = false;
                    //                 }
                    //             }

                    //             if valid_schema_change {
                    //                 // Because the replication stream doesn't
                    //                 // include columns' attnums, we need to check
                    //                 // the current local schema against the current
                    //                 // remote schema to ensure e.g. we haven't
                    //                 // received a schema update with the same
                    //                 // terminal column name which is actually a
                    //                 // different column.
                    //                 let current_publication_info =
                    //                     mz_postgres_util::publication_info(
                    //                         &client_config,
                    //                         publication,
                    //                         Some(rel_id),
                    //                     )
                    //                     .await
                    //                     .err_indefinite()?;

                    //                 let remote_schema_eq =
                    //                     Some(&info.desc) == current_publication_info.get(0);
                    //                 if !remote_schema_eq {
                    //                     warn!(
                    //                     "alter table error: name {}, oid {}, current local schema {:?}, current remote schema {:?}",
                    //                     info.desc.name,
                    //                     info.desc.oid,
                    //                     info.desc.columns,
                    //                     current_publication_info.get(0)
                    //                 );

                    //                     valid_schema_change = false;
                    //                 }
                    //             }

                    //             if !valid_schema_change {
                    //                 return Err(Definite(anyhow!(
                    //                     "source table {} with oid {} has been altered",
                    //                     info.desc.name,
                    //                     info.desc.oid
                    //                 )))?;
                    //             } else {
                    //                 warn!(
                    //                     "source table {} with oid {} has been altered, but we could not determine how",
                    //                     info.desc.name,
                    //                     info.desc.oid
                    //                 )
                    //             }
                    //         }
                    //     }
                    //     Insert(_) | Update(_) | Delete(_) | Origin(_) | Type(_) => {
                    //         last_data_message = Instant::now();
                    //         metrics.ignored.inc();
                    //     }
                    //     Truncate(truncate) => {
                    //         let tables = truncate
                    //             .rel_ids()
                    //             .iter()
                    //             // Filter here makes option handling in map "safe"
                    //             .filter_map(|id| source_tables.get(id))
                    //             .map(|info| {
                    //                 format!("name: {} id: {}", info.desc.name, info.desc.oid)
                    //             })
                    //             .collect::<Vec<String>>();
                    //         return Err(Definite(anyhow!(
                    //             "source table(s) {} got truncated",
                    //             tables.join(", ")
                    //         )))?;
                    //     }
                    //     // The enum is marked as non_exhaustive. Better to be conservative here in
                    //     // case a new message is relevant to the semantics of our source
                    //     _ => {
                    //         return Err(Definite(anyhow!(
                    //             "unexpected logical replication message"
                    //         )))?;
                    //     }
                    // },
                    Some(Ok(ReplicationMessage::PrimaryKeepAlive(keepalive))) => {
                        needs_status_update = needs_status_update || keepalive.reply() == 1;
                        // observed_wal_end = PgLsn::from(keepalive.wal_end());

                        if last_data_message.elapsed() > WAL_LAG_GRACE_PERIOD {
                            break;
                        }
                    }
                    Some(Err(err)) => {
                        return Err(ReplicationError::from(err))?;
                    }
                    None => {
                        dbg!("eof");
                        break;
                    }
                    // The enum is marked non_exhaustive, better be conservative
                    _ => {
                        return Err(anyhow!("Unexpected replication message"))?;
                    }
                }
                if needs_status_update {
                    let ts: i64 = PG_EPOCH
                        .elapsed()
                        .expect("system clock set earlier than year 2000!")
                        .as_micros()
                        .try_into()
                        .expect("software more than 200k years old, consider updating");

                    let committed_lsn = PgLsn::from(committed_lsn.load(Ordering::SeqCst));
                    stream
                        .as_mut()
                        .standby_status_update(committed_lsn, committed_lsn, committed_lsn, ts, 0)
                        .await?;
                    last_feedback = Instant::now();
                }
            }
            // This may not be required, but as mentioned above in
            // `postgres_replication_loop_inner`, we drop clients aggressively out of caution.
            drop(stream);

            let client = client_config.clone().connect_replication().await?;

            // We reach this place if the consume loop above detected large WAL lag. This
            // section determines whether or not we can skip over that part of the WAL by
            // peeking into the replication slot using a normal SQL query and the
            // `pg_logical_slot_peek_binary_changes` administrative function.
            //
            // By doing so we can get a positive statement about existence or absence of
            // relevant data from the current LSN to the observed WAL end. If there are no
            // messages then it is safe to fast forward last_commit_lsn to the WAL end LSN and restart
            // the replication stream from there.
            let query = format!(
                "SELECT lsn FROM pg_logical_slot_peek_binary_changes(
                     '{name}', NULL, NULL,
                     'proto_version', '1',
                     'publication_names', '{publication}'
                )",
                name = &slot,
                publication = publication
            );

            // let peek_binary_start_time = Instant::now();
            let rows = client.simple_query(&query).await?;

            let changes = rows
                .into_iter()
                .filter(|row| match row {
                    SimpleQueryMessage::Row(row) => {
                        let change_lsn: PgLsn = row
                            .get("lsn")
                            .expect("missing expected column: `lsn`")
                            .parse()
                            .expect("invalid lsn");
                        // Keep all the changes that may exist after our last observed transaction
                        // commit
                        change_lsn > last_commit_lsn
                    }
                    SimpleQueryMessage::CommandComplete(_) => false,
                    _ => panic!("unexpected enum variant"),
                })
                .count();

            dbg!(changes);

            // If there are no changes until the end of the WAL it's safe to fast forward
            // if changes == 0 {
            //     last_commit_lsn = observed_wal_end;
            //     // `Progress` events are _frontiers_, so we add 1, just like when we
            //     // handle data in `Commit` above.
            //     yield Event::Progress([PgLsn::from(u64::from(last_commit_lsn) + 1)]);
            // }
        }
    })
}