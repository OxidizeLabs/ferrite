use ferrite::common::db_instance::{DBConfig, DBInstance};
use ferrite::common::result_writer::CliResultWriter;
use ferrite::concurrency::transaction::IsolationLevel;

use crate::common::logger::init_test_logger;
use crate::common::tempdb::temp_db_config;

#[cfg_attr(miri, ignore)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn db_instance_recovery_integration() {
    init_test_logger();
    let cfg: DBConfig = temp_db_config();

    {
        let db = DBInstance::new(cfg.clone()).await.unwrap();
        let mut writer = CliResultWriter::new();
        db.execute_sql(
            "CREATE TABLE test (id INTEGER, value VARCHAR);",
            IsolationLevel::ReadCommitted,
            &mut writer,
        )
        .await
        .unwrap();
        db.execute_sql(
            "INSERT INTO test VALUES (0, 'abc');",
            IsolationLevel::ReadCommitted,
            &mut writer,
        )
        .await
        .unwrap();
    }

    let db = DBInstance::new(cfg.clone()).await.unwrap();
    assert!(db.get_recovery_manager().is_some());

    let mut writer = CliResultWriter::new();
    let result = db
        .execute_sql(
            "SELECT * FROM test;",
            IsolationLevel::ReadCommitted,
            &mut writer,
        )
        .await;
    assert!(result.is_ok());
}
