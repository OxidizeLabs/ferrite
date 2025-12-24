use crate::common::logger::init_test_logger;
use crate::common::tempdb::temp_db_config;
use ferrite::common::db_instance::{DBConfig, DBInstance};
use ferrite::common::result_writer::CliResultWriter;
use ferrite::concurrency::transaction::IsolationLevel;

#[tokio::test]
async fn recovery_smoke_reopen_triggers_recovery() {
    init_test_logger();
    let cfg: DBConfig = temp_db_config();

    {
        let db = DBInstance::new(cfg.clone()).await.unwrap();
        let mut writer = CliResultWriter::new();
        db.execute_sql(
            "CREATE TABLE r (id INTEGER);",
            IsolationLevel::ReadCommitted,
            &mut writer,
        )
        .await
        .unwrap();
        db.execute_sql(
            "INSERT INTO r VALUES (1);",
            IsolationLevel::ReadCommitted,
            &mut writer,
        )
        .await
        .unwrap();
    }

    let db = DBInstance::new(cfg.clone()).await.unwrap();
    assert!(db.get_recovery_manager().is_some());
}
