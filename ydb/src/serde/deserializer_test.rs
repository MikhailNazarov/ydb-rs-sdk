use serde::{de::DeserializeOwned, Deserialize};

use crate::{
    serde::deserializer::RowDeserializer, test_helpers::test_client_builder, Query, Row, YdbResult,
};
#[cfg(test)]
fn from_row(row: Row) -> RowDeserializer {
    RowDeserializer::from_row(row)
}

#[cfg(test)]
async fn simple_test<T: DeserializeOwned>(query: &str) -> YdbResult<T> {
    let client = test_client_builder().client()?;

    #[derive(Debug, PartialEq, Eq, Deserialize)]
    struct TestStruct {
        id: i32,
        name: String,
    }

    client.wait().await?;

    let db_value: T = client
        .table_client()
        .retry_transaction(|mut t| async move {
            let res = t.query(Query::from(query)).await?;

            Ok(T::deserialize(from_row(res.into_only_row()?))?)
        })
        .await?;

    Ok(db_value)
}

#[tokio::test]
#[ignore] // need YDB access
async fn test_deserialize_struct() -> YdbResult<()> {
    #[derive(Debug, PartialEq, Eq, Deserialize)]
    struct TestStruct {
        id: i32,
        name: String,
    }

    let db_value = simple_test::<TestStruct>(
        r#"
            select 99 as id, "TestUser" as name
        "#,
    )
    .await?;

    let test_value = TestStruct {
        id: 99,
        name: "TestUser".to_string(),
    };
    assert_eq!(test_value, db_value);

    Ok(())
}

#[tokio::test]
#[ignore] // need YDB access
async fn test_deserialize_i32() -> YdbResult<()> {
    let db_value = simple_test::<i32>(
        r#"
            select 99
        "#,
    )
    .await?;

    let test_value = 99;
    assert_eq!(test_value, db_value);

    Ok(())
}

#[tokio::test]
#[ignore] // need YDB access
async fn test_deserialize_i64() -> YdbResult<()> {
    let db_value = simple_test::<i64>(
        r#"
            select 99l
        "#,
    )
    .await?;

    let test_value = 99;
    assert_eq!(test_value, db_value);

    Ok(())
}

#[tokio::test]
#[ignore] // need YDB access
async fn test_deserialize_u32() -> YdbResult<()> {
    let db_value = simple_test::<u32>(
        r#"
            select 99u
        "#,
    )
    .await?;

    let test_value = 99;
    assert_eq!(test_value, db_value);

    Ok(())
}

#[tokio::test]
#[ignore] // need YDB access
async fn test_deserialize_u64() -> YdbResult<()> {
    let db_value = simple_test::<u64>(
        r#"
            select 99ul
        "#,
    )
    .await?;

    let test_value = 99;
    assert_eq!(test_value, db_value);

    Ok(())
}

#[tokio::test]
#[ignore] // need YDB access
async fn test_deserialize_f32() -> YdbResult<()> {
    let db_value = simple_test::<f32>(
        r#"
            select 99.9f
        "#,
    )
    .await?;

    let test_value = 99.9;
    assert_eq!(test_value, db_value);

    Ok(())
}

#[tokio::test]
#[ignore] // need YDB access
async fn test_deserialize_f64() -> YdbResult<()> {
    let db_value = simple_test::<f64>(
        r#"
            select 99.9
        "#,
    )
    .await?;

    let test_value = 99.9;
    assert_eq!(test_value, db_value);

    Ok(())
}
