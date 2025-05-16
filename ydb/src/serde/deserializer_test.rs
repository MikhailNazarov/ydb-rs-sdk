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

#[tokio::test]
#[ignore] // need YDB access
async fn test_deserialize_bool() -> YdbResult<()> {
    let db_value = simple_test::<bool>(
        r#"
            select true
        "#,
    )
    .await?;

    let test_value = true;
    assert_eq!(test_value, db_value);

    Ok(())
}

#[tokio::test]
#[ignore] // need YDB access
async fn test_deserialize_string() -> YdbResult<()> {
    let db_value = simple_test::<String>(
        r#"
            select 'test'
        "#,
    )
    .await?;

    let test_value = "test".to_string();
    assert_eq!(test_value, db_value);

    Ok(())
}

#[tokio::test]
#[ignore] // need YDB access
async fn test_deserialize_array() -> YdbResult<()> {
    let db_value = simple_test::<Vec<i32>>(
        r#"
            select [1, 2, 3]
        "#,
    )
    .await?;

    let test_value = vec![1, 2, 3];
    assert_eq!(test_value, db_value);

    Ok(())
}

#[tokio::test]
#[ignore] // need YDB access
async fn test_deserialize_optional_string_some() -> YdbResult<()> {
    let db_value = simple_test::<Option<String>>(
        r#"
            select CAST('test' AS Utf8?)
        "#,
    )
    .await?;

    let test_value = Some("test".to_string());
    assert_eq!(test_value, db_value);

    Ok(())
}

#[tokio::test]
#[ignore] // need YDB access
async fn test_deserialize_optional_string_none() -> YdbResult<()> {
    let db_value = simple_test::<Option<String>>(
        r#"
            select CAST(NULL AS Utf8?)
        "#,
    )
    .await?;

    let test_value = None;
    assert_eq!(test_value, db_value);

    Ok(())
}

#[tokio::test]
#[ignore] // need YDB access
async fn test_deserialize_optional_bool_some() -> YdbResult<()> {
    let db_value = simple_test::<Option<bool>>(
        r#"
            select CAST(true AS Bool?)
        "#,
    )
    .await?;

    let test_value = Some(true);
    assert_eq!(test_value, db_value);

    Ok(())
}

#[tokio::test]
#[ignore] // need YDB access
async fn test_deserialize_optional_bool_none() -> YdbResult<()> {
    let db_value = simple_test::<Option<bool>>(
        r#"
            select CAST(NULL AS Bool?)
        "#,
    )
    .await?;

    let test_value = None;
    assert_eq!(test_value, db_value);

    Ok(())
}

#[tokio::test]
#[ignore] // need YDB access
async fn test_deserialize_enum() -> YdbResult<()> {
    #[derive(Deserialize, Eq, PartialEq, Debug)]
    enum Enum {
        Variant1,
        Variant2,
        Variant3,
    }

    let db_value = simple_test::<Enum>(
        r#"
            select 'Variant3'
        "#,
    )
    .await?;

    let test_value = Enum::Variant3;
    assert_eq!(test_value, db_value);

    Ok(())
}

#[tokio::test]
#[ignore] // need YDB access
async fn test_deserialize_enum_i32() -> YdbResult<()> {
    #[derive(Deserialize, Eq, PartialEq, Debug)]
    #[repr(i32)]
    enum Enum {
        Variant1 = 0,
        Variant2 = 1,
    }

    let db_value = simple_test::<Enum>(
        r#"
            select 1
        "#,
    )
    .await?;

    let test_value = Enum::Variant2;
    assert_eq!(test_value, db_value);

    Ok(())
}
