use async_recursion::async_recursion;
use postgres_types::ToSql;
use tokio_postgres::Error;
use crate::common::Location;

#[async_recursion]
pub async fn insert_data(
    conn: &tokio_postgres::Client,
    data: &Location,
    parent_id: Option<i32>,
) -> Result<(), Error> {
    let mut has_children: bool = false;

    if &data.children.len() > &0 {
        has_children = true
    }
    let params: &[&(dyn ToSql + Sync)] = &[
        &data.google_audience_id.to_string(),
        &data.canonical_name,
        &parent_id,
        &data.country_code,
        &data.target_type,
        &has_children,
    ];

    let query =
        "
    INSERT INTO \"GoogleLocation\" (\"googleAudienceId\", \"title\", \"parentId\", \"countryCode\", \"targetType\", \"hasChildren\")
    VALUES ($1, $2, $3, $4, $5, $6)
";

    conn.execute(query, &params).await?;

    for child in &data.children {
        insert_data(conn, child, Some(data.google_audience_id)).await?;
    }

    Ok(())
}
