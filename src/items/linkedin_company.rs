use tokio_postgres::Client;

use crate::common::ParentEntity;
use crate::common::LinkedInCoreItemType;


pub async fn insert(
    client: &Client,
    parent_id: Option<i32>,
    entity: &ParentEntity,
) -> Result<(), Box<dyn std::error::Error>> {
    let stmt = client
    .prepare(
        "INSERT INTO \"LinkedInCoreItems\" (\"name\", \"urn\", \"facetUrn\", \"parentId\", \"type\", \"entityTypes\")
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .await?;
    let company_type: LinkedInCoreItemType;
    company_type = LinkedInCoreItemType::COMPANY;
    let entity_types = entity.entity_types.first();

    for child in &entity.children {
        client
            .execute(
                &stmt,
                &[
                    &child.name,
                    &child.urn,
                    &child.urn,
                    &parent_id,
                    &company_type,
                    &entity_types,
                ],
            )
            .await?;
    }

    Ok(())
}
