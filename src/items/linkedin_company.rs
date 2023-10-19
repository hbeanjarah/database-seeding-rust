use tokio_postgres::Client;

use crate::common::LinkedInCoreItemType;
use crate::common::ParentEntity;

pub async fn insert(
    client: &Client,
    parent_id: Option<i32>,
    entity: &ParentEntity,
) -> Result<(), Box<dyn std::error::Error>> {
    let  is_child_has_children: bool = false;
    let mut is_parent_has_children: bool = false;

    if &entity.children.len() > &0 {
        is_parent_has_children = true
    }

    let stmt = client
    .prepare(
        "INSERT INTO \"LinkedInCoreItems\" (\"name\", \"urn\", \"facetUrn\", \"parentId\", \"type\", \"entityTypes\", \"hasChildren\")
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .await?;
    let company_type: LinkedInCoreItemType;
    company_type = LinkedInCoreItemType::INTERESTS;
    let entity_types = entity.entity_types.first();
    let null_parent_id: Option<i32> = None;
    // println!("children from each insertion  {:?}", &entity);

    // Insert Parent
    client
        .execute(
            &stmt,
            &[
                &entity.facet_name,
                &entity.facet_urn,
                &entity.facet_urn,
                &null_parent_id,
                &company_type,
                &entity_types,
            ],
        )
        .await?;

    if is_parent_has_children {
        let null_parent_id:Option<i32> = None;
        client
            .execute(
                &stmt,
                &[
                    &entity.facet_name,
                    &entity.facet_urn,
                    &entity.facet_urn,
                    &null_parent_id,
                    &company_type,
                    &entity_types,
                    &is_parent_has_children,
                ],
            )
            .await?;
    }

    for child in &entity.children {
        client
            .execute(
                &stmt,
                &[
                    &child.name,
                    &child.urn,
                    &child.facet_urn,
                    &parent_id,
                    &company_type,
                    &entity_types,
                    &is_child_has_children,
                ],
            )
            .await?;
    }

    Ok(())
}
