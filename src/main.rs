use async_recursion::async_recursion;
use dotenv::dotenv;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tokio_postgres::{Error, NoTls};

#[derive(Debug, Deserialize, Serialize, ToSql, FromSql)]
enum CountryCode {
    AD,
    AE,
    AF,
    AG,
    AI,
    AL,
    AM,
    AO,
    AR,
    AS,
    AT,
    AQ,
    AU,
    AW,
    AZ,
    BA,
    BB,
    BD,
    BE,
    BF,
    BG,
    BH,
    IO,
    CM,
    CV,
    ET,
    FK,
    YT,
    CO,
    MM,
    BV,
    BI,
    BJ,
    BM,
    PF,
    BN,
    BO,
    BQ,
    BR,
    BS,
    BT,
    BW,
    BY,
    TF,
    BZ,
    GI,
    HM,
    HU,
    IS,
    CA,
    KE,
    CC,
    KG,
    LB,
    MU,
    CD,
    MA,
    CF,
    NR,
    CG,
    NZ,
    NU,
    NF,
    CH,
    MP,
    SE,
    PA,
    PY,
    GW,
    BL,
    TR,
    TH,
    GB,
    JE,
    XK,
    EH,
    RE,
    SZ,
    CI,
    CK,
    CL,
    CN,
    CR,
    CU,
    CW,
    CX,
    CY,
    CZ,
    DE,
    DJ,
    DK,
    DM,
    DO,
    DZ,
    EC,
    EE,
    EG,
    ER,
    ES,
    FI,
    FJ,
    FM,
    FO,
    FR,
    GA,
    GD,
    GE,
    GF,
    GG,
    GH,
    GL,
    GM,
    GN,
    GP,
    GQ,
    GR,
    GS,
    GT,
    GU,
    GY,
    HK,
    HN,
    HR,
    HT,
    ID,
    IE,
    IL,
    IM,
    IN,
    IQ,
    IT,
    JM,
    JO,
    JP,
    KH,
    KI,
    KM,
    KN,
    KP,
    KR,
    KW,
    KY,
    KZ,
    LA,
    LC,
    LI,
    LK,
    LR,
    LS,
    LT,
    LU,
    LV,
    LY,
    MC,
    MD,
    ME,
    MF,
    MG,
    MH,
    MK,
    ML,
    MN,
    MO,
    MQ,
    MR,
    MS,
    MT,
    MV,
    MW,
    MX,
    MY,
    MZ,
    NA,
    NC,
    NE,
    NG,
    NI,
    NL,
    NO,
    NP,
    OM,
    PE,
    PG,
    PH,
    PK,
    PL,
    PM,
    PN,
    PR,
    PS,
    PT,
    PW,
    QA,
    RO,
    RS,
    RU,
    RW,
    SA,
    SB,
    SC,
    SD,
    SG,
    SH,
    SI,
    SJ,
    SK,
    SL,
    SM,
    SN,
    SO,
    SR,
    SS,
    ST,
    SV,
    SX,
    SY,
    TC,
    TD,
    TG,
    TJ,
    TK,
    TL,
    TM,
    TN,
    TO,
    TT,
    TV,
    TW,
    TZ,
    UA,
    UG,
    UM,
    US,
    UY,
    UZ,
    VA,
    VC,
    VE,
    VG,
    VI,
    VN,
    VU,
    WF,
    WS,
    YE,
    ZA,
    ZM,
    ZW,
}
#[derive(Debug, Deserialize, Serialize, ToSql, FromSql)]
enum TargetType {
    COUNTRY,
    REGION,
    CITY,
    NEIGHBORHOOD,
    #[serde(rename = "POSTAL_CODE")]
    POSTAL_CODE,
    AIRPORT,
    UNIVERSITY,
    DEPARTMENT,
    MUNICIPALITY,
    PROVINCE,
    COUNTY,
    #[serde(rename = "NATIONAL_PARK")]
    NATIONAL_PARK,
    DISTRICT,
    #[serde(rename = "CONGRESSIONAL_DISTRICT")]
    CONGRESSIONAL_DISTRICT,
    STATE,
    #[serde(rename = "CITY_REGION")]
    CITY_REGION,
    GOVERNORATE,
    CANTON,
    TERRITORY,
    PREFECTURE,
    #[serde[rename = "AUTONOMOUS_COMMUNITY"]]
    AUTONOMOUS_COMMUNITY,
    #[serde[rename = "UNION_TERRITORY"]]
    UNION_TERRITORY,
    #[serde[rename = "TV_REGION"]]
    TV_REGION,
    BOROUGH,
    OKRUG,
}

// Data interface for the json data
#[derive(Deserialize, Serialize, Debug)]
struct Location {
    #[serde(rename = "googleAudienceId")]
    google_audience_id: i32,
    #[serde(rename = "canonicalName")]
    canonical_name: String,
    #[serde(rename = "parentId")]
    parent_id: Option<i32>,
    #[serde(rename = "countryCode")]
    country_code: CountryCode,
    #[serde(rename = "targetType")]
    target_type: TargetType,
    #[serde(rename = "Status")]
    status: String,
    children: Vec<Location>,
}

#[async_recursion]
async fn insert_data(
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
    INSERT INTO \"Location\" (\"googleAudienceId\", \"title\", \"parentId\", \"countryCode\", \"targetType\", \"hasChildren\")
    VALUES ($1, $2, $3, $4, $5, $6)
";

    conn.execute(query, &params).await?;

    for child in &data.children {
        insert_data(conn, child, Some(data.google_audience_id)).await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    // Attempt to retrieve environment variables
    let database_name = match env::var("DATABASE_NAME") {
        Ok(val) => val,
        Err(_) => {
            eprintln!("Error: DATABASE_NAME environment variable not set");
            return; // Exit the program or handle the error as needed
        }
    };

    let database_user = match env::var("DATABASE_USER") {
        Ok(val) => val,
        Err(_) => {
            eprintln!("Error: DATABASE_USER environment variable not set");
            return; // Exit the program or handle the error as needed
        }
    };

    let database_password = match env::var("DATABASE_PASSWORD") {
        Ok(val) => val,
        Err(_) => {
            eprintln!("Error: DATABASE_PASSWORD environment variable not set");
            return; // Exit the program or handle the error as needed
        }
    };

    // Construct the database URL
    let database_url = format!(
        "postgres://{}:{}@localhost:5432/{}",
        database_user, database_password, database_name
    );

    let (client, connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .expect("Error connecting to the database");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let file_path = Path::new("./src/locations.json");

    println!("file path: {:?}", file_path);
    let mut file = File::open(file_path).expect("Error opening file");
    let mut json_data = String::new();
    file.read_to_string(&mut json_data)
        .expect("Error reading file");

    let data: Vec<Location> = serde_json::from_str(&json_data).expect("Error parsing JSON");
    for el in &data {
        println!("passing here");
        if let Err(e) = insert_data(&client, &el, None).await {
            eprintln!("Error inserting data: {}", e);
        }
    }
}
