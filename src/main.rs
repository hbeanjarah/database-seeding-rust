use std::fs::File;
use std::io::Read;
use std::path::Path;
use postgres_types::{ FromSql, ToSql };
use tokio_postgres::{ NoTls, Error };
use serde::{ Deserialize, Serialize };
use async_recursion::async_recursion;

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
    parent_id: Option<i32>
) -> Result<(), Error> {
    let params: &[&(dyn ToSql + Sync)] = &[
        &data.google_audience_id.to_string(),
        &data.canonical_name,
        &parent_id,
        &data.country_code,
        &data.target_type,
    ];

    let query =
        "
    INSERT INTO \"Location\" (\"googleAudienceId\", \"title\", \"parentId\", \"countryCode\", \"targetType\")
    VALUES ($1, $2, $3, $4, $5)
";

    conn.execute(query, &params).await?;

    for child in &data.children {
        insert_data(conn, child, Some(data.google_audience_id)).await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    let (client, connection) = tokio_postgres
        ::connect("postgres://postgres:postgres@localhost:5432/personas", NoTls).await
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
    file.read_to_string(&mut json_data).expect("Error reading file");

    let data: Vec<Location> = serde_json::from_str(&json_data).expect("Error parsing JSON");
    // println!("passing here: {:?}", &data);
    for el in &data {
        println!("passing here");
        if let Err(e) = insert_data(&client, &el, None).await {
            eprintln!("Error inserting data: {}", e);
        }
    }
}
