use std::collections::HashMap;
use std::env;

#[derive(Debug)]
struct Profile {
    user_id: i64,
    api_id: String,
    username: String,
}

#[derive(Debug)]
struct CodeSnippet {
    slug: String,
    language: String,
    title: String,
    public: bool,
    user_id: Option<i64>,
    created: chrono::DateTime<chrono::FixedOffset>,
    modified: chrono::DateTime<chrono::FixedOffset>,
}

#[derive(Debug)]
struct CodeFile {
    name: String,
    content: Vec<u8>,
}


fn main() {
    let psql_user = env::var("PSQL_USER").unwrap();
    let psql_pass = env::var("PSQL_PASS").unwrap();
    let couchdb_base_url = env::var("COUCHDB_BASE_URL").unwrap();

    let conn_str = format!("host=localhost user={} password={}", psql_user, psql_pass);
    let mut client = postgres::Client::connect(&conn_str, postgres::NoTls).unwrap();

    let profiles = client.query("SELECT user_id, snippets_api_id, username FROM profile", &[])
        .unwrap()
        .iter()
        .map(|row| {
            let profile = Profile{
                user_id: row.get(0),
                api_id: row.get(1),
                username: row.get(2),
            };

            (profile.api_id.clone(), profile)
        })
        .collect::<HashMap<String, Profile>>();


    process_loop(None, 0, profiles, client, &couchdb_base_url)
}

fn process_loop(start_key: Option<String>, rows_processed: usize, profiles: HashMap<String, Profile>, mut client: postgres::Client, couchdb_base_url: &str) {
    let documents = get_documents(couchdb_base_url, start_key, 1000);
    let documents_count = documents.rows.len();

    println!("Processed {} of {}", rows_processed, documents.total_rows);

    if documents_count > 0 {
        process_loop(process_rows(documents.rows, &profiles, &mut client), rows_processed + documents_count, profiles, client, couchdb_base_url);
    }
}

fn process_rows(rows: Vec<CouchRow>, profiles: &HashMap<String, Profile>, client: &mut postgres::Client) -> Option<String> {

    let insert_snippet: postgres::Statement = client.prepare("INSERT INTO code_snippet (slug, language, title, public, user_id, created, modified) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id").unwrap();
    let insert_file: postgres::Statement = client.prepare("INSERT INTO code_file (code_snippet_id, name, content) VALUES ($1, $2, $3) RETURNING id").unwrap();
    let mut transaction = client.transaction().unwrap();

    for row in &rows {
        let profile = profiles.get(&row.doc.owner);

        let snippet = CodeSnippet{
            slug: row.doc._id.clone(),
            language: normalize_language(&row.doc.language),
            title: row.doc.title.replace("\0", ""),
            public: row.doc.public,
            user_id: profile.map(|profile| profile.user_id),
            created: chrono::DateTime::parse_from_rfc3339(&row.doc.created).unwrap(),
            modified: chrono::DateTime::parse_from_rfc3339(&row.doc.modified).unwrap(),
        };

        let inserted_rows = transaction.query(&insert_snippet, &[
            &snippet.slug,
            &snippet.language,
            &snippet.title,
            &snippet.public,
            &snippet.user_id,
            &snippet.created,
            &snippet.modified,
        ]).unwrap();

        let snippet_id: i64 = inserted_rows.last().unwrap().get(0);

        for file in &row.doc.files {
            transaction.query(
                &insert_file,
                &[
                    &snippet_id,
                    &file.name.replace("\0", ""),
                    &file.content,
                ],
            ).unwrap();
        }

    }

    transaction.commit().unwrap();

    rows.last().map(|row| row.doc._id.clone())
}


fn get_documents(couchdb_base_url: &str, optional_start_key: Option<String>, limit: u64) -> CouchResponse {
    let url = format!("{}/snippets/_all_docs", couchdb_base_url);

    let response = match optional_start_key {
        Some(start_key) => {
            ureq::get(&url)
                .query("descending", "false")
                .query("limit", &limit.to_string())
                .query("startkey", &format!("\"{}\"", start_key))
                .query("startkey_docid", &start_key)
                .query("skip", "1") // Skip start_key
                .query("include_docs", "true")
                .call()
        }

        None => {
            ureq::get(&url)
                .query("descending", "false")
                .query("limit", &limit.to_string())
                .query("skip", "1") // Skip design document
                .query("include_docs", "true")
                .call()
        }
    };

    if !response.ok() {
        panic!("response not ok: {:?}", response);
    }

    response.into_json_deserialize().unwrap()
}


#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct CouchResponse {
    pub total_rows: u64,
    pub offset: u64,
    pub rows: Vec<CouchRow>
}


#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct CouchRow {
    pub doc: CouchDocument,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct CouchDocument {
    pub _id: String,
    pub created: String,
    pub modified: String,
    pub language: String,
    pub title: String,
    pub public: bool,
    pub owner: String,
    pub files: Vec<File>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct File {
    pub name: String,
    #[serde(with = "serde_bytes")]
    pub content: Vec<u8>,
}


fn normalize_language(input: &str) -> String {
    let language = input.to_ascii_lowercase();

    match language.as_str() {
        "assembly" => language.to_string(),
        "ats" => language.to_string(),
        "bash" => language.to_string(),
        "clojure" => language.to_string(),
        "cobol" => language.to_string(),
        "coffeescript" => language.to_string(),
        "cpp" => language.to_string(),
        "c" => language.to_string(),
        "crystal" => language.to_string(),
        "csharp" => language.to_string(),
        "d" => language.to_string(),
        "elixir" => language.to_string(),
        "elm" => language.to_string(),
        "erlang" => language.to_string(),
        "fsharp" => language.to_string(),
        "go" => language.to_string(),
        "groovy" => language.to_string(),
        "haskell" => language.to_string(),
        "idris" => language.to_string(),
        "javascript" => language.to_string(),
        "julia" => language.to_string(),
        "kotlin" => language.to_string(),
        "lua" => language.to_string(),
        "mercury" => language.to_string(),
        "nim" => language.to_string(),
        "ocaml" => language.to_string(),
        "java" => language.to_string(),
        "perl" => language.to_string(),
        "php" => language.to_string(),
        "python" => language.to_string(),
        "raku" => language.to_string(),
        "ruby" => language.to_string(),
        "rust" => language.to_string(),
        "scala" => language.to_string(),
        "swift" => language.to_string(),
        "typescript" => language.to_string(),
        "plaintext" => language.to_string(),
        "perl6" => "raku".to_string(),
        _ => {
            println!("Invalid language '{}', changing to 'plaintext'", language);
            "plaintext".to_string()
        }

    }
}



#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Language {
    Assembly,
    Ats,
    Bash,
    C,
    Clojure,
    Cobol,
    CoffeeScript,
    Cpp,
    Crystal,
    Csharp,
    D,
    Elixir,
    Elm,
    Erlang,
    Fsharp,
    Go,
    Groovy,
    Haskell,
    Idris,
    Java,
    JavaScript,
    Julia,
    Kotlin,
    Lua,
    Mercury,
    Nim,
    Ocaml,
    Perl,
    Php,
    Python,
    Raku,
    Ruby,
    Rust,
    Scala,
    Swift,
    TypeScript,
}
