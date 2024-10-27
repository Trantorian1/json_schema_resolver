#![feature(string_remove_matches)]

use std::collections::{HashMap, HashSet};
use std::{ffi, fs, io};

use anyhow::Context;
use clap::Parser;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;

#[derive(Parser)]
struct Cli {
    #[arg(short, long, required = true, num_args = 1..)]
    files: Vec<std::path::PathBuf>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct SchemaComponents {
    schemas: serde_json::Map<String, serde_json::Value>,
    // TODO:
    // errors: serde_json::Value::Object
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct SchemaFile {
    methods: Vec<serde_json::Value>,
    components: SchemaComponents,
}

// TODO: move error handling to eyre
fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    logger_init();

    // First, we open all JSON files. This short-circuits in case of any IO error
    let size_hint = Some(cli.files.len());
    let readers_and_paths = files_load(cli.files, size_hint)?;

    // Then, we parse all files to JSON, loading them into RAM
    let size_hint = Some(readers_and_paths.len());
    let mut json_in_and_files = json_load(readers_and_paths, size_hint)?;

    // With the files contents in memory, we can start grouping all components together. These are
    // stored along with the file they were declared in to support relative path dereferencing.
    let ref_map = ref_load(&json_in_and_files)?;

    // This is where the actual component de-referencing takes place, yielding a de-referenced map
    // of all components, stored with information on their file of origin.
    let deref_map = ref_resolved(ref_map)?;

    // Now, we can finally go through and de-reference the rest of the JSON Schema
    json_resolve(&mut json_in_and_files, &deref_map)?;

    // Once the de-referencing has taken place, we now need to merge each JSON Schema into one
    let _json_out = json_merge(json_in_and_files)?;

    anyhow::Ok(())
}

fn logger_init() {
    #[cfg(not(test))]
    let level = tracing::level_filters::LevelFilter::INFO;
    #[cfg(test)]
    let level = tracing::level_filters::LevelFilter::TRACE;
    let fmt_layer = tracing_subscriber::fmt::layer().with_test_writer().pretty().with_filter(level);
    let subscriber = tracing_subscriber::registry().with(fmt_layer);

    if tracing::subscriber::set_global_default(subscriber).is_err() {
        tracing::warn!("Attempted to set global subscriber again");
    }
}

#[tracing::instrument]
fn files_load<I>(iter: I, size_hint: Option<usize>) -> anyhow::Result<Vec<(io::BufReader<fs::File>, String)>>
where
    I: IntoIterator<Item = std::path::PathBuf> + std::fmt::Debug,
{
    tracing::info!("Loading files");

    iter.into_iter().try_fold(Vec::with_capacity(size_hint.unwrap_or_default()), |mut acc, path| {
        tracing::debug!("Opening file: {}", path.to_string_lossy());

        anyhow::ensure!(
            path.extension().map(ffi::OsStr::to_str) == Some(Some("json")),
            "{} is not a json file",
            path.to_string_lossy()
        );

        tracing::debug!("Opening file: {} - SUCCESS", path.to_string_lossy());
        let file = fs::File::open(&path).with_context(|| format!("Failed to open {}", path.to_string_lossy()))?;
        acc.push((io::BufReader::new(file), path.to_string_lossy().to_string()));

        anyhow::Ok(acc)
    })
}

#[tracing::instrument(skip(iter))]
fn json_load<I>(iter: I, size_hint: Option<usize>) -> anyhow::Result<Vec<(SchemaFile, String)>>
where
    I: IntoIterator<Item = (io::BufReader<fs::File>, String)> + std::fmt::Debug,
{
    tracing::info!("Loading file json");
    iter.into_iter()
        .try_fold(Vec::with_capacity(size_hint.unwrap_or_default()), |mut acc, (reader, path)| {
            tracing::debug!("Loading file json: {path}");
            let json = (serde_json::from_reader(reader).with_context(|| format!("Failed to read {path}")))?;
            tracing::debug!("Loading file json: {path} - SUCCESS");

            acc.push((json, path));
            anyhow::Ok(acc)
        })
        .context("Failed to read files to json")
}

#[tracing::instrument(skip(iter))]
fn ref_load<I>(iter: &I) -> anyhow::Result<HashMap<String, (String, serde_json::Value)>>
where
    I: ?Sized,
    for<'a> &'a I: IntoIterator<Item = &'a (SchemaFile, String)> + std::fmt::Debug + serde::Serialize,
{
    tracing::info!("Loading file references");
    tracing::trace!("File references are: {}", serde_json::to_string_pretty(&iter).unwrap_or_default());

    iter.into_iter().try_fold(HashMap::<String, (String, serde_json::Value)>::default(), |mut acc, (json, path)| {
        tracing::debug!("Loading references from file: {path}");

        for (key, value) in json.components.schemas.iter() {
            if !acc.contains_key(key) {
                let mut path_key = String::with_capacity(path.len() + key.len());
                path_key.push_str(path);
                path_key.push_str(key);

                tracing::debug!("Storing reference at key: {path_key}");
                acc.insert(path_key, (path.to_owned(), value.clone()));
            } else {
                anyhow::bail!(format!("Error parsing {path}: '{key}' component is a duplicate"));
            }
        }

        tracing::debug!("Loading references from file: {path} - SUCCESS");

        anyhow::Ok(acc)
    })
}

/// This is the heart of the program: it will recursively traverse a component, resolving any
/// sub-references down to a single component. Once this step is complete, we will have a fully
/// de-referenced map we can use to insert components into the rest of the schema
#[tracing::instrument(skip(val, ref_map))]
fn ref_resolve(
    local_file: &str,
    val: &serde_json::Value,
    ref_map: &HashMap<String, (String, serde_json::Value)>,
) -> anyhow::Result<serde_json::Value> {
    tracing::info!("Resolving reference");
    tracing::trace!("Reference is: {}", serde_json::to_string_pretty(val).unwrap_or_default());
    tracing::debug!("Asserting reference type");

    match val {
        serde_json::Value::Object(ref object) => {
            tracing::debug!("Asserting reference type - OBJECT");
            let object = object.into_iter().try_fold(serde_json::Map::default(), |mut acc, (key_outer, val)| {
                tracing::debug!("Checking for nested reference, key is {key_outer}");

                if key_outer == "$ref" {
                    let serde_json::Value::String(ref_name) = val else {
                        anyhow::bail!(
                            "Error parsing {local_file}: references must be strings, found: {}",
                            serde_json::to_string_pretty(&val).unwrap_or_default()
                        );
                    };

                    tracing::debug!("Found a nested reference: {ref_name}");
                    if let serde_json::Value::Object(deref_val) = ref_resolve(local_file, val, ref_map)? {
                        for (key_inner, val) in deref_val {
                            anyhow::ensure!(
                                acc.insert(key_inner.clone(), val.clone()).is_none(),
                                "Error parsing {local_file}: '{key_inner}' is overwritten multiple times in \
                                 {key_outer}"
                            );
                        }
                    } else {
                        anyhow::bail!(
                            "Invalid reference component {}, components must be a JSON object",
                            val.as_str().unwrap_or_default()
                        );
                    }
                } else if matches!(val, serde_json::Value::Object(_)) || matches!(val, serde_json::Value::Array(_)) {
                    acc.insert(key_outer.clone(), ref_resolve(local_file, val, ref_map)?);
                } else {
                    acc.insert(key_outer.clone(), val.clone());
                }

                anyhow::Ok(acc)
            })?;

            tracing::debug!("Resolving reference - SUCCESS");
            anyhow::Ok(serde_json::Value::Object(object))
        }
        serde_json::Value::Array(array) => {
            tracing::debug!("Asserting reference type - ARRAY");

            let array = array.iter().try_fold(Vec::with_capacity(array.len()), |mut acc, val| {
                let val = if matches!(val, serde_json::Value::Object(_)) {
                    tracing::debug!("Found a nested object in an array");
                    ref_resolve(local_file, val, ref_map)?
                } else {
                    val.clone()
                };
                acc.push(val);
                anyhow::Ok(acc)
            })?;

            anyhow::Ok(serde_json::Value::Array(array))
        }
        serde_json::Value::String(ref_path) => {
            tracing::debug!("Asserting reference type - NESTED REFERENCE");
            tracing::debug!("Extracting reference path");

            let key = extract_key(ref_path, local_file)?;

            tracing::debug!("Extracted reference key: {key}");

            let _span = tracing::debug_span!("Resolving nested reference", key).entered();
            tracing::trace!("Reference map is: {}", serde_json::to_string_pretty(&ref_map).unwrap_or_default());
            tracing::debug!("Looking for reference in reference map");
            let (ref_file, ref_val) = ref_map
                .get(&key)
                .with_context(|| format!("Error paring {local_file}: invalid reference {ref_path}"))?
                .clone();

            tracing::debug!("Resolving reference - SUCCESS");
            ref_resolve(&ref_file, &ref_val, ref_map)
        }
        _ => anyhow::Ok(val.clone()),
    }
}

#[tracing::instrument]
fn extract_key(ref_path: &str, local_file: &str) -> anyhow::Result<String> {
    tracing::debug!("Extracting key from reference {ref_path}");

    anyhow::ensure!(ref_path.len() > 20, "Error parsing {local_file}: invalid reference format {ref_path}");
    let (ref_file, ref_name) = ref_path
        .split_once("#")
        .map(|(l, r)| (l.trim_end_matches('/'), &r[20..]))
        .with_context(|| format!("Error parsing {local_file}: invalid reference format {ref_path}"))?;

    tracing::debug!("Extracting reference path - SUCCESS");
    tracing::debug!("Extracting reference key");

    let key = if ref_file.is_empty() {
        tracing::debug!("Reference is local");
        let mut key = String::with_capacity(local_file.len() + ref_name.len());
        key.push_str(local_file);
        key.push_str(ref_name);
        key
    } else {
        tracing::debug!("Reference was declared in a separate file");
        let mut key = String::with_capacity(ref_file.len() + ref_name.len());
        key.push_str(ref_file);
        key.push_str(ref_name);
        key
    };

    anyhow::Ok(key)
}

#[tracing::instrument(skip(ref_map))]
fn ref_resolved(
    ref_map: HashMap<String, (String, serde_json::Value)>,
) -> Result<serde_json::Map<String, serde_json::Value>, anyhow::Error> {
    tracing::info!("Resolving references");

    let acc = serde_json::Map::with_capacity(ref_map.len());

    ref_map.iter().try_fold(acc, |mut acc, (key, (local_file, val))| {
        let span = tracing::debug_span!("Resolving file", local_file, key).entered();
        acc.insert(key.clone(), ref_resolve(local_file, val, &ref_map)?);
        span.exit();

        anyhow::Ok(acc)
    })
}

#[tracing::instrument(skip(val, deref_map))]
fn ref_replace(
    local_file: &str,
    val: &serde_json::Value,
    deref_map: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<serde_json::Value> {
    tracing::info!("Resolving method reference in: {local_file}");
    tracing::trace!("Reference is: {}", serde_json::to_string_pretty(val).unwrap_or_default());
    tracing::debug!("Asserting reference type");

    match val {
        serde_json::Value::Object(object) => {
            tracing::debug!("Asserting reference type - OBJECT");
            let object =
                object.iter().try_fold(serde_json::Map::with_capacity(object.len()), |mut acc, (key_outer, val)| {
                    tracing::debug!("Checking for nested reference, key is {key_outer}");

                    if key_outer == "$ref" {
                        let serde_json::Value::String(ref_name) = val else {
                            anyhow::bail!(
                                "Error parsing {local_file}: references must be strings, found: {}",
                                serde_json::to_string_pretty(&val).unwrap_or_default()
                            );
                        };

                        tracing::debug!("Found a nested reference: {ref_name}");
                        if let serde_json::Value::Object(deref_val) = ref_replace(local_file, val, deref_map)? {
                            for (key_inner, val) in deref_val {
                                anyhow::ensure!(
                                    acc.insert(key_inner.clone(), val.clone()).is_none(),
                                    "Error parsing {local_file}: '{key_inner}' is overwritten multiple times in \
                                     {key_outer}"
                                );
                            }
                        } else {
                            anyhow::bail!(
                                "Invalid reference component {}, components must be a JSON object",
                                val.as_str().unwrap_or_default()
                            );
                        }
                    } else if matches!(val, serde_json::Value::Object(_)) || matches!(val, serde_json::Value::Array(_))
                    {
                        acc.insert(key_outer.clone(), ref_replace(local_file, val, deref_map)?);
                    } else {
                        acc.insert(key_outer.clone(), val.clone());
                    }
                    anyhow::Ok(acc)
                })?;

            tracing::trace!("Dereferenced object is {}", serde_json::to_string_pretty(&object).unwrap_or_default());

            anyhow::Ok(serde_json::Value::Object(object))
        }
        serde_json::Value::Array(array) => {
            tracing::debug!("Asserting reference type - ARRAY");

            let array = array.iter().try_fold(Vec::with_capacity(array.len()), |mut acc, val| {
                let val = if matches!(val, serde_json::Value::Object(_)) {
                    tracing::debug!("Found a nested object in an array");
                    ref_replace(local_file, val, deref_map)?
                } else {
                    val.clone()
                };
                acc.push(val);
                anyhow::Ok(acc)
            })?;

            anyhow::Ok(serde_json::Value::Array(array))
        }
        serde_json::Value::String(ref_path) => {
            tracing::debug!("Asserting reference type - NESTED REFERENCE");
            tracing::trace!("Reference map is: {}", serde_json::to_string_pretty(&deref_map).unwrap_or_default());

            let key = extract_key(ref_path, local_file)?;

            tracing::debug!("Extracted reference key: {key}");

            deref_map
                .get(&key)
                .cloned()
                .with_context(|| format!("Error parsing {local_file}: reference '{ref_path}' does not exists"))
        }
        _ => anyhow::Ok(val.clone()),
    }
}

#[tracing::instrument(skip(iter, deref_map))]
fn json_resolve(
    iter: &mut [(SchemaFile, String)],
    deref_map: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<()> {
    tracing::info!("Resolving json files");

    iter.iter_mut().try_fold((), |_, (json, local_file)| {
        tracing::debug!("Resolving json in: {local_file}");

        tracing::trace!("Defined methods are: {}", serde_json::to_string_pretty(&json.methods).unwrap_or_default());

        json.methods.iter_mut().try_fold((), |_, val| {
            *val = ref_replace(local_file, val, deref_map)?;
            anyhow::Ok(())
        })?;

        tracing::trace!(
            "Defined components are: {}",
            serde_json::to_string_pretty(&json.components.schemas).unwrap_or_default()
        );

        json.components.schemas.iter_mut().try_fold((), |_, (key, val)| {
            tracing::debug!("Resolving component: {key}");

            let mut key_new = String::with_capacity(local_file.len() + key.len());
            key_new.push_str(local_file);
            key_new.push_str(key);

            tracing::debug!("Retrieving at key: {key_new}");

            *val = deref_map
                .get(&key_new)
                .with_context(|| format!("Error parsing {local_file}: could not dereference {key}"))
                .cloned()?;

            anyhow::Ok(())
        })?;

        anyhow::Ok(())
    })
}

#[tracing::instrument(skip(iter))]
fn json_merge<I>(iter: I) -> anyhow::Result<SchemaFile>
where
    I: IntoIterator<Item = (SchemaFile, String)>,
{
    tracing::info!("Merging files");

    let mut method_set = HashSet::<String>::default();
    let mut schema_set = HashSet::<String>::default();

    iter.into_iter().try_fold(SchemaFile::default(), |mut acc, (json, local_file)| {
        tracing::debug!("Merging methods for {local_file}");

        tracing::trace!("File methods are: {}", serde_json::to_string_pretty(&json.methods).unwrap_or_default());

        for method in json.methods {
            let name = method
                .get("name")
                .with_context(|| format!("Error parsing {local_file}: methods should have a name"))?
                .as_str()
                .with_context(|| format!("Error parsing {local_file}: method name should be a string"))?;

            tracing::debug!("Merging method: {}", serde_json::to_string_pretty(&method).unwrap_or_default());

            anyhow::ensure!(
                method_set.insert(name.to_string()),
                "Error merging {local_file}: method '{name}' has already been declared in a previous file"
            );

            acc.methods.push(method.clone());
        }

        tracing::debug!("Merging schemas for {local_file}");

        tracing::trace!(
            "File schemas are: {}",
            serde_json::to_string_pretty(&json.components.schemas).unwrap_or_default()
        );

        for (name, schema) in json.components.schemas {
            anyhow::ensure!(
                schema_set.insert(name.clone()),
                "Error merging {local_file}: schema '{name}' has already been declared in a previous file"
            );

            acc.components.schemas.insert(name, schema);
        }

        anyhow::Ok(acc)
    })
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::SchemaFile;

    #[rstest::fixture]
    fn file_paths() -> Vec<std::path::PathBuf> {
        vec![std::path::PathBuf::from("./src/schema.json"), std::path::PathBuf::from("./src/schema_components.json")]
    }

    #[rstest::fixture]
    fn file_paths_deref() -> Vec<std::path::PathBuf> {
        vec![
            std::path::PathBuf::from("./src/schema_deref.json"),
            std::path::PathBuf::from("./src/schema_components.json"),
        ]
    }

    #[rstest::fixture]
    fn file_jsons(file_paths: Vec<std::path::PathBuf>) -> Vec<(SchemaFile, String)> {
        file_paths
            .into_iter()
            .map(|path| (std::fs::File::open(&path).unwrap(), path.to_string_lossy().to_string()))
            .map(|(file, path)| (std::io::BufReader::new(file), path))
            .map(|(reader, path)| (serde_json::from_reader(reader).unwrap(), path))
            .collect()
    }

    #[rstest::fixture]
    fn file_jsons_deref(file_paths_deref: Vec<std::path::PathBuf>) -> Vec<(SchemaFile, String)> {
        file_paths_deref
            .into_iter()
            .map(|path| (std::fs::File::open(&path).unwrap(), path.to_string_lossy().to_string()))
            .map(|(file, path)| (std::io::BufReader::new(file), path))
            .map(|(reader, path)| (serde_json::from_reader(reader).unwrap(), path))
            .map(|(json, mut path)| {
                path.remove_matches("_deref");
                (json, path)
            })
            .collect()
    }

    #[rstest::fixture]
    fn file_json_merged() -> SchemaFile {
        let path = std::path::PathBuf::from("./src/schema_merged.json");
        let file = std::fs::File::open(path).expect("Opening ./src/schema_merged.json");
        let reader = std::io::BufReader::new(file);
        serde_json::from_reader(reader).expect("Loading json from ./src/schema_merged.json")
    }

    #[rstest::rstest]
    fn files_and_json_load_valid(file_paths: Vec<std::path::PathBuf>, file_jsons: Vec<(SchemaFile, String)>) {
        crate::logger_init();

        let readers_and_paths = crate::files_load(file_paths, None).expect("Initializing file readers");
        let json_in_and_files = crate::json_load(readers_and_paths, None).expect("Loading json");

        assert_eq!(json_in_and_files, file_jsons);
    }

    #[rstest::rstest]
    fn ref_load_valid(file_paths: Vec<std::path::PathBuf>, file_jsons: Vec<(SchemaFile, String)>) {
        crate::logger_init();

        let readers_and_paths = crate::files_load(file_paths, None).expect("Initializing file readers");
        let json_in_and_files = crate::json_load(readers_and_paths, None).expect("Loading json");
        let ref_map = crate::ref_load(&json_in_and_files).expect("Resolving references");

        let expected = file_jsons.into_iter().fold(HashMap::with_capacity(ref_map.len()), |mut acc, (json, path)| {
            json.components.schemas.into_iter().for_each(|(key, val)| {
                acc.insert(format!("{path}{key}"), (path.clone(), val.clone()));
            });
            acc
        });

        let file = std::fs::File::create("./actual.json").unwrap();
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &ref_map).unwrap();

        let file = std::fs::File::create("./expected.json").unwrap();
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &expected).unwrap();

        assert_eq!(
            ref_map,
            expected,
            "actual: {}\nexpected: {}",
            serde_json::to_string_pretty(&ref_map).unwrap(),
            serde_json::to_string_pretty(&expected).unwrap()
        );
    }

    #[rstest::rstest]
    fn ref_resolve_valid(file_paths: Vec<std::path::PathBuf>, file_jsons_deref: Vec<(SchemaFile, String)>) {
        crate::logger_init();

        let readers_and_paths = crate::files_load(file_paths, None).expect("Initializing file readers");
        let json_in_and_files = crate::json_load(readers_and_paths, None).expect("Loading json");
        let ref_map = crate::ref_load(&json_in_and_files).unwrap();
        let deref_map = crate::ref_resolved(ref_map).expect("Resolving references");

        let expected = file_jsons_deref.into_iter().fold(serde_json::Map::default(), |mut acc, (json, path)| {
            json.components.schemas.into_iter().for_each(|(key, val)| {
                acc.insert(format!("{path}{key}"), val.clone());
            });
            acc
        });

        let file = std::fs::File::create("./actual.json").unwrap();
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &deref_map).unwrap();

        let file = std::fs::File::create("./expected.json").unwrap();
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &expected).unwrap();

        assert_eq!(
            deref_map,
            expected,
            "actual: {}\nexpected: {}",
            serde_json::to_string_pretty(&deref_map).unwrap(),
            serde_json::to_string_pretty(&expected).unwrap()
        )
    }

    #[rstest::rstest]
    // TODO: add error reference resolution
    fn json_resolve_valid(file_paths: Vec<std::path::PathBuf>, file_jsons_deref: Vec<(SchemaFile, String)>) {
        crate::logger_init();

        let readers_and_paths = crate::files_load(file_paths, None).expect("Initializing file readers");
        let mut json_in_and_files = crate::json_load(readers_and_paths, None).expect("Loading json");
        let ref_map = crate::ref_load(&json_in_and_files).unwrap();
        let deref_map = crate::ref_resolved(ref_map).expect("Resolving references");
        crate::json_resolve(&mut json_in_and_files, &deref_map).expect("Resolving json methods");

        let file = std::fs::File::create("./actual.json").unwrap();
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &json_in_and_files).unwrap();

        let file = std::fs::File::create("./expected.json").unwrap();
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &file_jsons_deref).unwrap();

        assert_eq!(
            json_in_and_files,
            file_jsons_deref,
            "actual: {}\nexpected: {}",
            serde_json::to_string_pretty(&json_in_and_files).unwrap(),
            serde_json::to_string_pretty(&file_jsons_deref).unwrap()
        )
    }

    #[rstest::rstest]
    fn json_merge_valid(file_paths: Vec<std::path::PathBuf>, file_json_merged: SchemaFile) {
        crate::logger_init();

        let readers_and_paths = crate::files_load(file_paths, None).expect("Initializing file readers");
        let mut json_in_and_files = crate::json_load(readers_and_paths, None).expect("Loading json");
        let ref_map = crate::ref_load(&json_in_and_files).unwrap();
        let deref_map = crate::ref_resolved(ref_map).expect("Resolving references");
        crate::json_resolve(&mut json_in_and_files, &deref_map).expect("Resolving json methods");
        let json_out = crate::json_merge(json_in_and_files).expect("Merging json files");

        let file = std::fs::File::create("./actual.json").unwrap();
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &json_out).unwrap();

        let file = std::fs::File::create("./expected.json").unwrap();
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &file_json_merged).unwrap();

        assert_eq!(
            json_out,
            file_json_merged,
            "actual: {}\nexpected: {}",
            serde_json::to_string_pretty(&json_out).unwrap(),
            serde_json::to_string_pretty(&file_json_merged).unwrap(),
        )
    }
}
