use crate::settings::*;
/**
 * Rules processing module
 *
 */
use async_std::{fs::File, io::BufReader, prelude::*, sync::Arc};
use log::*;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub async fn test_rules(file_name: &str, settings: Arc<Settings>) -> Result<()> {
    let file = File::open(file_name)
        .await
        .expect("Failed to open the file");
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut number: u64 = 0;

    while let Some(line) = lines.next().await {
        let line = line?;
        debug!("Testing the line: {}", line);
        number = number + 1;
        let mut matches: Vec<&str> = vec![];

        for rule in settings.rules.iter() {
            match rule.field {
                Field::Msg => {
                    if rule.jmespath.len() > 0 {
                        let expr = jmespath::compile(&rule.jmespath).unwrap();
                        if let Ok(data) = jmespath::Variable::from_json(&line) {
                            // Search the data with the compiled expression
                            if let Ok(result) = expr.search(data) {
                                if !result.is_null() {
                                    matches.push(&rule.jmespath);
                                }
                            }
                        }
                    } else if let Some(_captures) = rule.regex.captures(&line) {
                        matches.push(&rule.regex.as_str());
                    }
                }
                _ => {}
            }
        }

        if matches.len() > 0 {
            println!("Line {} matches on:", number);
            for m in matches.iter() {
                println!("\t - {}", m);
            }
        }
    }

    Ok(())
}
