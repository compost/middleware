use std::time::Duration;
use std::io;
use std::sync::Arc;
use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand, ValueEnum};
use regex::Regex;
use reqwest::{Client, header};
use serde::{Deserialize, Serialize};
use azure_identity::DefaultAzureCredential;
use azure_core::credentials::TokenCredential;
use colored::*;

// Constants
const AZURE_MANAGEMENT_SCOPE: &str = "https://management.azure.com/.default";
const AZURE_MANAGEMENT_URL: &str = "https://management.azure.com";
const AZURE_API_VERSION: &str = "2023-05-01";
const HTTP_TIMEOUT_SECS: u64 = 30;
const HEALTH_ENDPOINT_PATH: &str = "/q/health";
const RESOURCE_GROUP_ID_SEGMENT: usize = 4;

#[derive(Clone, Debug, ValueEnum)]
enum OutputFormat {
    Tsv,
    Csv,
    Json,
}

#[derive(Parser)]
#[command(name = "aca-health-checker")]
#[command(about = "Azure Container Apps health checker and revision manager")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[arg(long, value_enum, default_value_t = OutputFormat::Tsv, global = true, help = "Output format")]
    format: OutputFormat,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Check health status of all container apps")]
    Check {
        #[arg(long, help = "Include response payload in output")]
        include_payload: bool,
        #[arg(long, help = "Optional regex pattern to validate payload against")]
        payload_regex: Option<String>,
        #[arg(long, default_value_t = true, help = "Enable colored output for regex matches")]
        color_regex: bool,
        #[arg(long, default_value_t = true, help = "Enable colored output for status indicators")]
        color_status: bool,
    },
    #[command(about = "Activate a specific revision")]
    Activate {
        #[arg(help = "Container app name")]
        app_name: String,
        #[arg(help = "Resource group name")]
        resource_group: String,
        #[arg(help = "Revision name")]
        revision_name: String,
    },
    #[command(about = "Deactivate a specific revision")]
    Deactivate {
        #[arg(help = "Container app name")]
        app_name: String,
        #[arg(help = "Resource group name")]
        resource_group: String,
        #[arg(help = "Revision name")]
        revision_name: String,
    },
    #[command(about = "List all revisions for a container app")]
    ListRevisions {
        #[arg(help = "Container app name")]
        app_name: String,
        #[arg(help = "Resource group name")]
        resource_group: String,
    },
    #[command(about = "Activate latest revision for all apps in a resource group")]
    ActivateAll {
        #[arg(help = "Resource group name")]
        resource_group: String,
    },
    #[command(about = "Deactivate all revisions for all apps in a resource group")]
    DeactivateAll {
        #[arg(help = "Resource group name")]
        resource_group: String,
    },
    #[command(about = "Deactivate all revisions for a specific container app")]
    DeactivateAppRevisions {
        #[arg(help = "Container app name")]
        app_name: String,
        #[arg(help = "Resource group name")]
        resource_group: String,
    },
    #[command(about = "List all applications with their revisions for a resource group")]
    ListAppsWithRevisions {
        #[arg(help = "Resource group name")]
        resource_group: String,
    },
}

#[derive(Debug, Deserialize, Serialize)]
struct ContainerApp {
    name: String,
    #[serde(rename = "resourceGroup")]
    resource_group: String,
    fqdn: Option<String>,
    #[serde(rename = "runningState")]
    running_state: String,
}

#[derive(Debug, Serialize)]
struct HealthCheckResult {
    app_name: String,
    resource_group: String,
    running_state: String,
    fqdn: String,
    health_status: String,
    http_code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Revision {
    name: String,
    active: bool,
    #[serde(rename = "trafficWeight")]
    traffic_weight: Option<i32>,
    #[serde(rename = "createdTime")]
    created_time: String,
}

#[derive(Debug, Serialize)]
struct RevisionResult {
    app_name: String,
    resource_group: String,
    revision_name: String,
    active: bool,
    traffic_weight: Option<i32>,
    created_time: String,
}

#[derive(Debug, Serialize)]
struct ActionResult {
    action: String,
    app_name: String,
    resource_group: String,
    revision_name: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AzureResponse<T> {
    value: Vec<T>,
    #[serde(rename = "nextLink")]
    next_link: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AzureContainerApp {
    id: String,
    name: String,
    properties: ContainerAppProperties,
}

#[derive(Debug, Deserialize)]
struct ContainerAppProperties {
    #[serde(rename = "runningStatus")]
    running_status: Option<String>,
    configuration: Option<ConfigurationProperties>,
}

#[derive(Debug, Deserialize)]
struct ConfigurationProperties {
    ingress: Option<IngressProperties>,
}

#[derive(Debug, Deserialize)]
struct IngressProperties {
    fqdn: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AzureRevision {
    name: String,
    properties: RevisionProperties,
}

#[derive(Debug, Deserialize)]
struct RevisionProperties {
    active: bool,
    #[serde(rename = "trafficWeight")]
    traffic_weight: Option<i32>,
    #[serde(rename = "createdTime")]
    created_time: String,
}

struct AzureClient {
    client: Client,
    subscription_id: String,
    credential: Arc<DefaultAzureCredential>,
}

impl AzureClient {
    async fn new(subscription_id: String) -> Result<Self> {
        let credential = DefaultAzureCredential::new()
            .map_err(|e| anyhow!("Failed to create Azure credential: {}", e))?;
        let client = Client::builder()
            .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
            .build()
            .map_err(|e| anyhow!("Failed to create HTTP client: {}", e))?;

        Ok(AzureClient {
            client,
            subscription_id,
            credential,
        })
    }

    async fn get_access_token(&self) -> Result<String> {
        let token = self.credential.get_token(&[AZURE_MANAGEMENT_SCOPE], None).await
            .map_err(|e| anyhow!("Failed to get access token: {}", e))?;
        Ok(token.token.secret().to_string())
    }

    // Helper to make authenticated POST requests
    async fn post_with_auth(&self, url: &str) -> Result<()> {
        let token = self.get_access_token().await?;
        let response = self.client
            .post(url)
            .header(header::AUTHORIZATION, format!("Bearer {}", token))
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, "0")
            .send()
            .await
            .map_err(|e| anyhow!("Request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Request failed: HTTP {} - {}", status, error_text));
        }

        Ok(())
    }

    async fn get_paginated_results<T>(&self, initial_url: &str) -> Result<Vec<T>>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        let token = self.get_access_token().await?;
        let mut all_results = Vec::new();
        let mut url = initial_url.to_string();

        loop {
            let response = self.client
                .get(&url)
                .header(header::AUTHORIZATION, format!("Bearer {}", token))
                .header(header::CONTENT_TYPE, "application/json")
                .send()
                .await
                .map_err(|e| anyhow!("Failed to fetch data from {}: {}", url, e))?;

            if !response.status().is_success() {
                let status = response.status();
                let error_text = response.text().await.unwrap_or_default();
                return Err(anyhow!("Failed to get paginated results from {}: HTTP {} - {}", url, status, error_text));
            }

            let azure_response: AzureResponse<T> = response.json().await
                .map_err(|e| anyhow!("Failed to parse response from {}: {}", url, e))?;
            all_results.extend(azure_response.value);

            match azure_response.next_link {
                Some(next_url) => url = next_url,
                None => break,
            }
        }

        Ok(all_results)
    }

    async fn get_container_apps(&self) -> Result<Vec<ContainerApp>> {
        let url = format!(
            "{}/subscriptions/{}/providers/Microsoft.App/containerApps?api-version={}",
            AZURE_MANAGEMENT_URL, self.subscription_id, AZURE_API_VERSION
        );

        let azure_apps: Vec<AzureContainerApp> = self.get_paginated_results(&url).await?;

        let apps = azure_apps.into_iter().map(|azure_app| {
            let resource_group = extract_resource_group_from_id(&azure_app.id);
            let fqdn = azure_app.properties.configuration
                .and_then(|config| config.ingress)
                .and_then(|ingress| ingress.fqdn);
            let running_state = azure_app.properties.running_status.unwrap_or_default();

            ContainerApp {
                name: azure_app.name,
                resource_group,
                fqdn,
                running_state,
            }
        }).collect();

        Ok(apps)
    }

    async fn get_container_apps_in_resource_group(&self, resource_group: &str) -> Result<Vec<ContainerApp>> {
        let url = format!(
            "{}/subscriptions/{}/resourceGroups/{}/providers/Microsoft.App/containerApps?api-version={}",
            AZURE_MANAGEMENT_URL, self.subscription_id, resource_group, AZURE_API_VERSION
        );

        let azure_apps: Vec<AzureContainerApp> = self.get_paginated_results(&url).await?;

        let apps = azure_apps.into_iter().map(|azure_app| {
            let fqdn = azure_app.properties.configuration
                .and_then(|config| config.ingress)
                .and_then(|ingress| ingress.fqdn);
            let running_state = azure_app.properties.running_status.unwrap_or_default();

            ContainerApp {
                name: azure_app.name,
                resource_group: resource_group.to_string(),
                fqdn,
                running_state,
            }
        }).collect();

        Ok(apps)
    }

    async fn get_revisions(&self, app_name: &str, resource_group: &str) -> Result<Vec<Revision>> {
        let url = format!(
            "{}/subscriptions/{}/resourceGroups/{}/providers/Microsoft.App/containerApps/{}/revisions?api-version={}",
            AZURE_MANAGEMENT_URL, self.subscription_id, resource_group, app_name, AZURE_API_VERSION
        );

        let azure_revisions: Vec<AzureRevision> = self.get_paginated_results(&url).await?;

        let revisions = azure_revisions.into_iter().map(|azure_revision| {
            Revision {
                name: azure_revision.name,
                active: azure_revision.properties.active,
                traffic_weight: azure_revision.properties.traffic_weight,
                created_time: azure_revision.properties.created_time,
            }
        }).collect();

        Ok(revisions)
    }

    async fn activate_revision(&self, app_name: &str, resource_group: &str, revision_name: &str) -> Result<()> {
        let url = format!(
            "{}/subscriptions/{}/resourceGroups/{}/providers/Microsoft.App/containerApps/{}/revisions/{}/activate?api-version={}",
            AZURE_MANAGEMENT_URL, self.subscription_id, resource_group, app_name, revision_name, AZURE_API_VERSION
        );
        self.post_with_auth(&url).await
            .map_err(|e| anyhow!("Failed to activate revision {}: {}", revision_name, e))
    }

    async fn deactivate_revision(&self, app_name: &str, resource_group: &str, revision_name: &str) -> Result<()> {
        let url = format!(
            "{}/subscriptions/{}/resourceGroups/{}/providers/Microsoft.App/containerApps/{}/revisions/{}/deactivate?api-version={}",
            AZURE_MANAGEMENT_URL, self.subscription_id, resource_group, app_name, revision_name, AZURE_API_VERSION
        );
        self.post_with_auth(&url).await
            .map_err(|e| anyhow!("Failed to deactivate revision {}: {}", revision_name, e))
    }
}

fn output_results<T: Serialize>(results: &[T], format: &OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(results)?);
        }
        OutputFormat::Csv => {
            let mut wtr = csv::WriterBuilder::new()
                .delimiter(b',')
                .from_writer(io::stdout());
            for record in results {
                wtr.serialize(record)?;
            }
            wtr.flush()?;
        }
        OutputFormat::Tsv => {
            let mut wtr = csv::WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(io::stdout());
            for record in results {
                wtr.serialize(record)?;
            }
            wtr.flush()?;
        }
    }
    Ok(())
}

fn extract_resource_group_from_id(resource_id: &str) -> String {
    resource_id
        .split('/')
        .nth(RESOURCE_GROUP_ID_SEGMENT)
        .unwrap_or_default()
        .to_string()
}

async fn get_subscription_id() -> Result<String> {
    if let Ok(sub_id) = std::env::var("AZURE_SUBSCRIPTION_ID") {
        return Ok(sub_id);
    }

    // Try to get from Azure CLI config as fallback
    let output = std::process::Command::new("az")
        .args(["account", "show", "--query", "id", "--output", "tsv"])
        .output()
        .map_err(|e| anyhow!("Failed to execute Azure CLI: {}", e))?;

    if output.status.success() {
        String::from_utf8(output.stdout)
            .map(|s| s.trim().to_string())
            .map_err(|e| anyhow!("Failed to parse Azure CLI output: {}", e))
    } else {
        Err(anyhow!("Could not determine subscription ID. Please set AZURE_SUBSCRIPTION_ID environment variable or ensure Azure CLI is configured."))
    }
}

async fn check_health_endpoint(fqdn: &str) -> Result<(u16, String)> {
    let client = Client::builder()
        .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
        .build()?;

    let url = format!("https://{}{}", fqdn, HEALTH_ENDPOINT_PATH);

    match client.get(&url).send().await {
        Ok(response) => {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            Ok((status, body))
        }
        Err(_) => Ok((0, String::new())),
    }
}

// Helper function to apply color formatting conditionally
fn format_status<F>(text: &str, color_fn: F, should_color: bool, is_tsv: bool) -> String
where
    F: Fn(&str) -> ColoredString,
{
    if should_color && is_tsv {
        color_fn(text).to_string()
    } else {
        text.to_string()
    }
}

// Helper function to sanitize payload text for output
fn sanitize_payload(text: &str) -> String {
    text.replace('\t', " ").replace('\n', " ").replace('\r', " ")
}

async fn check_container_apps_health(include_payload: bool, payload_regex: Option<String>, color_regex: bool, color_status: bool, format: &OutputFormat) -> Result<()> {
    let subscription_id = get_subscription_id().await?;
    let azure_client = AzureClient::new(subscription_id).await?;
    let apps = azure_client.get_container_apps().await?;

    // Compile regex if provided
    let regex = payload_regex
        .map(|pattern| Regex::new(&pattern).map_err(|e| anyhow!("Invalid regex pattern: {}", e)))
        .transpose()?;

    let mut results = Vec::new();
    let is_tsv = matches!(format, OutputFormat::Tsv);

    for app in apps {
        let fqdn = app.fqdn.as_deref().unwrap_or("null");

        if app.running_state.contains("Running") {
            if let Some(app_fqdn) = &app.fqdn {
                match check_health_endpoint(app_fqdn).await {
                    Ok((200, body)) => {
                        // Validate payload against regex if provided
                        let regex_match = regex.as_ref().map_or(true, |r| r.is_match(&body));

                        let health_status = if regex_match {
                            format_status("OK", |s: &str| s.green(), color_regex, is_tsv)
                        } else {
                            format_status("REGEX_MISMATCH", |s: &str| s.red(), color_regex, is_tsv)
                        };

                        let status_code = format_status("200", |s: &str| s.green(), color_status, is_tsv);
                        let payload = include_payload.then(|| sanitize_payload(&body));

                        results.push(HealthCheckResult {
                            app_name: app.name.clone(),
                            resource_group: app.resource_group.clone(),
                            running_state: app.running_state.clone(),
                            fqdn: fqdn.to_string(),
                            health_status,
                            http_code: status_code,
                            payload,
                        });
                    }
                    Ok((0, body)) => {
                        let health_status = format_status("CONNECTION_FAILED", |s: &str| s.red(), color_status, is_tsv);
                        let status_code = format_status("0", |s: &str| s.red(), color_status, is_tsv);
                        let payload = include_payload.then(|| sanitize_payload(&body));

                        results.push(HealthCheckResult {
                            app_name: app.name.clone(),
                            resource_group: app.resource_group.clone(),
                            running_state: app.running_state.clone(),
                            fqdn: fqdn.to_string(),
                            health_status,
                            http_code: status_code,
                            payload,
                        });
                    }
                    Ok((code, body)) => {
                        let health_status = format_status("HTTP_ERROR", |s: &str| s.red(), color_status, is_tsv);
                        let status_code = format_status(&code.to_string(), |s: &str| s.red(), color_status, is_tsv);
                        let payload = include_payload.then(|| sanitize_payload(&body));

                        results.push(HealthCheckResult {
                            app_name: app.name.clone(),
                            resource_group: app.resource_group.clone(),
                            running_state: app.running_state.clone(),
                            fqdn: fqdn.to_string(),
                            health_status,
                            http_code: status_code,
                            payload,
                        });
                    }
                    Err(_) => {
                        let health_status = format_status("ERROR", |s: &str| s.red(), color_status, is_tsv);
                        let status_code = format_status("0", |s: &str| s.red(), color_status, is_tsv);

                        results.push(HealthCheckResult {
                            app_name: app.name.clone(),
                            resource_group: app.resource_group.clone(),
                            running_state: app.running_state.clone(),
                            fqdn: fqdn.to_string(),
                            health_status,
                            http_code: status_code,
                            payload: None,
                        });
                    }
                }
            } else {
                let health_status = format_status("NO_ENDPOINT", |s: &str| s.yellow(), color_status, is_tsv);
                let status_code = format_status("0", |s: &str| s.yellow(), color_status, is_tsv);

                results.push(HealthCheckResult {
                    app_name: app.name.clone(),
                    resource_group: app.resource_group.clone(),
                    running_state: app.running_state.clone(),
                    fqdn: fqdn.to_string(),
                    health_status,
                    http_code: status_code,
                    payload: None,
                });
            }
        } else {
            let health_status = format_status("NOT_RUNNING", |s: &str| s.yellow(), color_status, is_tsv);
            let status_code = format_status("0", |s: &str| s.yellow(), color_status, is_tsv);

            results.push(HealthCheckResult {
                app_name: app.name.clone(),
                resource_group: app.resource_group.clone(),
                running_state: app.running_state.clone(),
                fqdn: fqdn.to_string(),
                health_status,
                http_code: status_code,
                payload: None,
            });
        }
    }

    output_results(&results, format)?;

    Ok(())
}

async fn activate_revision_cmd(app_name: &str, resource_group: &str, revision_name: &str, format: &OutputFormat) -> Result<()> {
    let subscription_id = get_subscription_id().await?;
    let azure_client = AzureClient::new(subscription_id).await?;
    azure_client.activate_revision(app_name, resource_group, revision_name).await?;

    let results = vec![ActionResult {
        action: "activate".to_string(),
        app_name: app_name.to_string(),
        resource_group: resource_group.to_string(),
        revision_name: revision_name.to_string(),
        status: "SUCCESS".to_string(),
        error: None,
    }];

    output_results(&results, format)?;
    Ok(())
}

async fn deactivate_revision_cmd(app_name: &str, resource_group: &str, revision_name: &str, format: &OutputFormat) -> Result<()> {
    let subscription_id = get_subscription_id().await?;
    let azure_client = AzureClient::new(subscription_id).await?;
    azure_client.deactivate_revision(app_name, resource_group, revision_name).await?;

    let results = vec![ActionResult {
        action: "deactivate".to_string(),
        app_name: app_name.to_string(),
        resource_group: resource_group.to_string(),
        revision_name: revision_name.to_string(),
        status: "SUCCESS".to_string(),
        error: None,
    }];

    output_results(&results, format)?;
    Ok(())
}

async fn list_revisions(app_name: &str, resource_group: &str, format: &OutputFormat) -> Result<()> {
    let subscription_id = get_subscription_id().await?;
    let azure_client = AzureClient::new(subscription_id).await?;
    let revisions = azure_client.get_revisions(app_name, resource_group).await?;

    let results: Vec<RevisionResult> = revisions.into_iter().map(|revision| {
        RevisionResult {
            app_name: app_name.to_string(),
            resource_group: resource_group.to_string(),
            revision_name: revision.name,
            active: revision.active,
            traffic_weight: revision.traffic_weight,
            created_time: revision.created_time,
        }
    }).collect();

    output_results(&results, format)?;
    Ok(())
}

async fn get_latest_revision(azure_client: &AzureClient, app_name: &str, resource_group: &str) -> Result<Option<String>> {
    let revisions = azure_client.get_revisions(app_name, resource_group).await?;
    
    if revisions.is_empty() {
        return Ok(None);
    }

    let latest = revisions
        .iter()
        .max_by(|a, b| a.created_time.cmp(&b.created_time));

    Ok(latest.map(|r| r.name.clone()))
}

async fn activate_all_apps(resource_group: &str, format: &OutputFormat) -> Result<()> {
    let subscription_id = get_subscription_id().await?;
    let azure_client = AzureClient::new(subscription_id).await?;
    let apps = azure_client.get_container_apps_in_resource_group(resource_group).await?;

    let mut results = Vec::new();

    for app in apps {
        match get_latest_revision(&azure_client, &app.name, &app.resource_group).await {
            Ok(Some(revision_name)) => {
                match azure_client.activate_revision(&app.name, &app.resource_group, &revision_name).await {
                    Ok(_) => {
                        results.push(ActionResult {
                            action: "activate".to_string(),
                            app_name: app.name,
                            resource_group: app.resource_group,
                            revision_name,
                            status: "SUCCESS".to_string(),
                            error: None,
                        });
                    }
                    Err(e) => {
                        results.push(ActionResult {
                            action: "activate".to_string(),
                            app_name: app.name,
                            resource_group: app.resource_group,
                            revision_name,
                            status: "FAILED".to_string(),
                            error: Some(e.to_string()),
                        });
                    }
                }
            }
            Ok(None) => {
                results.push(ActionResult {
                    action: "activate".to_string(),
                    app_name: app.name,
                    resource_group: app.resource_group,
                    revision_name: String::new(),
                    status: "FAILED".to_string(),
                    error: Some("No revisions found".to_string()),
                });
            }
            Err(e) => {
                results.push(ActionResult {
                    action: "activate".to_string(),
                    app_name: app.name,
                    resource_group: app.resource_group,
                    revision_name: String::new(),
                    status: "FAILED".to_string(),
                    error: Some(format!("Failed to get revisions: {}", e)),
                });
            }
        }
    }

    output_results(&results, format)?;
    Ok(())
}

async fn deactivate_all_apps(resource_group: &str, format: &OutputFormat) -> Result<()> {
    let subscription_id = get_subscription_id().await?;
    let azure_client = AzureClient::new(subscription_id).await?;
    let apps = azure_client.get_container_apps_in_resource_group(resource_group).await?;

    let mut results = Vec::new();

    for app in apps {
        match azure_client.get_revisions(&app.name, &app.resource_group).await {
            Ok(revisions) => {
                let active_revisions: Vec<_> = revisions.iter().filter(|r| r.active).collect();

                if active_revisions.is_empty() {
                    results.push(ActionResult {
                        action: "deactivate".to_string(),
                        app_name: app.name,
                        resource_group: app.resource_group,
                        revision_name: String::new(),
                        status: "SKIPPED".to_string(),
                        error: Some("No active revisions".to_string()),
                    });
                    continue;
                }

                for revision in active_revisions {
                    match azure_client.deactivate_revision(&app.name, &app.resource_group, &revision.name).await {
                        Ok(_) => {
                            results.push(ActionResult {
                                action: "deactivate".to_string(),
                                app_name: app.name.clone(),
                                resource_group: app.resource_group.clone(),
                                revision_name: revision.name.clone(),
                                status: "SUCCESS".to_string(),
                                error: None,
                            });
                        }
                        Err(e) => {
                            results.push(ActionResult {
                                action: "deactivate".to_string(),
                                app_name: app.name.clone(),
                                resource_group: app.resource_group.clone(),
                                revision_name: revision.name.clone(),
                                status: "FAILED".to_string(),
                                error: Some(e.to_string()),
                            });
                        }
                    }
                }
            }
            Err(e) => {
                results.push(ActionResult {
                    action: "deactivate".to_string(),
                    app_name: app.name,
                    resource_group: app.resource_group,
                    revision_name: String::new(),
                    status: "FAILED".to_string(),
                    error: Some(format!("Failed to get revisions: {}", e)),
                });
            }
        }
    }

    output_results(&results, format)?;
    Ok(())
}

async fn deactivate_app_revisions(app_name: &str, resource_group: &str, format: &OutputFormat) -> Result<()> {
    let subscription_id = get_subscription_id().await?;
    let azure_client = AzureClient::new(subscription_id).await?;

    let mut results = Vec::new();

    match azure_client.get_revisions(app_name, resource_group).await {
        Ok(revisions) => {
            let active_revisions: Vec<_> = revisions.iter().filter(|r| r.active).collect();

            if active_revisions.is_empty() {
                results.push(ActionResult {
                    action: "deactivate".to_string(),
                    app_name: app_name.to_string(),
                    resource_group: resource_group.to_string(),
                    revision_name: String::new(),
                    status: "SKIPPED".to_string(),
                    error: Some("No active revisions".to_string()),
                });
            } else {
                for revision in active_revisions {
                    match azure_client.deactivate_revision(app_name, resource_group, &revision.name).await {
                        Ok(_) => {
                            results.push(ActionResult {
                                action: "deactivate".to_string(),
                                app_name: app_name.to_string(),
                                resource_group: resource_group.to_string(),
                                revision_name: revision.name.clone(),
                                status: "SUCCESS".to_string(),
                                error: None,
                            });
                        }
                        Err(e) => {
                            results.push(ActionResult {
                                action: "deactivate".to_string(),
                                app_name: app_name.to_string(),
                                resource_group: resource_group.to_string(),
                                revision_name: revision.name.clone(),
                                status: "FAILED".to_string(),
                                error: Some(e.to_string()),
                            });
                        }
                    }
                }
            }
        }
        Err(e) => {
            results.push(ActionResult {
                action: "deactivate".to_string(),
                app_name: app_name.to_string(),
                resource_group: resource_group.to_string(),
                revision_name: String::new(),
                status: "FAILED".to_string(),
                error: Some(format!("Failed to get revisions: {}", e)),
            });
        }
    }

    output_results(&results, format)?;
    Ok(())
}

async fn list_apps_with_revisions(resource_group: &str, format: &OutputFormat) -> Result<()> {
    let subscription_id = get_subscription_id().await?;
    let azure_client = AzureClient::new(subscription_id).await?;
    let apps = azure_client.get_container_apps_in_resource_group(resource_group).await?;

    let mut results = Vec::new();

    for app in apps {
        match azure_client.get_revisions(&app.name, &app.resource_group).await {
            Ok(revisions) => {
                if revisions.is_empty() {
                    results.push(RevisionResult {
                        app_name: app.name,
                        resource_group: app.resource_group,
                        revision_name: String::new(),
                        active: false,
                        traffic_weight: None,
                        created_time: String::new(),
                    });
                } else {
                    for revision in revisions {
                        results.push(RevisionResult {
                            app_name: app.name.clone(),
                            resource_group: app.resource_group.clone(),
                            revision_name: revision.name,
                            active: revision.active,
                            traffic_weight: revision.traffic_weight,
                            created_time: revision.created_time,
                        });
                    }
                }
            }
            Err(e) => {
                results.push(RevisionResult {
                    app_name: app.name,
                    resource_group: app.resource_group,
                    revision_name: format!("ERROR: Failed to get revisions: {}", e),
                    active: false,
                    traffic_weight: None,
                    created_time: String::new(),
                });
            }
        }
    }

    output_results(&results, format)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Check { include_payload, payload_regex, color_regex, color_status } => {
            check_container_apps_health(include_payload, payload_regex, color_regex, color_status, &cli.format).await?;
        }
        Commands::Activate { app_name, resource_group, revision_name } => {
            activate_revision_cmd(&app_name, &resource_group, &revision_name, &cli.format).await?;
        }
        Commands::Deactivate { app_name, resource_group, revision_name } => {
            deactivate_revision_cmd(&app_name, &resource_group, &revision_name, &cli.format).await?;
        }
        Commands::ListRevisions { app_name, resource_group } => {
            list_revisions(&app_name, &resource_group, &cli.format).await?;
        }
        Commands::ActivateAll { resource_group } => {
            activate_all_apps(&resource_group, &cli.format).await?;
        }
        Commands::DeactivateAll { resource_group } => {
            deactivate_all_apps(&resource_group, &cli.format).await?;
        }
        Commands::DeactivateAppRevisions { app_name, resource_group } => {
            deactivate_app_revisions(&app_name, &resource_group, &cli.format).await?;
        }
        Commands::ListAppsWithRevisions { resource_group } => {
            list_apps_with_revisions(&resource_group, &cli.format).await?;
        }
    }

    Ok(())
}
