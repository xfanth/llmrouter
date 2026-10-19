//! The static pages: the API reference at `/docs`, the document it renders
//! at `/openapi.json`, and the dashboard at `/dashboard`.
//!
//! The pages are compiled into the binary rather than read from disk so the
//! container image needs no assets directory and the pages cannot go missing
//! at boot. Both pages load their libraries from a CDN, and every one of
//! those loads carries a sha384 integrity pin; the tests assert the pin is
//! present so a tampered file from the CDN cannot run.

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

/// The OpenAPI 3 document served at `/openapi.json` and rendered by `/docs`.
const OPENAPI_JSON: &str = include_str!("../static/openapi.json");
/// The Swagger UI shell that renders the document.
const DOCS_HTML: &str = include_str!("../static/docs.html");
/// The telemetry dashboard that renders `GET /metrics`.
const DASHBOARD_HTML: &str = include_str!("../static/dashboard.html");

/// `GET /docs`: the API reference, a Swagger UI shell over the document.
pub async fn docs_page() -> Response {
    html(DOCS_HTML)
}

/// `GET /openapi.json`: the document the reference page renders.
pub async fn openapi_json() -> Response {
    json_response(OPENAPI_JSON)
}

/// `GET /dashboard`: the telemetry page, which reads `GET /metrics`.
pub async fn dashboard_page() -> Response {
    html(DASHBOARD_HTML)
}

/// `GET /`: the dashboard is the front page, so send visitors there.
pub async fn index() -> Response {
    axum::response::Redirect::to("/dashboard").into_response()
}

/// `text/html` with an explicit charset, because browsers otherwise guess.
fn html(body: &'static str) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        body,
    )
        .into_response()
}

/// `application/json` for the OpenAPI document.
fn json_response(body: &'static str) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The response body as text, for assertions.
    async fn body(response: Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), 8 * 1024 * 1024)
            .await
            .expect("the body collects");
        String::from_utf8(bytes.to_vec()).expect("the pages are utf-8")
    }

    /// Every remote asset must pin its expected bytes with a sha384 hash,
    /// or a tampered file from the CDN would run in the visitor's browser.
    fn assert_pins(page: &str, name: &str) {
        for attribute in ["src=\"http", "href=\"http"] {
            let mut search_from = 0;
            while let Some(offset) = page[search_from..].find(attribute) {
                let tag_start = search_from + offset;
                let tag_end = tag_start
                    + page[tag_start..]
                        .find('>')
                        .unwrap_or_else(|| panic!("{name} has a tag with no closing >"));
                let tag = &page[tag_start..tag_end];
                assert!(
                    tag.contains("integrity=\"sha384-"),
                    "{name} loads a remote asset without a sha384 pin: {tag}"
                );
                search_from = tag_end;
            }
        }
    }

    #[tokio::test]
    async fn docs_page_serves_the_swagger_shell() {
        let response = docs_page().await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[&header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        let body = body(response).await;
        assert!(body.contains("id=\"swagger-ui\""));
        assert!(body.contains("url: '/openapi.json'"));
        assert_pins(&body, "docs.html");
    }

    #[tokio::test]
    async fn dashboard_page_serves_the_dashboard() {
        let response = dashboard_page().await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body(response).await;
        assert!(body.contains("id=\"tiles\""));
        assert!(body.contains("/metrics"));
        assert_pins(&body, "dashboard.html");
        // The hedge-wins chart reads the `copy` label the metrics write; the
        // first version read a label name that never existed, so the chart
        // sat empty however much hedged traffic ran.
        assert!(
            body.contains("byLabelMulti(\"router_hedge_wins_total\", \"provider\", \"copy\")"),
            "the hedge wins chart must read the copy label"
        );
        // The rolling charts read the backend ring, whose points carry each
        // window's own percentiles and the five-minute rolling rate.
        assert!(body.contains("/metrics/history"));
        assert!(body.contains("rate5m"));
        // The request log table reads its own endpoint.
        assert!(body.contains("/metrics/requests"));
        assert!(body.contains("id=\"reqtable\""));
        // The hedging cards explain themselves in plain words.
        assert!(body.contains("Per-channel hedging (M)"));
        assert!(body.contains("Router-wide race (N)"));
    }

    #[tokio::test]
    async fn index_redirects_to_the_dashboard() {
        let response = index().await;
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(response.headers()[&header::LOCATION], "/dashboard");
    }

    #[test]
    fn openapi_document_parses_and_names_the_endpoints() {
        let doc = serde_json::from_str::<serde_json::Value>(OPENAPI_JSON)
            .expect("the embedded document is strict JSON");
        assert_eq!(doc["openapi"], "3.0.3");
        let paths = &doc["paths"];
        for path in [
            "/",
            "/v1/messages",
            "/v1/chat/completions",
            "/v1/models",
            "/healthz",
            "/metrics",
            "/docs",
            "/dashboard",
            "/openapi.json",
        ] {
            assert!(
                paths.get(path).is_some(),
                "{path} is missing from the document"
            );
        }
        for path in ["/v1/messages", "/v1/chat/completions"] {
            assert!(paths[path].get("post").is_some(), "{path} is a POST");
        }
        let responses = &paths["/v1/messages"]["post"]["responses"];
        for code in ["200", "400", "404", "413", "502", "503", "504"] {
            assert!(
                responses.get(code).is_some(),
                "/v1/messages does not document {code}"
            );
        }
    }

    #[test]
    fn every_embedded_page_pins_its_cdn_assets() {
        assert_pins(DOCS_HTML, "docs.html");
        assert_pins(DASHBOARD_HTML, "dashboard.html");
    }
}
