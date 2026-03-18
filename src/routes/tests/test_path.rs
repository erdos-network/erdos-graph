#[cfg(test)]
mod tests {
    use crate::db::queries::{PaperInfo, PathStep};
    use crate::routes::path::shortest_path;
    use crate::routes::tests::test_utils::{MockGraphQueries, make_state};
    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::get,
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn make_router(mock: MockGraphQueries) -> Router {
        Router::new()
            .route("/path", get(shortest_path))
            .with_state(make_state(mock))
    }

    fn two_step_path() -> Vec<PathStep> {
        vec![
            PathStep {
                name: "Alice".into(),
                papers_to_next: vec![PaperInfo {
                    title: "A Great Paper".into(),
                    year: "2020".into(),
                    venue: "ArXiv".into(),
                    publication_id: "arxiv:2001.12345".into(),
                }],
            },
            PathStep {
                name: "Bob".into(),
                papers_to_next: vec![PaperInfo {
                    title: "Another Paper".into(),
                    year: "2015".into(),
                    venue: "JCTA".into(),
                    publication_id: "zbmath:0001".into(),
                }],
            },
            PathStep {
                name: "Charlie".into(),
                papers_to_next: vec![],
            },
        ]
    }

    #[tokio::test]
    async fn test_path_found_returns_distance_and_steps() {
        let mock = MockGraphQueries {
            person: None,
            path: Some(two_step_path()),
        };
        let response = make_router(mock)
            .oneshot(
                Request::builder()
                    .uri("/path?from=Alice&to=Charlie")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["found"], true);
        assert_eq!(json["distance"], 2);
        assert_eq!(json["path"].as_array().unwrap().len(), 3);
        assert_eq!(json["path"][0]["name"], "Alice");
        assert_eq!(json["path"][2]["name"], "Charlie");
        assert!(
            json["path"][2]["papers_to_next"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_path_not_found_returns_found_false() {
        let mock = MockGraphQueries {
            person: None,
            path: None,
        };
        let response = make_router(mock)
            .oneshot(
                Request::builder()
                    .uri("/path?from=Alice&to=Unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["found"], false);
    }

    #[tokio::test]
    async fn test_path_missing_params_returns_400() {
        let mock = MockGraphQueries {
            person: None,
            path: None,
        };
        let response = make_router(mock)
            .oneshot(
                Request::builder()
                    .uri("/path?from=Alice") // missing `to`
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_path_single_step_has_distance_zero() {
        let single = vec![PathStep {
            name: "Solo".into(),
            papers_to_next: vec![],
        }];
        let mock = MockGraphQueries {
            person: None,
            path: Some(single),
        };
        let response = make_router(mock)
            .oneshot(
                Request::builder()
                    .uri("/path?from=Solo&to=Solo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["distance"], 0);
    }
}
