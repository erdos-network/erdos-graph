#[cfg(test)]
mod tests {
    use crate::db::queries::{PaperInfo, PathStep};
    use crate::routes::erdos::erdos_number;
    use crate::routes::tests::test_utils::{MockGraphQueries, make_state};
    use axum::{Router, body::Body, http::{Request, StatusCode}, routing::get};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn make_router(mock: MockGraphQueries) -> Router {
        Router::new()
            .route("/erdos", get(erdos_number))
            .with_state(make_state(mock))
    }

    fn erdos_path_length_2() -> Vec<PathStep> {
        vec![
            PathStep {
                name: "Alice".into(),
                papers_to_next: vec![PaperInfo {
                    title: "Joint Work".into(),
                    year: "1999".into(),
                    venue: "Combinatorica".into(),
                    publication_id: "zbmath:0002".into(),
                }],
            },
            PathStep {
                name: "Bob".into(),
                papers_to_next: vec![PaperInfo {
                    title: "Erdős Collab".into(),
                    year: "1985".into(),
                    venue: "Discrete Math".into(),
                    publication_id: "zbmath:0003".into(),
                }],
            },
            PathStep {
                name: "Paul Erdős".into(),
                papers_to_next: vec![],
            },
        ]
    }

    #[tokio::test]
    async fn test_erdos_number_connected_returns_number_and_path() {
        let mock = MockGraphQueries {
            person: None,
            path: Some(erdos_path_length_2()),
        };
        let response = make_router(mock)
            .oneshot(
                Request::builder()
                    .uri("/erdos?name=Alice")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["erdos_number"], 2);
        assert_eq!(json["path"].as_array().unwrap().len(), 3);
        assert_eq!(json["path"][2]["name"], "Paul Erdős");
    }

    #[tokio::test]
    async fn test_erdos_number_not_connected_returns_null() {
        let mock = MockGraphQueries {
            person: None,
            path: None,
        };
        let response = make_router(mock)
            .oneshot(
                Request::builder()
                    .uri("/erdos?name=Isolated+Author")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["erdos_number"].is_null());
        assert!(json["path"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_erdos_number_missing_name_returns_400() {
        let mock = MockGraphQueries {
            person: None,
            path: None,
        };
        let response = make_router(mock)
            .oneshot(
                Request::builder()
                    .uri("/erdos")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_erdos_number_zero_for_erdos_himself() {
        let mock = MockGraphQueries {
            person: None,
            path: Some(vec![PathStep {
                name: "Paul Erdős".into(),
                papers_to_next: vec![],
            }]),
        };
        let response = make_router(mock)
            .oneshot(
                Request::builder()
                    .uri("/erdos?name=Paul+Erd%C5%91s")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["erdos_number"], 0);
    }
}
