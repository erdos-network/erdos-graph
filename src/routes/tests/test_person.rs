#[cfg(test)]
mod tests {
    use crate::db::queries::PersonInfo;
    use crate::routes::person::person_exists;
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
            .route("/person/exists", get(person_exists))
            .with_state(make_state(mock))
    }

    #[tokio::test]
    async fn test_person_found_returns_details() {
        let mock = MockGraphQueries {
            person: Some(PersonInfo {
                name: "Terence Tao".into(),
                erdos_number: "2".into(),
            }),
            path: None,
        };
        let response = make_router(mock)
            .oneshot(
                Request::builder()
                    .uri("/person/exists?name=Terence+Tao")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["exists"], true);
        assert_eq!(json["person"]["name"], "Terence Tao");
        assert_eq!(json["person"]["erdos_number"], "2");
    }

    #[tokio::test]
    async fn test_person_not_found_omits_person_field() {
        let mock = MockGraphQueries {
            person: None,
            path: None,
        };
        let response = make_router(mock)
            .oneshot(
                Request::builder()
                    .uri("/person/exists?name=Unknown+Author")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["exists"], false);
        assert!(json["person"].is_null());
    }

    #[tokio::test]
    async fn test_person_missing_name_param_returns_400() {
        let mock = MockGraphQueries {
            person: None,
            path: None,
        };
        let response = make_router(mock)
            .oneshot(
                Request::builder()
                    .uri("/person/exists")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
