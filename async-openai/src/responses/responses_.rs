use crate::{
    config::Config,
    error::OpenAIError,
    types::responses::{
        CompactResource, CompactResponseRequest, CreateResponse, DeleteResponse, Response,
        ResponseItemList, TokenCountsBody, TokenCountsResource,
    },
    Client, RequestOptions,
};

use crate::types::responses::ResponseStream;
#[cfg(not(feature = "byot"))]
use crate::types::responses::ResponseStreamEvent;
#[cfg(not(feature = "byot"))]
use futures::stream::StreamExt;

/// Drops events this crate cannot model (e.g. provider-native `keepalive`
/// heartbeats, which deserialize as `ResponseStreamEvent::Unknown`). They carry
/// no client-actionable payload; yielding them would hand every consumer an
/// event it cannot interpret.
#[cfg(not(feature = "byot"))]
fn drop_unknown_events(stream: ResponseStream) -> ResponseStream {
    Box::pin(stream.filter_map(|event| async move {
        match event {
            Ok(ResponseStreamEvent::Unknown) => None,
            event => Some(event),
        }
    }))
}

pub struct Responses<'c, C: Config> {
    client: &'c Client<C>,
    pub(crate) request_options: RequestOptions,
}

impl<'c, C: Config> Responses<'c, C> {
    /// Constructs a new Responses client.
    pub fn new(client: &'c Client<C>) -> Self {
        Self {
            client,
            request_options: RequestOptions::new(),
        }
    }

    /// Creates a model response. Provide [text](https://platform.openai.com/docs/guides/text) or
    /// [image](https://platform.openai.com/docs/guides/images) inputs to generate
    /// [text](https://platform.openai.com/docs/guides/text) or
    /// [JSON](https://platform.openai.com/docs/guides/structured-outputs) outputs. Have the model call
    /// your own [custom code](https://platform.openai.com/docs/guides/function-calling) or use
    /// built-in [tools](https://platform.openai.com/docs/guides/tools) like
    /// [web search](https://platform.openai.com/docs/guides/tools-web-search)
    /// or [file search](https://platform.openai.com/docs/guides/tools-file-search) to use your own data
    /// as input for the model's response.
    #[crate::byot(
        T0 = serde::Serialize,
        R = serde::de::DeserializeOwned
    )]
    pub async fn create(&self, request: CreateResponse) -> Result<Response, OpenAIError> {
        self.client
            .post("/responses", request, &self.request_options)
            .await
    }

    /// Creates a model response for the given input with streaming.
    ///
    /// Response events will be sent as server-sent events as they become available,
    #[crate::byot(
        T0 = serde::Serialize,
        R = serde::de::DeserializeOwned,
        stream = "true",
        where_clause = "R: crate::traits::MaybeSend + 'static"
    )]
    #[allow(unused_mut)]
    pub async fn create_stream(
        &self,
        mut request: CreateResponse,
    ) -> Result<ResponseStream, OpenAIError> {
        #[cfg(not(feature = "byot"))]
        {
            if matches!(request.stream, Some(false)) {
                return Err(OpenAIError::InvalidArgument(
                    "When stream is false, use Responses::create".into(),
                ));
            }
            request.stream = Some(true);
        }
        let stream = self
            .client
            .post_stream("/responses", request, &self.request_options)
            .await?;
        #[cfg(not(feature = "byot"))]
        let stream = drop_unknown_events(stream);
        Ok(stream)
    }

    /// Retrieves a model response with the given ID.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn retrieve(&self, response_id: &str) -> Result<Response, OpenAIError> {
        self.client
            .get(
                &format!("/responses/{}", response_id),
                &self.request_options,
            )
            .await
    }

    /// Retrieves a model response with the given ID with streaming.
    ///
    /// Response events will be sent as server-sent events as they become available.
    #[crate::byot(
        T0 = std::fmt::Display,
        R = serde::de::DeserializeOwned,
        stream = "true",
        where_clause = "R: crate::traits::MaybeSend + 'static"
    )]
    pub async fn retrieve_stream(&self, response_id: &str) -> Result<ResponseStream, OpenAIError> {
        let mut request_options = self.request_options.clone();
        request_options.with_query(&[("stream", "true")])?;

        let stream = self
            .client
            .get_stream(&format!("/responses/{}", response_id), &request_options)
            .await?;
        #[cfg(not(feature = "byot"))]
        let stream = drop_unknown_events(stream);
        Ok(stream)
    }

    /// Deletes a model response with the given ID.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn delete(&self, response_id: &str) -> Result<DeleteResponse, OpenAIError> {
        self.client
            .delete(
                &format!("/responses/{}", response_id),
                &self.request_options,
            )
            .await
    }

    /// Cancels a model response with the given ID. Only responses created with the
    /// `background` parameter set to `true` can be cancelled.
    /// [Learn more](https://platform.openai.com/docs/guides/background).
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn cancel(&self, response_id: &str) -> Result<Response, OpenAIError> {
        self.client
            .post(
                &format!("/responses/{}/cancel", response_id),
                serde_json::json!({}),
                &self.request_options,
            )
            .await
    }

    /// Returns a list of input items for a given response.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn list_input_items(
        &self,
        response_id: &str,
    ) -> Result<ResponseItemList, OpenAIError> {
        self.client
            .get(
                &format!("/responses/{}/input_items", response_id),
                &self.request_options,
            )
            .await
    }

    /// Get input token counts
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn get_input_token_counts(
        &self,
        request: TokenCountsBody,
    ) -> Result<TokenCountsResource, OpenAIError> {
        self.client
            .post("/responses/input_tokens", request, &self.request_options)
            .await
    }

    /// Compact a conversation.
    ///
    /// Learn when and how to compact long-running conversations in the
    /// [conversation state guide](https://platform.openai.com/docs/guides/conversation-state#managing-the-context-window).
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn compact(
        &self,
        request: CompactResponseRequest,
    ) -> Result<CompactResource, OpenAIError> {
        self.client
            .post("/responses/compact", request, &self.request_options)
            .await
    }
}

#[cfg(all(test, not(feature = "byot")))]
mod tests {
    use super::*;
    use crate::types::responses::ResponseErrorEvent;

    #[tokio::test]
    async fn drop_unknown_events_skips_only_unknown() {
        let error_event = || {
            Ok(ResponseStreamEvent::ResponseError(ResponseErrorEvent {
                sequence_number: None,
                code: None,
                message: "boom".into(),
                param: None,
            }))
        };
        let stream: ResponseStream = Box::pin(futures::stream::iter([
            Ok(ResponseStreamEvent::Unknown),
            error_event(),
            Ok(ResponseStreamEvent::Unknown),
        ]));

        let collected: Vec<_> = drop_unknown_events(stream).collect().await;

        assert_eq!(collected.len(), 1);
        assert!(matches!(
            collected[0],
            Ok(ResponseStreamEvent::ResponseError(_))
        ));
    }
}
