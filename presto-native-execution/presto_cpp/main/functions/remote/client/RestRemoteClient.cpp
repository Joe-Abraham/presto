/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "presto_cpp/main/functions/remote/client/RestRemoteClient.h"

#include <folly/Uri.h>
#include <proxygen/lib/http/HTTPMessage.h>

#include "presto_cpp/main/functions/remote/utils/ContentTypes.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"
#include "velox/expression/EvalCtx.h"
#include "velox/functions/remote/if/GetSerde.h"

#include <curl/curl.h>

using namespace folly;
using namespace facebook::velox;
namespace facebook::presto::functions {
namespace {
inline std::string getContentType(velox::functions::remote::PageFormat fmt) {
  return fmt == velox::functions::remote::PageFormat::SPARK_UNSAFE_ROW
      ? remote::CONTENT_TYPE_SPARK_UNSAFE_ROW
      : remote::CONTENT_TYPE_PRESTO_PAGE;
}
size_t readCallback(char* dest, size_t size, size_t nmemb, void* userp) {
  auto* inputBufQueue = static_cast<IOBufQueue*>(userp);
  size_t bufferSize = size * nmemb;
  size_t totalCopied = 0;

  while (totalCopied < bufferSize && !inputBufQueue->empty()) {
    auto buf = inputBufQueue->front();
    size_t remainingSize = bufferSize - totalCopied;
    size_t copySize = std::min(remainingSize, buf->length());
    std::memcpy(dest + totalCopied, buf->data(), copySize);
    totalCopied += copySize;
    inputBufQueue->pop_front();
  }

  return totalCopied;
}

size_t writeCallback(char* ptr, size_t size, size_t nmemb, void* userData) {
  auto* outputBuf = static_cast<IOBufQueue*>(userData);
  size_t totalSize = size * nmemb;
  auto buf = IOBuf::copyBuffer(ptr, totalSize);
  outputBuf->append(std::move(buf));
  return totalSize;
}
} // namespace

RestRemoteClient::RestRemoteClient(
    const std::string& url,
    const std::string& functionName,
    RowTypePtr remoteInputType,
    std::vector<std::string> serializedInputTypes,
    const PrestoRemoteFunctionsMetadata& metadata)
    : functionName_(functionName),
      remoteInputType_(std::move(remoteInputType)),
      serializedInputTypes_(std::move(serializedInputTypes)),
      serdeFormat_(metadata.serdeFormat),
      metadata_(metadata),
      serde_(velox::functions::getSerde(serdeFormat_)),
      url_(url) {
  folly::Uri uri(url_);
  VELOX_USER_CHECK(
      uri.scheme() == "http" || uri.scheme() == "https",
      "Unsupported URL scheme: {}",
      uri.scheme());

  const auto& host = uri.host();
  const auto port = uri.port();
  const bool secure = (uri.scheme() == "https");

  evbThread_ = std::make_unique<folly::ScopedEventBaseThread>("rest-client");
  proxygen::Endpoint endpoint(host, port, secure);
  folly::SocketAddress addr(host.c_str(), port, true);
  auto memPool = memory::MemoryManager::getInstance()->addLeafPool();
  static http::HttpClientConnectionPool connPool;

  httpClient_ = std::make_shared<http::HttpClient>(
      evbThread_->getEventBase(),
      &connPool,
      endpoint,
      addr,
      requestTimeoutMs,
      connectTimeoutMs,
      memPool,
      nullptr);
}

void RestRemoteClient::applyRemote(
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args,
    const TypePtr& outputType,
    exec::EvalCtx& context,
    VectorPtr& result) const {
  try {
    auto remoteRowVector = std::make_shared<RowVector>(
        context.pool(),
        remoteInputType_,
        BufferPtr{},
        rows.end(),
        std::move(args));

    auto requestBody = std::make_unique<folly::IOBuf>(rowVectorToIOBuf(
        remoteRowVector, rows.end(), *context.pool(), serde_.get()));

    auto responseBody = invokeFunction(
        metadata_.location, std::move(requestBody), metadata_.serdeFormat);

    auto outputRowVector = IOBufToRowVector(
        *responseBody, ROW({outputType}), *context.pool(), serde_.get());

    result = outputRowVector->childAt(0);
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "Error while executing remote function '{}': {}",
        functionName_,
        e.what());
  }
}

std::unique_ptr<folly::IOBuf> RestRemoteClient::invokeFunction(
    const std::string& fullUrl,
    std::unique_ptr<folly::IOBuf> requestPayload,
    velox::functions::remote::PageFormat serdeFormat) const {
  try {
    IOBufQueue inputBufQueue(IOBufQueue::cacheChainLength());
    inputBufQueue.append(std::move(requestPayload));

    CURL* curl = curl_easy_init();
    if (!curl) {
      VELOX_FAIL(
          fmt::format(
              "Error initializing CURL: {}",
              curl_easy_strerror(CURLE_FAILED_INIT)));
    }

    curl_easy_setopt(curl, CURLOPT_URL, fullUrl.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_READFUNCTION, readCallback);
    curl_easy_setopt(curl, CURLOPT_READDATA, &inputBufQueue);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);

    IOBufQueue outputBuf(IOBufQueue::cacheChainLength());
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &outputBuf);
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);

    std::string contentType = getContentType(serdeFormat);

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(
        headers, fmt::format("Content-Type: {}", contentType).c_str());
    headers = curl_slist_append(
        headers, fmt::format("Accept: {}", contentType).c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    curl_easy_setopt(
        curl,
        CURLOPT_POSTFIELDSIZE,
        static_cast<long>(inputBufQueue.chainLength()));

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      curl_slist_free_all(headers);
      curl_easy_cleanup(curl);
      VELOX_FAIL(
          fmt::format(
              "Error communicating with server: {}\nURL: {}\nCURL Error: {}",
              curl_easy_strerror(res),
              fullUrl.c_str(),
              curl_easy_strerror(res)));
    }
    long responseCode;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode);

    if (responseCode < 200 || responseCode >= 300) {
      std::string responseMessage;
      if (!outputBuf.empty()) {
        auto responseData = outputBuf.move();
        responseMessage = responseData->moveToFbString().toStdString();
      } else {
        responseMessage = "No response body received";
      }

      curl_slist_free_all(headers);
      curl_easy_cleanup(curl);
      VELOX_FAIL(
          fmt::format(
              "Server responded with status {}. Message: '{}'. URL: {}",
              responseCode,
              responseMessage,
              fullUrl));
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    return outputBuf.move();

  } catch (const std::exception& e) {
    VELOX_FAIL(fmt::format("Exception during CURL request: {}", e.what()));
  }
}

} // namespace facebook::presto::functions
