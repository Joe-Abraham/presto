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

#include "RestClient.h"

#include "presto_cpp/main/functions/remote/client/Remote.h"

#include <folly/io/IOBufQueue.h>
#include "presto_cpp/main/functions/remote/utils/ContentTypes.h"
#include "velox/common/base/Exceptions.h"

#include <curl/curl.h>

using namespace folly;
using namespace facebook::velox;
namespace facebook::presto::functions {
namespace {
inline std::string getContentType(
    velox::functions::remote::PageFormat serdeFormat) {
  return serdeFormat == velox::functions::remote::PageFormat::SPARK_UNSAFE_ROW
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

std::unique_ptr<IOBuf> RestClient::invokeFunction(
    const std::string& fullUrl,
    std::unique_ptr<IOBuf> requestPayload,
    velox::functions::remote::PageFormat serdeFormat) {
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

std::unique_ptr<RestClient> getRestClient() {
  return std::make_unique<RestClient>();
}

} // namespace facebook::presto::functions
