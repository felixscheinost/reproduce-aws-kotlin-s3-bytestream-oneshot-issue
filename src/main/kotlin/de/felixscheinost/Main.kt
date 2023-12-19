package de.felixscheinost

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.createBucket
import aws.sdk.kotlin.services.s3.model.ChecksumAlgorithm
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.sdk.kotlin.services.s3.putObject
import aws.smithy.kotlin.runtime.InternalApi
import aws.smithy.kotlin.runtime.auth.awscredentials.Credentials
import aws.smithy.kotlin.runtime.auth.awscredentials.CredentialsProvider
import aws.smithy.kotlin.runtime.auth.awssigning.AwsSigningAttributes
import aws.smithy.kotlin.runtime.auth.awssigning.HashSpecification
import aws.smithy.kotlin.runtime.auth.awssigning.internal.AWS_CHUNKED_THRESHOLD
import aws.smithy.kotlin.runtime.client.ProtocolRequestInterceptorContext
import aws.smithy.kotlin.runtime.collections.Attributes
import aws.smithy.kotlin.runtime.content.ByteStream
import aws.smithy.kotlin.runtime.content.toByteStream
import aws.smithy.kotlin.runtime.http.interceptors.HttpInterceptor
import aws.smithy.kotlin.runtime.http.request.HttpRequest
import aws.smithy.kotlin.runtime.net.url.Url
import aws.smithy.kotlin.runtime.text.encoding.encodeToHex
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MinIOContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.DockerLoggerFactory
import java.security.MessageDigest
import java.util.*

@OptIn(InternalApi::class)
fun main() {
  val minioContainer = MinIOContainer("minio/minio:RELEASE.2023-12-14T18-51-57Z").apply {
    withLogConsumer(Slf4jLogConsumer(DockerLoggerFactory.getLogger("minio")))
    start()
  }

  val client = S3Client {
    credentialsProvider = object : CredentialsProvider {
      override suspend fun resolve(attributes: Attributes) = Credentials(
        accessKeyId = minioContainer.userName,
        secretAccessKey = minioContainer.password
      )
    }
    endpointUrl = Url.parse(minioContainer.s3URL)
    region = "us-east-1"
    forcePathStyle = true

    // Workaround
    // Uncomment to fix
//    interceptors.add(object : HttpInterceptor {
//      override suspend fun modifyBeforeSigning(context: ProtocolRequestInterceptorContext<Any, HttpRequest>): HttpRequest {
//        (context.request as? PutObjectRequest)?.let { putObjectRequest ->
//          val body = putObjectRequest.body
//          val sha256 = putObjectRequest.checksumSha256
//          if (body?.isOneShot == true && sha256 != null) {
//            val sha256Base64 = Base64.getDecoder().decode(sha256).encodeToHex()
//            context.executionContext.attributes[AwsSigningAttributes.HashSpecification] =
//              HashSpecification.Precalculated(sha256Base64)
//          }
//        }
//        return context.protocolRequest
//      }
//    })
  }

  val log = LoggerFactory.getLogger("de.felixscheinost.MainKt")
  runBlocking(Dispatchers.IO) {
    log.info("Creating bucket")
    client.createBucket {
      bucket = "test"
    }
    log.info("OK")

    val bytes = "abc".toByteArray(Charsets.UTF_8)
    val bigBytes = ByteArray(AWS_CHUNKED_THRESHOLD + 1) { 0x42 }

    fun sha256sum(bytes: ByteArray): String {
      val messageDigest = MessageDigest.getInstance("SHA-256")
      messageDigest.update(bytes)
      return Base64.getEncoder().encodeToString(messageDigest.digest())
    }

    log.info("Putting object using repeatable stream")
    client.putObject {
      bucket = "test"
      key = "object_repeatable"
      checksumAlgorithm = ChecksumAlgorithm.Sha256
      checksumSha256 = sha256sum(bytes)
      contentLength = bytes.size.toLong()
      body = ByteStream.fromBytes(bytes)
    }
    log.info("OK")

    // Note: This also fails as `flow {}.toByteStream` doesn't produce a HttpBody.SourceContent || HttpBody.ChannelContent
    // Not eligible for chunked encoding?
    log.info("Putting big object using non-repeatable stream")
    client.putObject {
      bucket = "test"
      key = "object_non_repeatable_big"
      checksumAlgorithm = ChecksumAlgorithm.Sha256
      checksumSha256 = sha256sum(bigBytes)
      contentLength = bigBytes.size.toLong()
      body = flow {
        emit(bigBytes)
      }.toByteStream(this@runBlocking)
    }
    log.info("OK")

    log.info("Putting small object using non-repeatable stream")
    client.putObject {
      bucket = "test"
      key = "object_non_repeatable_small"
      checksumAlgorithm = ChecksumAlgorithm.Sha256
      checksumSha256 = sha256sum(bytes)
      contentLength = bytes.size.toLong()
      body = flow {
        emit(bytes)
      }.toByteStream(this@runBlocking)
      log.info("OK")

      log.info("Done")
    }
  }
}